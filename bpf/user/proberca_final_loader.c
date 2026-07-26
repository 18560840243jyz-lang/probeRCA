#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/types.h>

#include "../final_normal/final_normal.h"

#define MAX_LINKS 64
#define DEFAULT_TIMEOUT_MS 5000
#define ACTIVE_WAIT_TABLE_SIZE (PROBERCA_FINAL_MAX_CGROUPS * 2)

struct options {
    const char *object_path;
    const char *cgroup_path;
    const char *pin_dir;
    const char *snapshot_dir;
    uint64_t timeout_ms;
};

struct active_wait_entry {
    uint64_t cgroup_id;
    uint64_t total_ns;
    bool occupied;
};

static volatile sig_atomic_t stopping;

static void on_signal(int signal_number)
{
    (void)signal_number;
    stopping = 1;
}

static uint64_t monotonic_ns(void)
{
    struct timespec timestamp;

    if (clock_gettime(CLOCK_MONOTONIC, &timestamp) != 0)
        return 0;
    return (uint64_t)timestamp.tv_sec * 1000000000ULL +
           (uint64_t)timestamp.tv_nsec;
}

static int parse_u64(const char *text, uint64_t *value)
{
    char *end = NULL;

    errno = 0;
    *value = strtoull(text, &end, 10);
    return errno || !end || *end ? -EINVAL : 0;
}

static int parse_options(
    int argc, char **argv, struct options *options)
{
    enum {
        OPT_OBJECT = 1000,
        OPT_CGROUP,
        OPT_PIN_DIR,
        OPT_SNAPSHOT,
        OPT_TIMEOUT_MS,
    };
    static const struct option long_options[] = {
        {"object", required_argument, NULL, OPT_OBJECT},
        {"cgroup", required_argument, NULL, OPT_CGROUP},
        {"pin-dir", required_argument, NULL, OPT_PIN_DIR},
        {"snapshot", required_argument, NULL, OPT_SNAPSHOT},
        {"timeout-ms", required_argument, NULL, OPT_TIMEOUT_MS},
        {NULL, 0, NULL, 0},
    };
    int option;

    memset(options, 0, sizeof(*options));
    options->cgroup_path = "/sys/fs/cgroup";
    options->pin_dir = "/sys/fs/bpf/proberca-final";
    options->timeout_ms = DEFAULT_TIMEOUT_MS;
    while ((option = getopt_long(
                argc, argv, "", long_options, NULL)) != -1) {
        switch (option) {
        case OPT_OBJECT:
            options->object_path = optarg;
            break;
        case OPT_CGROUP:
            options->cgroup_path = optarg;
            break;
        case OPT_PIN_DIR:
            options->pin_dir = optarg;
            break;
        case OPT_SNAPSHOT:
            options->snapshot_dir = optarg;
            break;
        case OPT_TIMEOUT_MS:
            if (parse_u64(optarg, &options->timeout_ms) != 0 ||
                options->timeout_ms < 100 ||
                options->timeout_ms > 60000)
                return -EINVAL;
            break;
        default:
            return -EINVAL;
        }
    }
    if (optind != argc)
        return -EINVAL;
    if (options->snapshot_dir)
        return options->object_path ? -EINVAL : 0;
    return options->object_path ? 0 : -EINVAL;
}

static int make_path(
    char output[PATH_MAX], const char *directory, const char *name)
{
    int written = snprintf(output, PATH_MAX, "%s/%s", directory, name);

    return written < 0 || written >= PATH_MAX ? -ENAMETOOLONG : 0;
}

static int open_pinned(
    const char *directory, const char *name)
{
    char path[PATH_MAX];

    if (make_path(path, directory, name) != 0)
        return -ENAMETOOLONG;
    return bpf_obj_get(path);
}

static int pin_map(
    struct bpf_object *object, const char *directory, const char *name)
{
    struct bpf_map *map = bpf_object__find_map_by_name(object, name);
    char path[PATH_MAX];
    int result;

    if (!map)
        return -ENOENT;
    result = make_path(path, directory, name);
    if (result != 0)
        return result;
    if (unlink(path) != 0 && errno != ENOENT)
        return -errno;
    result = bpf_map__pin(map, path);
    return result == 0 ? 0 : -errno;
}

static int pin_required_maps(
    struct bpf_object *object, const char *directory)
{
    static const char *const names[] = {
        "cgroup_counters",
        "futex_starts",
        "dns_pending",
        "dns_edge_counters",
        "dns_timeout_counters",
    };
    size_t index;
    int result;

    if (mkdir(directory, 0755) != 0 && errno != EEXIST)
        return -errno;
    for (index = 0; index < sizeof(names) / sizeof(names[0]); index++) {
        result = pin_map(object, directory, names[index]);
        if (result != 0)
            return result;
    }
    return 0;
}

static int attach_programs(
    struct bpf_object *object,
    int cgroup_fd,
    struct bpf_link *links[MAX_LINKS],
    size_t *link_count)
{
    struct bpf_program *program;
    struct bpf_link *link;
    const char *section;
    int error;

    bpf_object__for_each_program(program, object) {
        if (*link_count >= MAX_LINKS)
            return -E2BIG;
        section = bpf_program__section_name(program);
        if (section && strncmp(section, "cgroup_skb/", 11) == 0)
            link = bpf_program__attach_cgroup(program, cgroup_fd);
        else
            link = bpf_program__attach(program);
        error = libbpf_get_error(link);
        if (error)
            return error;
        links[(*link_count)++] = link;
    }
    return 0;
}

static int run_loader(const struct options *options)
{
    struct bpf_object *object = NULL;
    struct bpf_link *links[MAX_LINKS] = {};
    size_t link_count = 0;
    struct rlimit limit = {RLIM_INFINITY, RLIM_INFINITY};
    int cgroup_fd = -1;
    int result = 1;
    size_t index;

    if (setrlimit(RLIMIT_MEMLOCK, &limit) != 0) {
        perror("setrlimit(RLIMIT_MEMLOCK)");
        goto cleanup;
    }
    object = bpf_object__open_file(options->object_path, NULL);
    if (libbpf_get_error(object)) {
        fprintf(stderr, "cannot open BPF object\n");
        object = NULL;
        goto cleanup;
    }
    if (bpf_object__load(object) != 0) {
        fprintf(stderr, "cannot load BPF object\n");
        goto cleanup;
    }
    cgroup_fd = open(options->cgroup_path, O_RDONLY | O_DIRECTORY);
    if (cgroup_fd < 0) {
        perror("open cgroup");
        goto cleanup;
    }
    if (attach_programs(
            object, cgroup_fd, links, &link_count) != 0) {
        fprintf(stderr, "cannot attach one or more final data-plane probes\n");
        goto cleanup;
    }
    if (pin_required_maps(object, options->pin_dir) != 0) {
        fprintf(stderr, "cannot pin final data-plane maps\n");
        goto cleanup;
    }
    printf(
        "{\"record_type\":\"control\",\"state\":\"ready\","
        "\"map_directory\":\"%s\",\"program_count\":%zu}\n",
        options->pin_dir, link_count);
    fflush(stdout);
    while (!stopping)
        pause();
    result = 0;

cleanup:
    for (index = 0; index < link_count; index++)
        bpf_link__destroy(links[index]);
    if (cgroup_fd >= 0)
        close(cgroup_fd);
    bpf_object__close(object);
    return result;
}

static int increment_timeout(
    int timeout_fd,
    const struct proberca_final_dns_pending_key *pending)
{
    struct proberca_final_dns_edge_key edge = {};
    uint64_t value = 0;

    edge.cgroup_id = pending->cgroup_id;
    edge.server_ipv4 = pending->server_ipv4;
    if (bpf_map_lookup_elem(timeout_fd, &edge, &value) != 0 &&
        errno != ENOENT)
        return -errno;
    value++;
    return bpf_map_update_elem(timeout_fd, &edge, &value, BPF_ANY);
}

static int sweep_dns_timeouts(
    int pending_fd, int timeout_fd, uint64_t timeout_ns)
{
    struct proberca_final_dns_pending_key key;
    struct proberca_final_dns_pending_key next;
    bool have_key = false;
    uint64_t started_ns;
    uint64_t now = monotonic_ns();
    int result;

    if (!now)
        return -EIO;
    while (true) {
        result = bpf_map_get_next_key(
            pending_fd, have_key ? &key : NULL, &next);
        if (result != 0) {
            if (errno == ENOENT)
                return 0;
            return -errno;
        }
        key = next;
        have_key = true;
        if (bpf_map_lookup_elem(pending_fd, &key, &started_ns) != 0)
            continue;
        if (now >= started_ns && now - started_ns >= timeout_ns) {
            result = increment_timeout(timeout_fd, &key);
            if (result != 0)
                return result;
            bpf_map_delete_elem(pending_fd, &key);
            have_key = false;
        }
    }
}

static struct active_wait_entry *active_wait_entry(
    struct active_wait_entry *table, uint64_t cgroup_id, bool create)
{
    size_t index = (size_t)(
        (cgroup_id * 11400714819323198485ULL) &
        (ACTIVE_WAIT_TABLE_SIZE - 1));
    size_t scanned;

    for (scanned = 0; scanned < ACTIVE_WAIT_TABLE_SIZE; scanned++) {
        struct active_wait_entry *entry = &table[index];

        if (!entry->occupied) {
            if (!create)
                return NULL;
            entry->occupied = true;
            entry->cgroup_id = cgroup_id;
            return entry;
        }
        if (entry->cgroup_id == cgroup_id)
            return entry;
        index = (index + 1) & (ACTIVE_WAIT_TABLE_SIZE - 1);
    }
    return NULL;
}

static int collect_active_futex_waits(
    int futex_fd, uint64_t now_ns, struct active_wait_entry *table)
{
    struct proberca_final_futex_start value;
    struct active_wait_entry *entry;
    uint64_t key;
    uint64_t next;
    uint64_t elapsed;
    bool have_key = false;

    errno = 0;
    while (bpf_map_get_next_key(
               futex_fd, have_key ? &key : NULL, &next) == 0) {
        key = next;
        have_key = true;
        if (bpf_map_lookup_elem(futex_fd, &key, &value) != 0)
            continue;
        if (now_ns < value.started_ns)
            continue;
        elapsed = now_ns - value.started_ns;
        entry = active_wait_entry(table, value.cgroup_id, true);
        if (!entry)
            return -ENOSPC;
        if (UINT64_MAX - entry->total_ns < elapsed)
            return -EOVERFLOW;
        entry->total_ns += elapsed;
    }
    return errno == ENOENT ? 0 : -errno;
}

static int print_cgroups(int map_fd, int futex_fd, uint64_t now_ns)
{
    struct active_wait_entry *active_waits;
    struct active_wait_entry *active;
    struct proberca_final_cgroup_counters value;
    uint64_t active_wait_ns;
    uint64_t wait_total;
    uint64_t key;
    uint64_t next;
    bool have_key = false;
    int result;

    active_waits = calloc(
        ACTIVE_WAIT_TABLE_SIZE, sizeof(*active_waits));
    if (!active_waits)
        return -ENOMEM;
    result = collect_active_futex_waits(
        futex_fd, now_ns, active_waits);
    if (result != 0) {
        free(active_waits);
        return result;
    }
    errno = 0;
    while (bpf_map_get_next_key(
               map_fd, have_key ? &key : NULL, &next) == 0) {
        key = next;
        have_key = true;
        if (bpf_map_lookup_elem(map_fd, &key, &value) != 0)
            continue;
        active = active_wait_entry(active_waits, key, false);
        active_wait_ns = active ? active->total_ns : 0;
        if (UINT64_MAX - value.futex_wait_ns_total < active_wait_ns)
            result = -EOVERFLOW;
        if (result != 0)
            break;
        wait_total = value.futex_wait_ns_total + active_wait_ns;
        printf(
            "{\"record_type\":\"cgroup\",\"cgroup_id\":%llu,"
            "\"futex_wait_ns_total\":%llu,"
            "\"socket_backlog_overflow_total\":%llu,"
            "\"socket_accept_fail_total\":%llu,"
            "\"socket_local_rst_total\":%llu,"
            "\"socket_local_drop_total\":%llu,"
            "\"socket_ops_total\":%llu}\n",
            (unsigned long long)key,
            (unsigned long long)wait_total,
            (unsigned long long)value.socket_backlog_overflow_total,
            (unsigned long long)value.socket_accept_fail_total,
            (unsigned long long)value.socket_local_rst_total,
            (unsigned long long)value.socket_local_drop_total,
            (unsigned long long)value.socket_ops_total);
    }
    if (result == 0 && errno != ENOENT)
        result = -errno;
    free(active_waits);
    return result;
}

static int print_dns(int counters_fd, int timeout_fd)
{
    struct proberca_final_dns_edge_counters value;
    struct proberca_final_dns_edge_key key;
    struct proberca_final_dns_edge_key next;
    uint64_t timeout_total;
    bool have_key = false;
    char address[INET_ADDRSTRLEN];
    size_t index;

    while (bpf_map_get_next_key(
               counters_fd, have_key ? &key : NULL, &next) == 0) {
        key = next;
        have_key = true;
        if (bpf_map_lookup_elem(counters_fd, &key, &value) != 0)
            continue;
        timeout_total = 0;
        if (bpf_map_lookup_elem(
                timeout_fd, &key, &timeout_total) != 0 &&
            errno != ENOENT)
            return -errno;
        if (!inet_ntop(
                AF_INET, &key.server_ipv4, address, sizeof(address)))
            return -errno;
        printf(
            "{\"record_type\":\"dns\",\"cgroup_id\":%llu,"
            "\"server_ipv4\":\"%s\",\"query_total\":%llu,"
            "\"timeout_total\":%llu,\"error_rcode_total\":%llu,"
            "\"latency_buckets\":[",
            (unsigned long long)key.cgroup_id,
            address,
            (unsigned long long)value.query_total,
            (unsigned long long)timeout_total,
            (unsigned long long)value.error_rcode_total);
        for (index = 0; index < PROBERCA_FINAL_DNS_BUCKETS; index++) {
            printf(
                "%s%llu", index ? "," : "",
                (unsigned long long)value.latency_buckets[index]);
        }
        puts("]}");
    }
    return errno == ENOENT ? 0 : -errno;
}

static int run_snapshot(const struct options *options)
{
    int cgroups_fd = -1;
    int futex_fd = -1;
    int pending_fd = -1;
    int dns_fd = -1;
    int timeout_fd = -1;
    int result = 1;

    cgroups_fd = open_pinned(
        options->snapshot_dir, "cgroup_counters");
    futex_fd = open_pinned(options->snapshot_dir, "futex_starts");
    pending_fd = open_pinned(options->snapshot_dir, "dns_pending");
    dns_fd = open_pinned(
        options->snapshot_dir, "dns_edge_counters");
    timeout_fd = open_pinned(
        options->snapshot_dir, "dns_timeout_counters");
    if (cgroups_fd < 0 || futex_fd < 0 || pending_fd < 0 ||
        dns_fd < 0 || timeout_fd < 0) {
        fprintf(stderr, "final BPF maps are unavailable\n");
        goto cleanup;
    }
    if (sweep_dns_timeouts(
            pending_fd, timeout_fd,
            options->timeout_ms * 1000000ULL) != 0) {
        fprintf(stderr, "cannot sweep DNS timeout map\n");
        goto cleanup;
    }
    if (print_cgroups(cgroups_fd, futex_fd, monotonic_ns()) != 0 ||
        print_dns(dns_fd, timeout_fd) != 0) {
        fprintf(stderr, "cannot read final BPF maps\n");
        goto cleanup;
    }
    result = 0;

cleanup:
    if (cgroups_fd >= 0)
        close(cgroups_fd);
    if (futex_fd >= 0)
        close(futex_fd);
    if (pending_fd >= 0)
        close(pending_fd);
    if (dns_fd >= 0)
        close(dns_fd);
    if (timeout_fd >= 0)
        close(timeout_fd);
    return result;
}

int main(int argc, char **argv)
{
    struct options options;

    if (parse_options(argc, argv, &options) != 0) {
        fprintf(
            stderr,
            "usage: %s (--object FILE [--cgroup PATH] [--pin-dir DIR] | "
            "--snapshot DIR [--timeout-ms N])\n",
            argv[0]);
        return 2;
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    return options.snapshot_dir ?
        run_snapshot(&options) : run_loader(&options);
}
