#define _GNU_SOURCE

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
#include <time.h>
#include <unistd.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "../final_burst/final_burst.h"

#define MAX_LINKS 96
#define DEFAULT_TIMEOUT_MS 5000

struct options {
    const char *object_path;
    const char *cgroup_path;
    const char *output_path;
    const char *sampling_profile;
    uint64_t timeout_ms;
    bool dns_only;
};

struct writer_context {
    FILE *output;
    const char *sampling_profile;
    uint64_t epoch_offset_ns;
};

static volatile sig_atomic_t stopping;

static void on_signal(int signal_number)
{
    (void)signal_number;
    stopping = 1;
}

static uint64_t clock_ns(clockid_t clock_id)
{
    struct timespec timestamp;

    if (clock_gettime(clock_id, &timestamp) != 0)
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
        OPT_OUTPUT,
        OPT_TIMEOUT_MS,
        OPT_SAMPLING_PROFILE,
        OPT_DNS_ONLY,
    };
    static const struct option long_options[] = {
        {"object", required_argument, NULL, OPT_OBJECT},
        {"cgroup", required_argument, NULL, OPT_CGROUP},
        {"output", required_argument, NULL, OPT_OUTPUT},
        {"timeout-ms", required_argument, NULL, OPT_TIMEOUT_MS},
        {"sampling-profile", required_argument, NULL, OPT_SAMPLING_PROFILE},
        {"dns-only", no_argument, NULL, OPT_DNS_ONLY},
        {NULL, 0, NULL, 0},
    };
    int option;

    memset(options, 0, sizeof(*options));
    options->cgroup_path = "/sys/fs/cgroup";
    options->sampling_profile = "low";
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
        case OPT_OUTPUT:
            options->output_path = optarg;
            break;
        case OPT_TIMEOUT_MS:
            if (parse_u64(optarg, &options->timeout_ms) != 0 ||
                options->timeout_ms < 100 ||
                options->timeout_ms > 60000)
                return -EINVAL;
            break;
        case OPT_SAMPLING_PROFILE:
            if (strcmp(optarg, "low") != 0 &&
                strcmp(optarg, "full") != 0)
                return -EINVAL;
            options->sampling_profile = optarg;
            break;
        case OPT_DNS_ONLY:
            options->dns_only = true;
            break;
        default:
            return -EINVAL;
        }
    }
    if (optind != argc || !options->object_path || !options->output_path)
        return -EINVAL;
    return 0;
}

static int write_event(
    struct writer_context *writer,
    const struct proberca_burst_event *event)
{
    uint64_t timestamp_ns = event->monotonic_ns +
                            writer->epoch_offset_ns;

    if (fprintf(
            writer->output,
            "{\"record_type\":\"event\",\"schema_version\":%u,"
            "\"timestamp_ns\":%llu,\"monotonic_ns\":%llu,"
            "\"event_type\":%u,\"cgroup_id\":%llu,"
            "\"value\":%llu,\"duration_ns\":%llu,"
            "\"auxiliary_ns\":%llu,\"sequence\":%llu,\"cpu\":%u,"
            "\"src_ipv4\":%u,\"dst_ipv4\":%u,\"device\":%u,"
            "\"src_port\":%u,\"dst_port\":%u,\"protocol\":%u,"
            "\"direction\":%u,\"transaction_id\":%u,\"rcode\":%u,"
            "\"sampling_divisor\":%u}\n",
            PROBERCA_BURST_SCHEMA_VERSION,
            (unsigned long long)timestamp_ns,
            (unsigned long long)event->monotonic_ns,
            event->event_type,
            (unsigned long long)event->cgroup_id,
            (unsigned long long)event->value,
            (unsigned long long)event->duration_ns,
            (unsigned long long)event->auxiliary_ns,
            (unsigned long long)event->sequence,
            event->cpu,
            event->src_ipv4,
            event->dst_ipv4,
            event->device,
            event->src_port,
            event->dst_port,
            event->protocol,
            event->direction,
            event->transaction_id,
            event->rcode,
            event->sampling_divisor) < 0)
        return -EIO;
    return 0;
}

static int handle_event(
    void *context, void *payload, size_t payload_size)
{
    struct writer_context *writer = context;

    if (payload_size != sizeof(struct proberca_burst_event))
        return -EINVAL;
    return write_event(writer, payload);
}

static int attach_programs(
    struct bpf_object *object,
    int cgroup_fd,
    struct bpf_link *links[MAX_LINKS],
    size_t *link_count)
{
    struct bpf_program *program;

    bpf_object__for_each_program(program, object) {
        struct bpf_link *link;
        const char *section;
        int error;

        if (bpf_program__fd(program) < 0)
            continue;
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

static int configure_program_scope(
    struct bpf_object *object, bool dns_only)
{
    struct bpf_program *program;

    if (!dns_only)
        return 0;
    bpf_object__for_each_program(program, object) {
        const char *name = bpf_program__name(program);

        if (name &&
            (strcmp(name, "final_burst_dns_egress") == 0 ||
             strcmp(name, "final_burst_dns_ingress") == 0))
            continue;
        if (bpf_program__set_autoload(program, false) != 0)
            return -EINVAL;
    }
    return 0;
}

static int read_loss(
    int map_fd, uint64_t *emitted, uint64_t *reserve_failed)
{
    int cpu_count = libbpf_num_possible_cpus();
    struct proberca_burst_loss_counters *values;
    uint32_t zero = 0;
    int index;

    if (cpu_count <= 0)
        return -EINVAL;
    values = calloc(
        (size_t)cpu_count, sizeof(struct proberca_burst_loss_counters));
    if (!values)
        return -ENOMEM;
    if (bpf_map_lookup_elem(map_fd, &zero, values) != 0) {
        free(values);
        return -errno;
    }
    *emitted = 0;
    *reserve_failed = 0;
    for (index = 0; index < cpu_count; index++) {
        *emitted += values[index].emitted;
        *reserve_failed += values[index].reserve_failed;
    }
    free(values);
    return 0;
}

static int configure_sampling(
    int map_fd, const char *profile)
{
    uint32_t zero = 0;
    struct proberca_burst_sampling_config config = {
        .sched = 1,
        .futex = 1,
        .socket_wait = 1,
        .softirq = 1,
        .tcp_rtt = 1,
        .tcp_connection = 1,
        .dns_query = 1,
        .dns_response = 1,
    };

    if (strcmp(profile, "low") == 0) {
        config.sched = 64;
        config.futex = 32;
        config.socket_wait = 16;
        config.softirq = 32;
        config.tcp_rtt = 32;
        config.tcp_connection = 16;
        config.dns_query = 16;
        config.dns_response = 16;
    }
    return bpf_map_update_elem(map_fd, &zero, &config, BPF_ANY) == 0
               ? 0
               : -errno;
}

static int write_checkpoint(
    struct writer_context *writer,
    int loss_fd,
    size_t program_count)
{
    uint64_t emitted;
    uint64_t reserve_failed;
    uint64_t epoch_ns = clock_ns(CLOCK_REALTIME);
    uint64_t monotonic_ns = clock_ns(CLOCK_MONOTONIC);
    int result = read_loss(loss_fd, &emitted, &reserve_failed);

    if (result != 0)
        return result;
    if (fprintf(
            writer->output,
            "{\"record_type\":\"checkpoint\",\"schema_version\":%u,"
            "\"timestamp_ns\":%llu,\"monotonic_ns\":%llu,"
            "\"emitted\":%llu,\"reserve_failed\":%llu,"
            "\"program_count\":%zu,\"sampling_profile\":\"%s\"}\n",
            PROBERCA_BURST_SCHEMA_VERSION,
            (unsigned long long)epoch_ns,
            (unsigned long long)monotonic_ns,
            (unsigned long long)emitted,
            (unsigned long long)reserve_failed,
            program_count,
            writer->sampling_profile) < 0)
        return -EIO;
    return fflush(writer->output) == 0 ? 0 : -EIO;
}

static int expire_dns(
    struct writer_context *writer,
    int pending_fd,
    uint64_t timeout_ns)
{
    struct proberca_burst_dns_key current;
    struct proberca_burst_dns_key next;
    struct proberca_burst_dns_pending pending;
    bool have_current = false;
    uint64_t now = clock_ns(CLOCK_MONOTONIC);

    while (bpf_map_get_next_key(
               pending_fd, have_current ? &current : NULL, &next) == 0) {
        if (bpf_map_lookup_elem(pending_fd, &next, &pending) == 0 &&
            now >= pending.started_ns &&
            now - pending.started_ns >= timeout_ns) {
            struct proberca_burst_event event = {
                .monotonic_ns = pending.started_ns + timeout_ns,
                .cgroup_id = next.cgroup_id,
                .value = 1,
                .event_type = PROBERCA_BURST_DNS_TIMEOUT,
                .src_ipv4 = pending.client_ipv4,
                .dst_ipv4 = next.server_ipv4,
                .src_port = next.client_port,
                .dst_port = PROBERCA_BURST_DNS_PORT,
                .protocol = PROBERCA_BURST_IPPROTO_UDP,
                .direction = 1,
                .transaction_id = next.transaction_id,
                .sampling_divisor = 1,
            };
            if (write_event(writer, &event) != 0)
                return -EIO;
            if (bpf_map_delete_elem(pending_fd, &next) != 0 &&
                errno != ENOENT)
                return -errno;
        }
        current = next;
        have_current = true;
    }
    return errno == ENOENT ? 0 : -errno;
}

static int run(const struct options *options)
{
    struct bpf_object *object = NULL;
    struct bpf_link *links[MAX_LINKS] = {};
    struct ring_buffer *ring = NULL;
    struct writer_context writer = {};
    struct rlimit limit = {RLIM_INFINITY, RLIM_INFINITY};
    struct bpf_map *events_map;
    struct bpf_map *loss_map;
    struct bpf_map *pending_map;
    struct bpf_map *sampling_map;
    size_t link_count = 0;
    uint64_t next_checkpoint_ns = 0;
    int cgroup_fd = -1;
    int result = 1;
    size_t index;

    if (setrlimit(RLIMIT_MEMLOCK, &limit) != 0) {
        perror("setrlimit(RLIMIT_MEMLOCK)");
        goto cleanup;
    }
    writer.output = fopen(options->output_path, "a");
    if (!writer.output) {
        perror("open Burst event output");
        goto cleanup;
    }
    if (setvbuf(writer.output, NULL, _IOLBF, 0) != 0) {
        perror("setvbuf");
        goto cleanup;
    }
    writer.epoch_offset_ns =
        clock_ns(CLOCK_REALTIME) - clock_ns(CLOCK_MONOTONIC);
    writer.sampling_profile = options->sampling_profile;
    object = bpf_object__open_file(options->object_path, NULL);
    if (libbpf_get_error(object)) {
        object = NULL;
        fprintf(stderr, "cannot open final Burst BPF object\n");
        goto cleanup;
    }
    if (configure_program_scope(object, options->dns_only) != 0) {
        fprintf(stderr, "cannot configure final Burst program scope\n");
        goto cleanup;
    }
    if (bpf_object__load(object) != 0) {
        fprintf(stderr, "cannot load final Burst BPF object\n");
        goto cleanup;
    }
    sampling_map = bpf_object__find_map_by_name(object, "burst_sampling");
    if (!sampling_map ||
        configure_sampling(
            bpf_map__fd(sampling_map), options->sampling_profile) != 0) {
        fprintf(stderr, "cannot configure final Burst sampling\n");
        goto cleanup;
    }
    cgroup_fd = open(options->cgroup_path, O_RDONLY | O_DIRECTORY);
    if (cgroup_fd < 0) {
        perror("open cgroup");
        goto cleanup;
    }
    if (attach_programs(object, cgroup_fd, links, &link_count) != 0) {
        fprintf(stderr, "cannot attach final Burst probes\n");
        goto cleanup;
    }
    events_map = bpf_object__find_map_by_name(object, "burst_events");
    loss_map = bpf_object__find_map_by_name(object, "burst_loss");
    pending_map = bpf_object__find_map_by_name(
        object, "burst_dns_pending");
    if (!events_map || !loss_map || !pending_map) {
        fprintf(stderr, "final Burst object lacks required maps\n");
        goto cleanup;
    }
    ring = ring_buffer__new(
        bpf_map__fd(events_map), handle_event, &writer, NULL);
    if (libbpf_get_error(ring)) {
        ring = NULL;
        fprintf(stderr, "cannot create final Burst ring reader\n");
        goto cleanup;
    }
    if (fprintf(
            writer.output,
            "{\"record_type\":\"control\",\"schema_version\":%u,"
            "\"state\":\"ready\",\"timestamp_ns\":%llu,"
            "\"program_count\":%zu,\"timeout_ms\":%llu,"
            "\"sampling_profile\":\"%s\",\"dns_only\":%s}\n",
            PROBERCA_BURST_SCHEMA_VERSION,
            (unsigned long long)clock_ns(CLOCK_REALTIME),
            link_count,
            (unsigned long long)options->timeout_ms,
            options->sampling_profile,
            options->dns_only ? "true" : "false") < 0)
        goto cleanup;
    fflush(writer.output);
    next_checkpoint_ns = clock_ns(CLOCK_MONOTONIC);
    while (!stopping) {
        int poll_result = ring_buffer__poll(ring, 100);
        uint64_t now;

        if (poll_result < 0 && poll_result != -EINTR) {
            fprintf(stderr, "final Burst ring poll failed\n");
            goto cleanup;
        }
        if (expire_dns(
                &writer, bpf_map__fd(pending_map),
                options->timeout_ms * 1000000ULL) != 0) {
            fprintf(stderr, "final Burst DNS expiry failed\n");
            goto cleanup;
        }
        now = clock_ns(CLOCK_MONOTONIC);
        if (now >= next_checkpoint_ns) {
            if (write_checkpoint(
                    &writer, bpf_map__fd(loss_map), link_count) != 0) {
                fprintf(stderr, "cannot write final Burst checkpoint\n");
                goto cleanup;
            }
            next_checkpoint_ns =
                ((now / 250000000ULL) + 1) * 250000000ULL;
        }
    }
    result = 0;

cleanup:
    if (ring)
        ring_buffer__free(ring);
    for (index = 0; index < link_count; index++)
        bpf_link__destroy(links[index]);
    if (cgroup_fd >= 0)
        close(cgroup_fd);
    if (object)
        bpf_object__close(object);
    if (writer.output)
        fclose(writer.output);
    return result;
}

int main(int argc, char **argv)
{
    struct options options;

    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    if (parse_options(argc, argv, &options) != 0) {
        fprintf(
            stderr,
            "usage: %s --object PATH [--cgroup PATH] --output PATH "
            "[--timeout-ms N] [--sampling-profile low|full] [--dns-only]\n",
            argv[0]);
        return 2;
    }
    return run(&options);
}
