#define _GNU_SOURCE
#include <getopt.h>
#include <errno.h>
#include <fcntl.h>
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
#include "event.h"
#include "map_contract.h"

#define MAX_LINKS 64
#define MAX_CANDIDATES P12_MAX_CANDIDATES

struct options {
    const char *object_path;
    const char *probe_name;
    const char *cgroup_path;
    unsigned int ttl_sec;
    uint64_t attach_epoch;
    uint64_t candidate_version;
    uint64_t cgroups[MAX_CANDIDATES];
    size_t cgroup_count;
    struct p12_edge_key edges[MAX_CANDIDATES];
    size_t edge_count;
    bool print_abi;
};

struct runtime {
    struct options options;
    uint64_t events_received;
    uint64_t last_sequence;
};

static volatile sig_atomic_t stopping;
static void on_signal(int signo) { (void)signo; stopping = 1; }

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void control(const char *state)
{
    printf("{\"record_type\":\"control\",\"state\":\"%s\"}\n", state);
    fflush(stdout);
}

static void json_comm(const char comm[P12_TASK_COMM_LEN])
{
    unsigned int i;
    putchar('"');
    for (i = 0; i < P12_TASK_COMM_LEN && comm[i]; i++) {
        unsigned char ch = (unsigned char)comm[i];
        if (ch == '"' || ch == '\\') { putchar('\\'); putchar(ch); }
        else if (ch >= 0x20 && ch < 0x7f) putchar(ch);
        else printf("\\u%04x", ch);
    }
    putchar('"');
}

static int handle_event(void *context, void *data, size_t size)
{
    struct runtime *runtime = context;
    const struct p12_event *event = data;
    if (size != sizeof(*event) || event->size != sizeof(*event) ||
        event->schema_version != P12_EVENT_SCHEMA_VERSION) {
        fprintf(stderr, "event ABI mismatch: size=%zu event_size=%u schema=%u\n",
                size, event->size, event->schema_version);
        stopping = 1;
        return -EPROTO;
    }
    runtime->events_received++;
    runtime->last_sequence = event->event_sequence;
    printf("{\"record_type\":\"event\",\"probe_name\":\"%s\","
           "\"schema_version\":%u,\"event_type\":%u,\"event_class\":%u,"
           "\"quality\":%u,\"timestamp_ns\":%llu,"
           "\"process_start_time_ns\":%llu,\"cgroup_id\":%llu,"
           "\"src_cgroup_id\":%llu,\"dst_cgroup_id\":%llu,"
           "\"value\":%llu,\"duration_ns\":%llu,\"attach_epoch\":%llu,"
           "\"event_sequence\":%llu,\"cpu\":%u,\"pid\":%u,\"tid\":%u,"
           "\"src_ipv4\":%u,\"dst_ipv4\":%u,\"src_port\":%u,"
           "\"dst_port\":%u,\"protocol\":%u,\"direction\":%u,"
           "\"mapping_status\":%u,\"comm\":",
           runtime->options.probe_name, event->schema_version, event->event_type,
           event->event_class, event->quality,
           (unsigned long long)event->timestamp_ns,
           (unsigned long long)event->process_start_time_ns,
           (unsigned long long)event->cgroup_id,
           (unsigned long long)event->src_cgroup_id,
           (unsigned long long)event->dst_cgroup_id,
           (unsigned long long)event->value,
           (unsigned long long)event->duration_ns,
           (unsigned long long)event->attach_epoch,
           (unsigned long long)event->event_sequence,
           event->cpu, event->pid, event->tid, event->src_ipv4, event->dst_ipv4,
           event->src_port, event->dst_port, event->protocol, event->direction,
           event->mapping_status);
    json_comm(event->comm);
    puts("}");
    fflush(stdout);
    return 0;
}

static int parse_u64(const char *text, uint64_t *value)
{
    char *end = NULL;
    errno = 0;
    *value = strtoull(text, &end, 10);
    return errno || !end || *end ? -EINVAL : 0;
}

static int parse_edge(const char *text, struct p12_edge_key *edge)
{
    char *copy = strdup(text), *separator;
    int result = -EINVAL;
    if (!copy) return -ENOMEM;
    separator = strchr(copy, ':');
    if (separator) {
        uint64_t source, target;
        *separator = 0;
        if (!parse_u64(copy, &source) && !parse_u64(separator + 1, &target)) {
            edge->source_cgroup_id = source;
            edge->target_cgroup_id = target;
            result = 0;
        }
    }
    free(copy);
    return result;
}

static int parse_options(int argc, char **argv, struct options *options)
{
    enum { OPT_OBJECT = 1000, OPT_PROBE, OPT_TTL, OPT_EPOCH, OPT_VERSION,
           OPT_CGROUP, OPT_EDGE, OPT_CGROUP_PATH, OPT_PRINT_ABI };
    static const struct option long_options[] = {
        {"object", required_argument, NULL, OPT_OBJECT},
        {"probe", required_argument, NULL, OPT_PROBE},
        {"ttl", required_argument, NULL, OPT_TTL},
        {"attach-epoch", required_argument, NULL, OPT_EPOCH},
        {"candidate-version", required_argument, NULL, OPT_VERSION},
        {"candidate-cgroup", required_argument, NULL, OPT_CGROUP},
        {"candidate-edge", required_argument, NULL, OPT_EDGE},
        {"cgroup-path", required_argument, NULL, OPT_CGROUP_PATH},
        {"print-abi", no_argument, NULL, OPT_PRINT_ABI},
        {NULL, 0, NULL, 0},
    };
    int option;
    options->ttl_sec = 30;
    options->cgroup_path = "/sys/fs/cgroup";
    while ((option = getopt_long(argc, argv, "", long_options, NULL)) != -1) {
        uint64_t value;
        switch (option) {
        case OPT_OBJECT: options->object_path = optarg; break;
        case OPT_PROBE: options->probe_name = optarg; break;
        case OPT_TTL:
            if (parse_u64(optarg, &value) || !value || value > 3600) return -EINVAL;
            options->ttl_sec = value; break;
        case OPT_EPOCH:
            if (parse_u64(optarg, &options->attach_epoch)) return -EINVAL;
            break;
        case OPT_VERSION:
            if (parse_u64(optarg, &options->candidate_version)) return -EINVAL;
            break;
        case OPT_CGROUP:
            if (options->cgroup_count >= MAX_CANDIDATES ||
                parse_u64(optarg, &options->cgroups[options->cgroup_count])) return -EINVAL;
            options->cgroup_count++; break;
        case OPT_EDGE:
            if (options->edge_count >= MAX_CANDIDATES ||
                parse_edge(optarg, &options->edges[options->edge_count])) return -EINVAL;
            options->edge_count++; break;
        case OPT_CGROUP_PATH: options->cgroup_path = optarg; break;
        case OPT_PRINT_ABI: options->print_abi = true; break;
        default: return -EINVAL;
        }
    }
    if (options->print_abi) return 0;
    if (!options->object_path || !options->probe_name || !options->attach_epoch ||
        !options->candidate_version || !options->cgroup_count) return -EINVAL;
    return 0;
}

static int delete_stale_cgroups(int fd, uint64_t generation)
{
    uint64_t current, next, value;
    int result = bpf_map_get_next_key(fd, NULL, &next);
    while (!result) {
        current = next;
        result = bpf_map_get_next_key(fd, &current, &next);
        if (!bpf_map_lookup_elem(fd, &current, &value) && value != generation)
            bpf_map_delete_elem(fd, &current);
    }
    return result == -1 && errno != ENOENT ? -errno : 0;
}

static int delete_stale_edges(int fd, uint64_t generation)
{
    struct p12_edge_key current, next;
    uint64_t value;
    int result = bpf_map_get_next_key(fd, NULL, &next);
    while (!result) {
        current = next;
        result = bpf_map_get_next_key(fd, &current, &next);
        if (!bpf_map_lookup_elem(fd, &current, &value) && value != generation)
            bpf_map_delete_elem(fd, &current);
    }
    return result == -1 && errno != ENOENT ? -errno : 0;
}

static int replace_candidate_maps(
    struct bpf_object *object, uint64_t attach_epoch, uint64_t generation,
    const uint64_t *cgroups, size_t cgroup_count,
    const struct p12_edge_key *edges, size_t edge_count)
{
    struct bpf_map *cgroup_map, *edge_map, *config_map;
    struct p12_config cfg = {
        .attach_epoch = attach_epoch,
        .candidate_version = generation,
        .enabled = 1,
        .edge_pair_count = edge_count,
    };
    uint32_t zero = 0;
    size_t index;
    int cgroup_fd, edge_fd;
    if (!generation || !cgroup_count || cgroup_count > MAX_CANDIDATES ||
        edge_count > MAX_CANDIDATES) return -EINVAL;
    cgroup_map = bpf_object__find_map_by_name(object, "candidate_cgroups");
    edge_map = bpf_object__find_map_by_name(object, "candidate_edges");
    config_map = bpf_object__find_map_by_name(object, "p12_config_map");
    if (!cgroup_map || !edge_map || !config_map) return -ENOENT;
    cgroup_fd = bpf_map__fd(cgroup_map);
    edge_fd = bpf_map__fd(edge_map);
    for (index = 0; index < cgroup_count; index++)
        if (bpf_map_update_elem(cgroup_fd, &cgroups[index], &generation, BPF_ANY))
            return -errno;
    for (index = 0; index < edge_count; index++)
        if (bpf_map_update_elem(edge_fd, &edges[index], &generation, BPF_ANY))
            return -errno;
    /* Publish the generation only after every candidate entry exists. */
    if (bpf_map_update_elem(bpf_map__fd(config_map), &zero, &cfg, BPF_ANY))
        return -errno;
    delete_stale_cgroups(cgroup_fd, generation);
    delete_stale_edges(edge_fd, generation);
    return 0;
}

static int configure_maps(struct bpf_object *object, const struct options *options)
{
    return replace_candidate_maps(
        object, options->attach_epoch, options->candidate_version,
        options->cgroups, options->cgroup_count, options->edges, options->edge_count);
}

static int parse_csv_cgroups(char *text, uint64_t *values, size_t *count)
{
    char *save = NULL, *token;
    *count = 0;
    if (!strcmp(text, "-")) return 0;
    for (token = strtok_r(text, ",", &save); token; token = strtok_r(NULL, ",", &save)) {
        if (*count >= MAX_CANDIDATES || parse_u64(token, &values[*count])) return -EINVAL;
        (*count)++;
    }
    return *count ? 0 : -EINVAL;
}

static int parse_csv_edges(char *text, struct p12_edge_key *values, size_t *count)
{
    char *save = NULL, *token;
    *count = 0;
    if (!strcmp(text, "-")) return 0;
    for (token = strtok_r(text, ",", &save); token; token = strtok_r(NULL, ",", &save)) {
        if (*count >= MAX_CANDIDATES || parse_edge(token, &values[*count])) return -EINVAL;
        (*count)++;
    }
    return 0;
}

static int apply_update_line(struct bpf_object *object, const struct options *options, char *line)
{
    uint64_t *cgroups = calloc(MAX_CANDIDATES, sizeof(*cgroups));
    struct p12_edge_key *edges = calloc(MAX_CANDIDATES, sizeof(*edges));
    char *save = NULL, *verb, *version_text, *cgroup_text, *edge_text;
    uint64_t generation;
    size_t cgroup_count, edge_count;
    int result = -ENOMEM;
    if (!cgroups || !edges) goto done;
    verb = strtok_r(line, " \n", &save);
    version_text = strtok_r(NULL, " \n", &save);
    cgroup_text = strtok_r(NULL, " \n", &save);
    edge_text = strtok_r(NULL, " \n", &save);
    if (!verb || strcmp(verb, "replace") || !version_text || !cgroup_text || !edge_text ||
        parse_u64(version_text, &generation) ||
        parse_csv_cgroups(cgroup_text, cgroups, &cgroup_count) ||
        parse_csv_edges(edge_text, edges, &edge_count)) {
        result = -EINVAL;
        goto done;
    }
    result = replace_candidate_maps(object, options->attach_epoch, generation,
                                    cgroups, cgroup_count, edges, edge_count);
done:
    free(cgroups);
    free(edges);
    return result;
}

static int read_candidate_updates(struct bpf_object *object, const struct options *options)
{
    static char buffer[262144];
    static size_t used;
    ssize_t count;
    char *newline;
    count = read(STDIN_FILENO, buffer + used, sizeof(buffer) - used - 1);
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
    if (count < 0) return -errno;
    if (!count) return 0;
    used += count;
    buffer[used] = 0;
    while ((newline = memchr(buffer, '\n', used))) {
        size_t line_size = (size_t)(newline - buffer) + 1;
        int result;
        *newline = 0;
        result = apply_update_line(object, options, buffer);
        memmove(buffer, buffer + line_size, used - line_size);
        used -= line_size;
        if (result) return result;
        printf("{\"record_type\":\"control\",\"state\":\"CANDIDATES_UPDATED\"}\n");
        fflush(stdout);
    }
    return used == sizeof(buffer) - 1 ? -E2BIG : 0;
}

static int attach_programs(struct bpf_object *object, const struct options *options,
                           struct bpf_link **links, size_t *link_count)
{
    struct bpf_program *program;
    int cgroup_fd = -1;
    bpf_object__for_each_program(program, object) {
        const char *section = bpf_program__section_name(program);
        struct bpf_link *link;
        if (*link_count >= MAX_LINKS) return -E2BIG;
        if (!strncmp(section, "cgroup_skb/", 11)) {
            if (cgroup_fd < 0) {
                cgroup_fd = open(options->cgroup_path, O_RDONLY | O_DIRECTORY);
                if (cgroup_fd < 0) return -errno;
            }
            link = bpf_program__attach_cgroup(program, cgroup_fd);
        } else {
            link = bpf_program__attach(program);
        }
        if (libbpf_get_error(link)) {
            int error = -libbpf_get_error(link);
            if (cgroup_fd >= 0) close(cgroup_fd);
            return error;
        }
        links[(*link_count)++] = link;
    }
    if (cgroup_fd >= 0) close(cgroup_fd);
    return *link_count ? 0 : -ENOENT;
}

static int sum_loss(struct bpf_object *object, uint64_t *drops, uint64_t *filtered)
{
    struct bpf_map *map = bpf_object__find_map_by_name(object, "loss_counters");
    int cpu_count = libbpf_num_possible_cpus(), cpu;
    struct p12_loss_counters *values;
    uint32_t zero = 0;
    if (!map || cpu_count <= 0) return -ENOENT;
    values = calloc(cpu_count, sizeof(*values));
    if (!values) return -ENOMEM;
    if (bpf_map_lookup_elem(bpf_map__fd(map), &zero, values)) {
        int error = -errno; free(values); return error;
    }
    for (cpu = 0; cpu < cpu_count; cpu++) {
        *drops += values[cpu].reserve_failed;
        *filtered += values[cpu].filtered;
    }
    free(values);
    return 0;
}

int main(int argc, char **argv)
{
    struct options options = {};
    struct runtime runtime = {};
    struct bpf_object *object = NULL;
    struct bpf_map *event_map;
    struct ring_buffer *ring = NULL;
    struct bpf_link *links[MAX_LINKS] = {};
    size_t link_count = 0, index;
    uint64_t active_at, drain_until, drops = 0, filtered = 0;
    struct rlimit limit = {RLIM_INFINITY, RLIM_INFINITY};
    int result;

    result = parse_options(argc, argv, &options);
    if (result) { fprintf(stderr, "invalid arguments\n"); return 2; }
    if (options.print_abi) {
        printf("{\"schema_version\":%d,\"event_size\":%zu}\n",
               P12_EVENT_SCHEMA_VERSION, sizeof(struct p12_event));
        return sizeof(struct p12_event) == P12_EVENT_ABI_SIZE ? 0 : 3;
    }
    runtime.options = options;
    signal(SIGINT, on_signal); signal(SIGTERM, on_signal);
    setrlimit(RLIMIT_MEMLOCK, &limit);
    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);
    object = bpf_object__open_file(options.object_path, NULL);
    result = libbpf_get_error(object);
    if (result) { object = NULL; fprintf(stderr, "open_failed:%s\n", strerror(-result)); goto done; }
    result = bpf_object__load(object);
    if (result) { fprintf(stderr, "verifier_rejected:%s\n", strerror(-result)); goto done; }
    result = configure_maps(object, &options);
    if (result) { fprintf(stderr, "map_configuration_failed:%s\n", strerror(-result)); goto done; }
    result = attach_programs(object, &options, links, &link_count);
    if (result) { fprintf(stderr, "attach_failed:%s\n", strerror(-result)); goto done; }
    control("ATTACHED");
    event_map = bpf_object__find_map_by_name(object, "events");
    if (!event_map) { result = -ENOENT; goto done; }
    ring = ring_buffer__new(bpf_map__fd(event_map), handle_event, &runtime, NULL);
    result = libbpf_get_error(ring);
    if (result) { ring = NULL; fprintf(stderr, "ring_buffer_failed:%s\n", strerror(-result)); goto done; }
    control("ACTIVE");
    fcntl(STDIN_FILENO, F_SETFL, fcntl(STDIN_FILENO, F_GETFL, 0) | O_NONBLOCK);
    active_at = monotonic_ns();
    while (!stopping && monotonic_ns() - active_at < (uint64_t)options.ttl_sec * 1000000000ULL) {
        result = ring_buffer__poll(ring, 100);
        if (result == -EINTR) { result = 0; continue; }
        if (result < 0) { fprintf(stderr, "read_failed:%s\n", strerror(-result)); goto done; }
        result = read_candidate_updates(object, &options);
        if (result) { fprintf(stderr, "candidate_update_failed:%s\n", strerror(-result)); goto done; }
    }
    control("DRAINING");
    drain_until = monotonic_ns() + 250000000ULL;
    while (monotonic_ns() < drain_until) {
        result = ring_buffer__poll(ring, 25);
        if (result < 0 && result != -EINTR) goto done;
    }
    result = 0;
    control("DETACHING");
done:
    if (object) sum_loss(object, &drops, &filtered);
    if (ring) ring_buffer__free(ring);
    for (index = link_count; index > 0; index--) bpf_link__destroy(links[index - 1]);
    if (object) bpf_object__close(object);
    if (!result) control("CLOSED");
    printf("{\"record_type\":\"summary\",\"events_received\":%llu,"
           "\"ring_buffer_drops\":%llu,\"filtered_events\":%llu,"
           "\"last_event_sequence\":%llu,\"residual_links\":0}\n",
           (unsigned long long)runtime.events_received,
           (unsigned long long)drops, (unsigned long long)filtered,
           (unsigned long long)runtime.last_sequence);
    fflush(stdout);
    return result ? 1 : 0;
}
