#include "vmlinux.h"
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#include "final_normal.h"

#define FUTEX_CMD_MASK 0x7f
#define FUTEX_WAIT 0
#define FUTEX_WAIT_BITSET 9
#define FUTEX_WAIT_REQUEUE_PI 11
#define EINPROGRESS 115
#define EALREADY 114
#define EINTR 4
#define IPPROTO_UDP_VALUE 17
#define DNS_PORT 53

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, PROBERCA_FINAL_MAX_CGROUPS);
    __type(key, __u64);
    __type(value, struct proberca_final_cgroup_counters);
} cgroup_counters SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_FINAL_MAX_DNS_PENDING);
    __type(key, struct proberca_final_dns_pending_key);
    __type(value, __u64);
} dns_pending SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, PROBERCA_FINAL_MAX_DNS_EDGES);
    __type(key, struct proberca_final_dns_edge_key);
    __type(value, struct proberca_final_dns_edge_counters);
} dns_edge_counters SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, PROBERCA_FINAL_MAX_DNS_EDGES);
    __type(key, struct proberca_final_dns_edge_key);
    __type(value, __u64);
} dns_timeout_counters SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_FINAL_MAX_DNS_PENDING);
    __type(key, __u64);
    __type(value, struct proberca_final_futex_start);
} futex_starts SEC(".maps");

static __always_inline struct proberca_final_cgroup_counters *
get_cgroup_counters(__u64 cgroup_id)
{
    struct proberca_final_cgroup_counters zero = {};
    struct proberca_final_cgroup_counters *value;

    value = bpf_map_lookup_elem(&cgroup_counters, &cgroup_id);
    if (value)
        return value;
    bpf_map_update_elem(&cgroup_counters, &cgroup_id, &zero, BPF_NOEXIST);
    return bpf_map_lookup_elem(&cgroup_counters, &cgroup_id);
}

static __always_inline struct proberca_final_dns_edge_counters *
get_dns_counters(const struct proberca_final_dns_edge_key *key)
{
    struct proberca_final_dns_edge_counters zero = {};
    struct proberca_final_dns_edge_counters *value;

    value = bpf_map_lookup_elem(&dns_edge_counters, key);
    if (value)
        return value;
    bpf_map_update_elem(&dns_edge_counters, key, &zero, BPF_NOEXIST);
    return bpf_map_lookup_elem(&dns_edge_counters, key);
}

static __always_inline bool is_futex_wait(__u64 operation)
{
    __u64 command = operation & FUTEX_CMD_MASK;

    return command == FUTEX_WAIT || command == FUTEX_WAIT_BITSET ||
           command == FUTEX_WAIT_REQUEUE_PI;
}

SEC("tracepoint/syscalls/sys_enter_futex")
int final_futex_enter(struct trace_event_raw_sys_enter *context)
{
    __u64 pid_tgid;
    struct proberca_final_futex_start start = {};

    if (!is_futex_wait(context->args[1]))
        return 0;
    pid_tgid = bpf_get_current_pid_tgid();
    start.started_ns = bpf_ktime_get_ns();
    start.cgroup_id = bpf_get_current_cgroup_id();
    bpf_map_update_elem(&futex_starts, &pid_tgid, &start, BPF_ANY);
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_futex")
int final_futex_exit(struct trace_event_raw_sys_exit *context)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct proberca_final_futex_start *start;
    struct proberca_final_cgroup_counters *counters;
    __u64 now;

    (void)context;
    start = bpf_map_lookup_elem(&futex_starts, &pid_tgid);
    if (!start)
        return 0;
    now = bpf_ktime_get_ns();
    counters = get_cgroup_counters(start->cgroup_id);
    if (counters && now >= start->started_ns)
        __sync_fetch_and_add(
            &counters->futex_wait_ns_total, now - start->started_ns);
    bpf_map_delete_elem(&futex_starts, &pid_tgid);
    return 0;
}

static __always_inline int record_accept(long result)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct proberca_final_cgroup_counters *counters =
        get_cgroup_counters(cgroup_id);

    if (!counters)
        return 0;
    __sync_fetch_and_add(&counters->socket_ops_total, 1);
    if (result < 0)
        __sync_fetch_and_add(&counters->socket_accept_fail_total, 1);
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_accept")
int final_accept_exit(struct trace_event_raw_sys_exit *context)
{
    return record_accept(context->ret);
}

SEC("tracepoint/syscalls/sys_exit_accept4")
int final_accept4_exit(struct trace_event_raw_sys_exit *context)
{
    return record_accept(context->ret);
}

SEC("tracepoint/syscalls/sys_exit_connect")
int final_connect_exit(struct trace_event_raw_sys_exit *context)
{
    long result = context->ret;
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct proberca_final_cgroup_counters *counters =
        get_cgroup_counters(cgroup_id);

    if (!counters)
        return 0;
    __sync_fetch_and_add(&counters->socket_ops_total, 1);
    if (result < 0 && result != -EINPROGRESS && result != -EALREADY &&
        result != -EINTR)
        __sync_fetch_and_add(&counters->socket_accept_fail_total, 1);
    return 0;
}

SEC("tracepoint/tcp/tcp_send_reset")
int final_tcp_send_reset(void *context)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct proberca_final_cgroup_counters *counters =
        get_cgroup_counters(cgroup_id);

    (void)context;
    if (counters)
        __sync_fetch_and_add(&counters->socket_local_rst_total, 1);
    return 0;
}

SEC("tracepoint/sock/sock_exceed_buf_limit")
int final_socket_backlog_overflow(void *context)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct proberca_final_cgroup_counters *counters =
        get_cgroup_counters(cgroup_id);

    (void)context;
    if (counters)
        __sync_fetch_and_add(
            &counters->socket_backlog_overflow_total, 1);
    return 0;
}

SEC("tracepoint/sock/sock_rcvqueue_full")
int final_socket_local_drop(void *context)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct proberca_final_cgroup_counters *counters =
        get_cgroup_counters(cgroup_id);

    (void)context;
    if (counters)
        __sync_fetch_and_add(&counters->socket_local_drop_total, 1);
    return 0;
}

struct ipv4_header {
    __u8 version_ihl;
    __u8 tos;
    __be16 total_length;
    __be16 identification;
    __be16 fragment_offset;
    __u8 ttl;
    __u8 protocol;
    __sum16 checksum;
    __be32 source;
    __be32 destination;
};

struct udp_header {
    __be16 source;
    __be16 destination;
    __be16 length;
    __sum16 checksum;
};

struct dns_header {
    __be16 transaction_id;
    __be16 flags;
    __be16 question_count;
    __be16 answer_count;
    __be16 authority_count;
    __be16 additional_count;
};

static __always_inline void add_dns_latency(
    struct proberca_final_dns_edge_counters *counters, __u64 latency_ns)
{
    const __u64 bounds[PROBERCA_FINAL_DNS_BUCKETS - 1] = {
        100000ULL, 250000ULL, 500000ULL, 1000000ULL, 2000000ULL,
        5000000ULL, 10000000ULL, 20000000ULL, 50000000ULL,
        100000000ULL, 250000000ULL, 500000000ULL, 1000000000ULL,
        2000000000ULL, 5000000000ULL,
    };
    int index;

#pragma unroll
    for (index = 0; index < PROBERCA_FINAL_DNS_BUCKETS - 1; index++) {
        if (latency_ns <= bounds[index])
            __sync_fetch_and_add(&counters->latency_buckets[index], 1);
    }
    __sync_fetch_and_add(
        &counters->latency_buckets[PROBERCA_FINAL_DNS_BUCKETS - 1], 1);
}

static __always_inline int inspect_dns(struct __sk_buff *skb, bool egress)
{
    struct ipv4_header ip = {};
    struct udp_header udp = {};
    struct dns_header dns = {};
    struct proberca_final_dns_pending_key pending_key = {};
    struct proberca_final_dns_edge_key edge_key = {};
    struct proberca_final_dns_edge_counters *counters;
    __u64 *started_ns;
    __u64 now;
    __u32 ip_header_length;
    __u16 source_port;
    __u16 destination_port;
    __u16 flags;

    if (bpf_skb_load_bytes(skb, 0, &ip, sizeof(ip)) < 0 ||
        (ip.version_ihl >> 4) != 4 ||
        ip.protocol != IPPROTO_UDP_VALUE)
        return 1;
    ip_header_length = (ip.version_ihl & 0x0f) * 4;
    if (ip_header_length < sizeof(ip) ||
        bpf_skb_load_bytes(
            skb, ip_header_length, &udp, sizeof(udp)) < 0 ||
        bpf_skb_load_bytes(
            skb, ip_header_length + sizeof(udp), &dns, sizeof(dns)) < 0)
        return 1;
    source_port = bpf_ntohs(udp.source);
    destination_port = bpf_ntohs(udp.destination);
    if (source_port != DNS_PORT && destination_port != DNS_PORT)
        return 1;
    flags = bpf_ntohs(dns.flags);
    now = bpf_ktime_get_ns();
    pending_key.cgroup_id = bpf_skb_cgroup_id(skb);
    if (!pending_key.cgroup_id)
        return 1;

    if (egress && destination_port == DNS_PORT && !(flags & 0x8000)) {
        pending_key.server_ipv4 = ip.destination;
        pending_key.client_port = udp.source;
        pending_key.transaction_id = dns.transaction_id;
        edge_key.cgroup_id = pending_key.cgroup_id;
        edge_key.server_ipv4 = pending_key.server_ipv4;
        counters = get_dns_counters(&edge_key);
        if (counters)
            __sync_fetch_and_add(&counters->query_total, 1);
        bpf_map_update_elem(&dns_pending, &pending_key, &now, BPF_ANY);
        return 1;
    }
    if (!egress && source_port == DNS_PORT && (flags & 0x8000)) {
        pending_key.server_ipv4 = ip.source;
        pending_key.client_port = udp.destination;
        pending_key.transaction_id = dns.transaction_id;
        started_ns = bpf_map_lookup_elem(&dns_pending, &pending_key);
        if (!started_ns)
            return 1;
        edge_key.cgroup_id = pending_key.cgroup_id;
        edge_key.server_ipv4 = pending_key.server_ipv4;
        counters = get_dns_counters(&edge_key);
        if (counters) {
            if ((flags & 0x000f) != 0)
                __sync_fetch_and_add(&counters->error_rcode_total, 1);
            if (now >= *started_ns)
                add_dns_latency(counters, now - *started_ns);
        }
        bpf_map_delete_elem(&dns_pending, &pending_key);
    }
    return 1;
}

SEC("cgroup_skb/egress")
int final_dns_egress(struct __sk_buff *skb)
{
    return inspect_dns(skb, true);
}

SEC("cgroup_skb/ingress")
int final_dns_ingress(struct __sk_buff *skb)
{
    return inspect_dns(skb, false);
}

char LICENSE[] SEC("license") = "GPL";
