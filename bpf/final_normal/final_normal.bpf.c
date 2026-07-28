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
#define DNS_RCODE_NOERROR 0
#define DNS_RCODE_SERVFAIL 2
#define DNS_RCODE_NXDOMAIN 3
#define DNS_RCODE_REFUSED 5
#define DNS_FLAG_QR 0x8000
#define DNS_FLAG_TC 0x0200
#define FNV1A_OFFSET 1469598103934665603ULL
#define FNV1A_PRIME 1099511628211ULL

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
    __type(value, struct proberca_final_dns_pending_value);
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
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct proberca_final_dns_scratch);
} dns_scratch SEC(".maps");

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

static __always_inline int parse_dns_question(
    struct __sk_buff *skb,
    __u32 question_offset,
    char qname[PROBERCA_FINAL_DNS_QNAME_MAX],
    __u64 *qname_hash,
    __be16 *qtype)
{
    __u64 hash = FNV1A_OFFSET;
    __u32 output_index = 0;
    __u32 consumed = 0;
    __u8 label_remaining = 0;
    bool ended = false;
    int index;

#pragma unroll
    for (index = 0; index < PROBERCA_FINAL_DNS_QNAME_MAX; index++) {
        __u8 byte = 0;

        if (bpf_skb_load_bytes(
                skb, question_offset + index, &byte, sizeof(byte)) < 0)
            return -1;
        consumed = index + 1;
        if (!label_remaining) {
            if (!byte) {
                if (!output_index ||
                    output_index >= PROBERCA_FINAL_DNS_QNAME_MAX - 1)
                    return -1;
                qname[output_index++] = '.';
                hash = (hash ^ (__u8)'.') * FNV1A_PRIME;
                qname[output_index] = '\0';
                ended = true;
                break;
            }
            if ((byte & 0xc0) || byte > 63)
                return -1;
            if (output_index) {
                if (output_index >= PROBERCA_FINAL_DNS_QNAME_MAX - 1)
                    return -1;
                qname[output_index++] = '.';
                hash = (hash ^ (__u8)'.') * FNV1A_PRIME;
            }
            label_remaining = byte;
            continue;
        }
        if (output_index >= PROBERCA_FINAL_DNS_QNAME_MAX - 1)
            return -1;
        if (byte >= 'A' && byte <= 'Z')
            byte += 'a' - 'A';
        qname[output_index++] = byte;
        hash = (hash ^ byte) * FNV1A_PRIME;
        label_remaining--;
    }
    if (!ended || label_remaining)
        return -1;
    if (bpf_skb_load_bytes(
            skb, question_offset + consumed, qtype, sizeof(*qtype)) < 0)
        return -1;
    *qname_hash = hash;
    return 0;
}

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

static __always_inline void record_dns_parse_failure(
    struct proberca_final_dns_scratch *scratch)
{
    struct proberca_final_dns_edge_counters *counters;

    /*
     * Emit an intentionally invalid qname/qtype coordinate instead of
     * silently dropping an unparseable transaction.  The userspace exporter
     * rejects this record and keeps the DNS coordinate Not Ready.
     */
    scratch->pending.edge.cgroup_id =
        scratch->pending_key.cgroup_id;
    scratch->pending.edge.server_ipv4 =
        scratch->pending_key.server_ipv4;
    counters = get_dns_counters(&scratch->pending.edge);
    if (counters) {
        __sync_fetch_and_add(&counters->query_total, 1);
        __sync_fetch_and_add(
            &counters->transport_error_total, 1);
    }
}

static __always_inline int inspect_dns(struct __sk_buff *skb, bool egress)
{
    struct ipv4_header ip = {};
    struct udp_header udp = {};
    struct dns_header dns = {};
    struct proberca_final_dns_scratch *scratch;
    struct proberca_final_dns_pending_value *started;
    struct proberca_final_dns_edge_counters *counters;
    __u64 now;
    __u32 scratch_key = 0;
    __u32 ip_header_length;
    __u32 question_offset;
    __u16 source_port;
    __u16 destination_port;
    __u16 flags;
    __u16 rcode;
    int update_result;

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
    scratch = bpf_map_lookup_elem(&dns_scratch, &scratch_key);
    if (!scratch)
        return 1;
    __builtin_memset(scratch, 0, sizeof(*scratch));
    scratch->pending_key.cgroup_id = bpf_skb_cgroup_id(skb);
    if (!scratch->pending_key.cgroup_id)
        return 1;
    question_offset = ip_header_length + sizeof(udp) + sizeof(dns);

    if (egress && destination_port == DNS_PORT && !(flags & DNS_FLAG_QR)) {
        scratch->pending_key.client_ipv4 = ip.source;
        scratch->pending_key.server_ipv4 = ip.destination;
        scratch->pending_key.client_port = udp.source;
        scratch->pending_key.transaction_id = dns.transaction_id;
        if (parse_dns_question(
                skb, question_offset, scratch->pending.edge.qname,
                &scratch->pending_key.qname_hash,
                &scratch->pending_key.qtype) != 0) {
            record_dns_parse_failure(scratch);
            return 1;
        }
        scratch->pending.edge.cgroup_id =
            scratch->pending_key.cgroup_id;
        scratch->pending.edge.server_ipv4 =
            scratch->pending_key.server_ipv4;
        scratch->pending.edge.qname_hash =
            scratch->pending_key.qname_hash;
        scratch->pending.edge.qtype = scratch->pending_key.qtype;
        scratch->pending.started_ns = now;
        update_result = bpf_map_update_elem(
            &dns_pending, &scratch->pending_key,
            &scratch->pending, BPF_NOEXIST);
        if (update_result != 0) {
            started = bpf_map_lookup_elem(
                &dns_pending, &scratch->pending_key);
            if (!started)
                return 1;
            __sync_fetch_and_add(&started->retry_count, 1);
            counters = get_dns_counters(&started->edge);
            if (counters)
                __sync_fetch_and_add(&counters->retry_total, 1);
            return 1;
        }
        counters = get_dns_counters(&scratch->pending.edge);
        if (counters)
            __sync_fetch_and_add(&counters->query_total, 1);
        return 1;
    }
    if (!egress && source_port == DNS_PORT && (flags & DNS_FLAG_QR)) {
        scratch->pending_key.client_ipv4 = ip.destination;
        scratch->pending_key.server_ipv4 = ip.source;
        scratch->pending_key.client_port = udp.destination;
        scratch->pending_key.transaction_id = dns.transaction_id;
        if (parse_dns_question(
                skb, question_offset, scratch->pending.edge.qname,
                &scratch->pending_key.qname_hash,
                &scratch->pending_key.qtype) != 0) {
            record_dns_parse_failure(scratch);
            return 1;
        }
        started = bpf_map_lookup_elem(
            &dns_pending, &scratch->pending_key);
        if (!started)
            return 1;
        counters = get_dns_counters(&started->edge);
        if (counters) {
            if (flags & DNS_FLAG_TC) {
                __sync_fetch_and_add(&counters->truncated_total, 1);
                return 1;
            }
            rcode = flags & 0x000f;
            if (rcode == DNS_RCODE_NOERROR) {
                __sync_fetch_and_add(&counters->success_total, 1);
                if (now >= started->started_ns)
                    add_dns_latency(
                        counters, now - started->started_ns);
            } else if (rcode == DNS_RCODE_SERVFAIL) {
                __sync_fetch_and_add(&counters->servfail_total, 1);
            } else if (rcode == DNS_RCODE_NXDOMAIN) {
                __sync_fetch_and_add(&counters->nxdomain_total, 1);
            } else if (rcode == DNS_RCODE_REFUSED) {
                __sync_fetch_and_add(&counters->refused_total, 1);
            } else {
                __sync_fetch_and_add(
                    &counters->transport_error_total, 1);
            }
        }
        bpf_map_delete_elem(
            &dns_pending, &scratch->pending_key);
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
