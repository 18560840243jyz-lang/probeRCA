#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include "common.h"

#define P12_IPPROTO_TCP 6
#define P12_AF_INET 2
#define P12_TCP_ESTABLISHED 1
#define P12_TCP_SYN_SENT 2
#define P12_TCP_SYN_RECV 3
#define P12_TCP_CLOSE 7

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, __u64);
    __type(value, __u64);
} tcp_connect_starts SEC(".maps");


static __always_inline int emit_tcp_tuple(
    __u16 type, __u16 family, __u16 sport, __u16 dport,
    __u32 saddr, __u32 daddr, __u64 value)
{
    __u64 source = bpf_get_current_cgroup_id();
    struct p12_event *event;
    if (family != P12_AF_INET || !p12_edge_allowed(source, (__u64)daddr))
        return 0;
    event = p12_reserve();
    if (!event)
        return 0;
    p12_fill_common(event, type, P12_CLASS_EDGE, P12_QUALITY_PARTIAL);
    event->src_cgroup_id = source;
    event->dst_cgroup_id = 0;
    event->src_ipv4 = saddr;
    event->dst_ipv4 = daddr;
    event->src_port = sport;
    event->dst_port = dport;
    event->protocol = P12_IPPROTO_TCP;
    event->direction = 1;
    event->value = type == P12_TCP_RTT ? 1 : value;
    event->duration_ns = type == P12_TCP_RTT ? value : 0;
    event->mapping_status = 1;
    p12_submit(event);
    return 0;
}

SEC("tracepoint/tcp/tcp_retransmit_skb")
int tcp_retransmit(struct trace_event_raw_tcp_event_sk_skb *ctx)
{
    __u32 source = 0, target = 0;
    bpf_probe_read_kernel(&source, sizeof(source), ctx->saddr);
    bpf_probe_read_kernel(&target, sizeof(target), ctx->daddr);
    return emit_tcp_tuple(P12_TCP_RETRANSMIT, ctx->family, ctx->sport, ctx->dport,
                          source, target, 1);
}

SEC("tracepoint/tcp/tcp_receive_reset")
int tcp_receive_reset(struct trace_event_raw_tcp_event_sk *ctx)
{
    __u32 source = 0, target = 0;
    bpf_probe_read_kernel(&source, sizeof(source), ctx->saddr);
    bpf_probe_read_kernel(&target, sizeof(target), ctx->daddr);
    return emit_tcp_tuple(P12_TCP_RESET, ctx->family, ctx->sport, ctx->dport,
                          source, target, 1);
}

SEC("tracepoint/tcp/tcp_send_reset")
int tcp_send_reset(struct trace_event_raw_tcp_event_sk *ctx)
{
    __u32 source = 0, target = 0;
    bpf_probe_read_kernel(&source, sizeof(source), ctx->saddr);
    bpf_probe_read_kernel(&target, sizeof(target), ctx->daddr);
    return emit_tcp_tuple(P12_TCP_RESET, ctx->family, ctx->sport, ctx->dport,
                          source, target, 1);
}

SEC("tracepoint/sock/inet_sock_set_state")
int tcp_state(struct trace_event_raw_inet_sock_set_state *ctx)
{
    __u32 source = 0, target = 0;
    __u64 socket_key = (__u64)ctx->skaddr;
    __u64 now = bpf_ktime_get_ns();
    __u64 *started;
    __u32 oldstate = ctx->oldstate;
    __u32 newstate = ctx->newstate;
    __u16 family = ctx->family;
    __u16 sport = ctx->sport;
    __u16 dport = ctx->dport;
    __u16 protocol = ctx->protocol;

    /* Tracepoint context pointers cannot be reused across map helper calls. */
    if (protocol != P12_IPPROTO_TCP)
        return 0;
    bpf_probe_read_kernel(&source, sizeof(source), ctx->saddr);
    bpf_probe_read_kernel(&target, sizeof(target), ctx->daddr);
    if (newstate == P12_TCP_SYN_SENT) {
        bpf_map_update_elem(&tcp_connect_starts, &socket_key, &now, BPF_ANY);
        return 0;
    }
    if (newstate == P12_TCP_CLOSE &&
        (oldstate == P12_TCP_SYN_SENT || oldstate == P12_TCP_SYN_RECV)) {
        bpf_map_delete_elem(&tcp_connect_starts, &socket_key);
        return emit_tcp_tuple(P12_TCP_CONNECT_FAIL, family, sport,
                              dport, source, target, oldstate);
    }
    if (newstate == P12_TCP_ESTABLISHED) {
        started = bpf_map_lookup_elem(&tcp_connect_starts, &socket_key);
        if (started) {
            __u64 duration = now - *started;
            bpf_map_delete_elem(&tcp_connect_starts, &socket_key);
            return emit_tcp_tuple(P12_TCP_RTT, family, sport,
                                  dport, source, target, duration);
        }
    }
    if (newstate == P12_TCP_CLOSE)
        bpf_map_delete_elem(&tcp_connect_starts, &socket_key);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
