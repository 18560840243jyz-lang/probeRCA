#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include "common.h"

struct block_key { __u32 dev; __u32 padding; __u64 sector; };
struct block_state {
    __u64 timestamp_ns;
    __u64 cgroup_id;
    __u64 process_start_time_ns;
    __u32 pid;
    __u32 bytes;
    __u16 direction;
};
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, struct block_key);
    __type(value, struct block_state);
} block_requests SEC(".maps");

SEC("tracepoint/block/block_rq_issue")
int block_issue(struct trace_event_raw_block_rq *ctx)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct block_key key = { .dev = ctx->dev, .sector = ctx->sector };
    struct block_state state = {};
    struct p12_event *event;
    if (!p12_cgroup_allowed(cgroup_id))
        return 0;
    state.timestamp_ns = bpf_ktime_get_ns();
    state.cgroup_id = cgroup_id;
    state.process_start_time_ns = p12_process_start_time();
    state.pid = bpf_get_current_pid_tgid() >> 32;
    state.bytes = ctx->bytes;
    state.direction = ctx->rwbs[0] == 'W' ? 2 : 1;
    bpf_map_update_elem(&block_requests, &key, &state, BPF_ANY);
    event = p12_reserve();
    if (event) {
        p12_fill_common(event, P12_BLOCK_ISSUE, P12_CLASS_NODE, P12_QUALITY_EXACT);
        event->value = ctx->bytes;
        event->direction = state.direction;
        p12_submit(event);
    }
    return 0;
}

SEC("tracepoint/block/block_rq_complete")
int block_complete(struct trace_event_raw_block_rq_complete *ctx)
{
    struct block_key key = { .dev = ctx->dev, .sector = ctx->sector };
    struct block_state *state = bpf_map_lookup_elem(&block_requests, &key);
    if (state) {
        struct p12_event *complete = p12_reserve();
        if (complete) {
            p12_fill_common(complete, P12_BLOCK_COMPLETE, P12_CLASS_NODE, P12_QUALITY_DERIVED);
            complete->cgroup_id = state->cgroup_id;
            complete->process_start_time_ns = state->process_start_time_ns;
            complete->pid = state->pid;
            complete->tid = state->pid;
            complete->value = ctx->error ? (__u64)(-ctx->error) : state->bytes;
            complete->direction = state->direction;
            p12_submit(complete);
        }
        struct p12_event *event = p12_reserve();
        if (event) {
            p12_fill_common(event, P12_BLOCK_LATENCY, P12_CLASS_NODE, P12_QUALITY_DERIVED);
            event->cgroup_id = state->cgroup_id;
            event->process_start_time_ns = state->process_start_time_ns;
            event->pid = state->pid;
            event->tid = state->pid;
            event->value = ctx->error ? (__u64)(-ctx->error) : state->bytes;
            event->duration_ns = bpf_ktime_get_ns() - state->timestamp_ns;
            event->direction = state->direction;
            p12_submit(event);
        }
        bpf_map_delete_elem(&block_requests, &key);
    }
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
