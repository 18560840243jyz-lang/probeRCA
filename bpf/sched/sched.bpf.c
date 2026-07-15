#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include "common.h"

struct offcpu_state {
    __u64 timestamp_ns;
    __u64 cgroup_id;
    __u64 process_start_time_ns;
};

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, __u32);
    __type(value, struct offcpu_state);
} offcpu_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, __u32);
    __type(value, __u64);
} wakeup_times SEC(".maps");

static __always_inline int record_wakeup(__u32 pid)
{
    __u64 now = bpf_ktime_get_ns();
    bpf_map_update_elem(&wakeup_times, &pid, &now, BPF_ANY);
    return 0;
}

SEC("tracepoint/sched/sched_wakeup")
int sched_wakeup(struct trace_event_raw_sched_wakeup_template *ctx)
{
    return record_wakeup(ctx->pid);
}

SEC("tracepoint/sched/sched_wakeup_new")
int sched_wakeup_new(struct trace_event_raw_sched_wakeup_template *ctx)
{
    return record_wakeup(ctx->pid);
}

SEC("tracepoint/sched/sched_switch")
int sched_switch(struct trace_event_raw_sched_switch *ctx)
{
    __u64 now = bpf_ktime_get_ns();
    __u64 current_cgroup = bpf_get_current_cgroup_id();
    __u32 prev = ctx->prev_pid;
    __u32 next = ctx->next_pid;
    struct offcpu_state *start = bpf_map_lookup_elem(&offcpu_starts, &next);
    if (start && p12_cgroup_allowed(start->cgroup_id)) {
        struct p12_event *event = p12_reserve();
        if (event) {
            p12_fill_common(event, P12_SCHED_OFFCPU, P12_CLASS_NODE, P12_QUALITY_DERIVED);
            event->cgroup_id = start->cgroup_id;
            event->pid = next;
            event->tid = next;
            event->process_start_time_ns = start->process_start_time_ns;
            event->duration_ns = now - start->timestamp_ns;
            p12_submit(event);
        }
        {
            __u64 *wakeup = bpf_map_lookup_elem(&wakeup_times, &next);
            if (wakeup) {
                struct p12_event *runqueue = p12_reserve();
                if (runqueue) {
                    p12_fill_common(runqueue, P12_SCHED_RUNQUEUE, P12_CLASS_NODE, P12_QUALITY_DERIVED);
                    runqueue->cgroup_id = start->cgroup_id;
                    runqueue->pid = next;
                    runqueue->tid = next;
                    runqueue->process_start_time_ns = start->process_start_time_ns;
                    runqueue->duration_ns = now - *wakeup;
                    p12_submit(runqueue);
                }
                bpf_map_delete_elem(&wakeup_times, &next);
            }
        }
        bpf_map_delete_elem(&offcpu_starts, &next);
    }
    if (prev > 0 && p12_cgroup_allowed(current_cgroup)) {
        struct offcpu_state state = {
            .timestamp_ns = now,
            .cgroup_id = current_cgroup,
            .process_start_time_ns = p12_process_start_time(),
        };
        bpf_map_update_elem(&offcpu_starts, &prev, &state, BPF_ANY);
    }
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
