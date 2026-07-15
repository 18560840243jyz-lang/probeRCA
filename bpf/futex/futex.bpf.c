#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include "common.h"

#define P12_FUTEX_CMD_MASK 127
#define P12_FUTEX_WAIT_OP 0
#define P12_FUTEX_WAKE_OP 1

struct futex_wait_state {
    __u64 timestamp_ns;
    __u64 cgroup_id;
    __u64 process_start_time_ns;
};

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 32768);
    __type(key, __u64);
    __type(value, struct futex_wait_state);
} futex_waits SEC(".maps");

SEC("tracepoint/syscalls/sys_enter_futex")
int futex_enter(struct trace_event_raw_sys_enter *ctx)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    __u32 op = (__u32)ctx->args[1] & P12_FUTEX_CMD_MASK;
    if (!p12_cgroup_allowed(cgroup_id))
        return 0;
    if (op == P12_FUTEX_WAIT_OP) {
        struct futex_wait_state state = {
            .timestamp_ns = bpf_ktime_get_ns(),
            .cgroup_id = cgroup_id,
            .process_start_time_ns = p12_process_start_time(),
        };
        bpf_map_update_elem(&futex_waits, &pid_tgid, &state, BPF_ANY);
    } else if (op == P12_FUTEX_WAKE_OP) {
        struct p12_event *event = p12_reserve();
        if (event) {
            p12_fill_common(event, P12_FUTEX_WAKE, P12_CLASS_NODE, P12_QUALITY_EXACT);
            event->value = ctx->args[2];
            p12_submit(event);
        }
    }
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_futex")
int futex_exit(struct trace_event_raw_sys_exit *ctx)
{
    __u64 key = bpf_get_current_pid_tgid();
    struct futex_wait_state *state = bpf_map_lookup_elem(&futex_waits, &key);
    if (state) {
        struct p12_event *event = p12_reserve();
        if (event) {
            p12_fill_common(event, P12_FUTEX_WAIT, P12_CLASS_NODE, P12_QUALITY_EXACT);
            event->cgroup_id = state->cgroup_id;
            event->process_start_time_ns = state->process_start_time_ns;
            event->duration_ns = bpf_ktime_get_ns() - state->timestamp_ns;
            event->value = ctx->ret < 0 ? (__u64)(-ctx->ret) : 0;
            p12_submit(event);
        }
        bpf_map_delete_elem(&futex_waits, &key);
    }
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
