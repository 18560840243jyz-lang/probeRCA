#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include "common.h"

static __always_inline int emit_process(__u16 type, __u32 subject_pid)
{
    __u64 cgroup_id = bpf_get_current_cgroup_id();
    struct p12_event *event;
    if (!p12_cgroup_allowed(cgroup_id))
        return 0;
    event = p12_reserve();
    if (!event)
        return 0;
    p12_fill_common(event, type, P12_CLASS_NODE, P12_QUALITY_EXACT);
    event->value = subject_pid;
    p12_submit(event);
    return 0;
}

SEC("tracepoint/sched/sched_process_fork")
int process_fork(struct trace_event_raw_sched_process_fork *ctx)
{
    return emit_process(P12_PROCESS_FORK, ctx->child_pid);
}

SEC("tracepoint/sched/sched_process_exec")
int process_exec(struct trace_event_raw_sched_process_exec *ctx)
{
    return emit_process(P12_PROCESS_EXEC, ctx->pid);
}

SEC("tracepoint/sched/sched_process_exit")
int process_exit(struct trace_event_raw_sched_process_template *ctx)
{
    return emit_process(P12_PROCESS_EXIT, ctx->pid);
}

SEC("tracepoint/cgroup/cgroup_attach_task")
int process_cgroup_migrate(struct trace_event_raw_cgroup_migrate *ctx)
{
    struct p12_event *event;
    if (!p12_cgroup_allowed(ctx->dst_id)) return 0;
    event = p12_reserve();
    if (!event) return 0;
    p12_fill_common(event, P12_PROCESS_CGROUP_MIGRATE, P12_CLASS_NODE, P12_QUALITY_EXACT);
    event->cgroup_id = ctx->dst_id;
    event->pid = ctx->pid;
    event->tid = ctx->pid;
    event->value = ctx->dst_id;
    p12_submit(event);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
