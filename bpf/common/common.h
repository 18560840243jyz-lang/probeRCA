#ifndef PROBERCA_P12_COMMON_H
#define PROBERCA_P12_COMMON_H

#include "event.h"
#include "maps.h"
#include "filters.h"

static __always_inline __u64 p12_next_sequence(void)
{
    __u32 zero = 0;
    __u64 *sequence = bpf_map_lookup_elem(&event_sequences, &zero);
    if (!sequence)
        return 0;
    *sequence += 1;
    return *sequence;
}

static __always_inline __u64 p12_process_start_time(void)
{
    struct task_struct *task = (struct task_struct *)bpf_get_current_task_btf();
    return BPF_CORE_READ(task, start_boottime);
}

static __always_inline void p12_fill_common(
    struct p12_event *event, __u16 type, __u16 event_class, __u16 quality)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct p12_config *cfg = p12_get_config();
    event->schema_version = P12_EVENT_SCHEMA_VERSION;
    event->event_type = type;
    event->event_class = event_class;
    event->quality = quality;
    event->size = sizeof(*event);
    event->timestamp_ns = bpf_ktime_get_ns();
    event->process_start_time_ns = p12_process_start_time();
    event->cgroup_id = bpf_get_current_cgroup_id();
    event->attach_epoch = cfg ? cfg->attach_epoch : 0;
    event->event_sequence = p12_next_sequence();
    event->cpu = bpf_get_smp_processor_id();
    event->pid = pid_tgid >> 32;
    event->tid = (__u32)pid_tgid;
    bpf_get_current_comm(event->comm, sizeof(event->comm));
}

static __always_inline struct p12_event *p12_reserve(void)
{
    struct p12_event *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
    if (!event) {
        __u32 zero = 0;
        struct p12_loss_counters *counters = bpf_map_lookup_elem(&loss_counters, &zero);
        if (counters)
            counters->reserve_failed++;
        return 0;
    }
    __builtin_memset(event, 0, sizeof(*event));
    return event;
}

static __always_inline void p12_submit(struct p12_event *event)
{
    bpf_ringbuf_submit(event, 0);
}

#endif
