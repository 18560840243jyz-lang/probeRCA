#include "vmlinux.h"

#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#include "final_burst.h"

#define FUTEX_CMD_MASK 127
#define FUTEX_WAIT 0
#define FUTEX_WAIT_BITSET 9
#define TCP_ESTABLISHED 1
#define TCP_SYN_SENT 2
#define TCP_SYN_RECV 3
#define TCP_CLOSE 7
#define EINTR_VALUE 4
#define EALREADY_VALUE 114
#define EINPROGRESS_VALUE 115
#define NET_TX_SOFTIRQ 2
#define NET_RX_SOFTIRQ 3

struct timed_cgroup {
    __u64 started_ns;
    __u64 cgroup_id;
};

struct wakeup_state {
    __u64 timestamp_ns;
};

struct block_key {
    __u32 device;
    __u32 padding;
    __u64 sector;
};

struct block_state {
    __u64 inserted_ns;
    __u64 issued_ns;
    __u64 cgroup_id;
    __u32 device;
    __u32 bytes;
};

struct socket_state {
    __u64 started_ns;
    __u64 cgroup_id;
    __u16 operation;
    __u16 reserved[3];
};

struct tuple_key {
    __be32 source;
    __be32 destination;
    __u16 source_port;
    __u16 destination_port;
};

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

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, PROBERCA_BURST_RING_BYTES);
} burst_events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct proberca_burst_loss_counters);
} burst_loss SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} burst_sequences SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct proberca_burst_sampling_config);
} burst_sampling SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u32);
    __type(value, struct timed_cgroup);
} offcpu_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u32);
    __type(value, struct wakeup_state);
} wakeup_times SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u32);
    __type(value, __u64);
} task_cgroups SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u64);
    __type(value, struct timed_cgroup);
} reclaim_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_REQUESTS);
    __type(key, struct block_key);
    __type(value, struct block_state);
} block_requests SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u64);
    __type(value, struct timed_cgroup);
} futex_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_TASKS);
    __type(key, __u64);
    __type(value, struct socket_state);
} socket_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} softirq_starts SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_REQUESTS);
    __type(key, struct tuple_key);
    __type(value, __u64);
} tcp_tuple_cgroups SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, PROBERCA_BURST_MAX_REQUESTS);
    __type(key, struct proberca_burst_dns_key);
    __type(value, struct proberca_burst_dns_pending);
} burst_dns_pending SEC(".maps");

static __always_inline __u64 next_sequence(void)
{
    __u32 zero = 0;
    __u64 *value = bpf_map_lookup_elem(&burst_sequences, &zero);

    if (!value)
        return 0;
    return __sync_fetch_and_add(value, 1) + 1;
}

static __always_inline __u32 sampling_divisor(__u32 channel)
{
    __u32 zero = 0;
    __u32 divisor = 1;
    struct proberca_burst_sampling_config *config =
        bpf_map_lookup_elem(&burst_sampling, &zero);

    if (!config)
        return divisor;
    if (channel == 1)
        divisor = config->sched;
    else if (channel == 2)
        divisor = config->futex;
    else if (channel == 3)
        divisor = config->socket_wait;
    else if (channel == 4)
        divisor = config->softirq;
    else if (channel == 5)
        divisor = config->tcp_rtt;
    else if (channel == 6)
        divisor = config->tcp_connection;
    else if (channel == 7)
        divisor = config->dns_query;
    else if (channel == 8)
        divisor = config->dns_response;
    return divisor ? divisor : 1;
}

static __always_inline int sampled(__u32 divisor)
{
    return divisor <= 1 || bpf_get_prandom_u32() % divisor == 0;
}

static __always_inline struct proberca_burst_event *reserve_event(
    __u32 type, __u64 cgroup_id)
{
    __u32 zero = 0;
    struct proberca_burst_loss_counters *loss;
    struct proberca_burst_event *event;

    event = bpf_ringbuf_reserve(&burst_events, sizeof(*event), 0);
    loss = bpf_map_lookup_elem(&burst_loss, &zero);
    if (!event) {
        if (loss)
            __sync_fetch_and_add(&loss->reserve_failed, 1);
        return 0;
    }
    __builtin_memset(event, 0, sizeof(*event));
    event->monotonic_ns = bpf_ktime_get_ns();
    event->cgroup_id = cgroup_id;
    event->event_type = type;
    event->sequence = next_sequence();
    event->cpu = bpf_get_smp_processor_id();
    event->sampling_divisor = 1;
    if (loss)
        __sync_fetch_and_add(&loss->emitted, 1);
    return event;
}

static __always_inline void submit_event(
    struct proberca_burst_event *event)
{
    if (event)
        bpf_ringbuf_submit(event, 0);
}

static __always_inline __u64 socket_cgroup_id(const void *address)
{
    const struct sock *socket = address;
    struct cgroup *cgroup;
    struct kernfs_node *node;

    if (!socket)
        return 0;
    cgroup = BPF_CORE_READ(socket, sk_cgrp_data.cgroup);
    if (!cgroup)
        return 0;
    node = BPF_CORE_READ(cgroup, kn);
    return node ? BPF_CORE_READ(node, id) : 0;
}

static __always_inline void tuple_from_socket(
    const struct sock *socket, struct proberca_burst_event *event)
{
    event->src_ipv4 = BPF_CORE_READ(
        socket, __sk_common.skc_rcv_saddr);
    event->dst_ipv4 = BPF_CORE_READ(
        socket, __sk_common.skc_daddr);
    event->src_port = BPF_CORE_READ(
        socket, __sk_common.skc_num);
    event->dst_port = bpf_ntohs(BPF_CORE_READ(
        socket, __sk_common.skc_dport));
    event->protocol = PROBERCA_BURST_IPPROTO_TCP;
    event->direction = 1;
}

static __always_inline int remember_wakeup(__u32 pid)
{
    struct wakeup_state state = {
        .timestamp_ns = bpf_ktime_get_ns(),
    };

    return bpf_map_update_elem(&wakeup_times, &pid, &state, BPF_ANY);
}

SEC("tracepoint/sched/sched_wakeup")
int final_burst_sched_wakeup(
    struct trace_event_raw_sched_wakeup_template *context)
{
    remember_wakeup(context->pid);
    return 0;
}

SEC("tracepoint/sched/sched_wakeup_new")
int final_burst_sched_wakeup_new(
    struct trace_event_raw_sched_wakeup_template *context)
{
    remember_wakeup(context->pid);
    return 0;
}

SEC("tracepoint/sched/sched_switch")
int final_burst_sched_switch(
    struct trace_event_raw_sched_switch *context)
{
    __u64 now = bpf_ktime_get_ns();
    __u64 current_cgroup = bpf_get_current_cgroup_id();
    __u32 previous = context->prev_pid;
    __u32 next = context->next_pid;
    struct timed_cgroup *offcpu;
    struct wakeup_state *wakeup;

    if (previous > 0) {
        struct timed_cgroup state = {
            .started_ns = now,
            .cgroup_id = current_cgroup,
        };
        bpf_map_update_elem(&offcpu_starts, &previous, &state, BPF_ANY);
        bpf_map_update_elem(
            &task_cgroups, &previous, &current_cgroup, BPF_ANY);
    }
    offcpu = bpf_map_lookup_elem(&offcpu_starts, &next);
    wakeup = bpf_map_lookup_elem(&wakeup_times, &next);
    if (offcpu) {
        bpf_map_update_elem(
            &task_cgroups, &next, &offcpu->cgroup_id, BPF_ANY);
        if (wakeup && now >= wakeup->timestamp_ns) {
            __u32 divisor = sampling_divisor(1);

            if (sampled(divisor)) {
                struct proberca_burst_event *event = reserve_event(
                    PROBERCA_BURST_SCHED_RUNQUEUE, offcpu->cgroup_id);
                if (event) {
                    event->duration_ns = now - wakeup->timestamp_ns;
                    event->sampling_divisor = divisor;
                }
                submit_event(event);
            }
        }
        bpf_map_delete_elem(&offcpu_starts, &next);
    }
    if (wakeup)
        bpf_map_delete_elem(&wakeup_times, &next);
    return 0;
}

SEC("tracepoint/vmscan/mm_vmscan_direct_reclaim_begin")
int final_burst_reclaim_begin(
    struct trace_event_raw_mm_vmscan_direct_reclaim_begin_template *context)
{
    __u64 key = bpf_get_current_pid_tgid();
    struct timed_cgroup state = {
        .started_ns = bpf_ktime_get_ns(),
        .cgroup_id = bpf_get_current_cgroup_id(),
    };

    (void)context;
    bpf_map_update_elem(&reclaim_starts, &key, &state, BPF_ANY);
    return 0;
}

SEC("tracepoint/vmscan/mm_vmscan_direct_reclaim_end")
int final_burst_reclaim_end(
    struct trace_event_raw_mm_vmscan_direct_reclaim_end_template *context)
{
    __u64 key = bpf_get_current_pid_tgid();
    __u64 now = bpf_ktime_get_ns();
    struct timed_cgroup *state =
        bpf_map_lookup_elem(&reclaim_starts, &key);

    if (state) {
        struct proberca_burst_event *event = reserve_event(
            PROBERCA_BURST_RECLAIM_STALL, state->cgroup_id);
        if (event) {
            event->duration_ns = now - state->started_ns;
            event->value = context->nr_reclaimed;
        }
        submit_event(event);
        bpf_map_delete_elem(&reclaim_starts, &key);
    }
    return 0;
}

SEC("tracepoint/oom/mark_victim")
int final_burst_oom_victim(struct trace_event_raw_mark_victim *context)
{
    __u32 pid = context->pid;
    __u64 *known = bpf_map_lookup_elem(&task_cgroups, &pid);
    __u64 cgroup_id = known ? *known : bpf_get_current_cgroup_id();
    struct proberca_burst_event *event = reserve_event(
        PROBERCA_BURST_OOM_VICTIM, cgroup_id);

    if (event)
        event->value = 1;
    submit_event(event);
    return 0;
}

SEC("tracepoint/block/block_rq_insert")
int final_burst_block_insert(struct trace_event_raw_block_rq *context)
{
    struct block_key key = {
        .device = context->dev,
        .sector = context->sector,
    };
    struct block_state state = {
        .inserted_ns = bpf_ktime_get_ns(),
        .cgroup_id = bpf_get_current_cgroup_id(),
        .device = context->dev,
        .bytes = context->bytes,
    };

    bpf_map_update_elem(&block_requests, &key, &state, BPF_ANY);
    return 0;
}

SEC("tracepoint/block/block_rq_issue")
int final_burst_block_issue(struct trace_event_raw_block_rq *context)
{
    struct block_key key = {
        .device = context->dev,
        .sector = context->sector,
    };
    struct block_state *state = bpf_map_lookup_elem(&block_requests, &key);
    __u64 now = bpf_ktime_get_ns();

    if (state) {
        state->issued_ns = now;
    } else {
        struct block_state fallback = {
            .inserted_ns = now,
            .issued_ns = now,
            .cgroup_id = bpf_get_current_cgroup_id(),
            .device = context->dev,
            .bytes = context->bytes,
        };
        bpf_map_update_elem(&block_requests, &key, &fallback, BPF_ANY);
    }
    return 0;
}

SEC("tracepoint/block/block_rq_complete")
int final_burst_block_complete(
    struct trace_event_raw_block_rq_complete *context)
{
    struct block_key key = {
        .device = context->dev,
        .sector = context->sector,
    };
    struct block_state *state = bpf_map_lookup_elem(&block_requests, &key);
    __u64 now = bpf_ktime_get_ns();

    if (state) {
        struct proberca_burst_event *event = reserve_event(
            PROBERCA_BURST_BLOCK_IO, state->cgroup_id);
        if (event) {
            event->duration_ns = now - state->inserted_ns;
            event->auxiliary_ns = state->issued_ns >= state->inserted_ns
                ? state->issued_ns - state->inserted_ns : 0;
            event->value = state->bytes;
            event->device = state->device;
        }
        submit_event(event);
        bpf_map_delete_elem(&block_requests, &key);
    }
    return 0;
}

SEC("tracepoint/syscalls/sys_enter_futex")
int final_burst_futex_enter(struct trace_event_raw_sys_enter *context)
{
    __u32 operation = (__u32)context->args[1] & FUTEX_CMD_MASK;
    __u64 key = bpf_get_current_pid_tgid();
    struct timed_cgroup state = {
        .started_ns = bpf_ktime_get_ns(),
        .cgroup_id = bpf_get_current_cgroup_id(),
    };

    if (operation == FUTEX_WAIT || operation == FUTEX_WAIT_BITSET)
        bpf_map_update_elem(&futex_starts, &key, &state, BPF_ANY);
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_futex")
int final_burst_futex_exit(struct trace_event_raw_sys_exit *context)
{
    __u64 key = bpf_get_current_pid_tgid();
    struct timed_cgroup *state =
        bpf_map_lookup_elem(&futex_starts, &key);

    if (state) {
        __u32 divisor = sampling_divisor(2);

        if (sampled(divisor)) {
            struct proberca_burst_event *event = reserve_event(
                PROBERCA_BURST_FUTEX_WAIT, state->cgroup_id);
            if (event) {
                event->duration_ns =
                    bpf_ktime_get_ns() - state->started_ns;
                event->value = context->ret < 0 ? -context->ret : 0;
                event->sampling_divisor = divisor;
            }
            submit_event(event);
        }
        bpf_map_delete_elem(&futex_starts, &key);
    }
    return 0;
}

static __always_inline int socket_enter(__u16 operation)
{
    __u64 key = bpf_get_current_pid_tgid();
    struct socket_state state = {
        .started_ns = bpf_ktime_get_ns(),
        .cgroup_id = bpf_get_current_cgroup_id(),
        .operation = operation,
    };

    bpf_map_update_elem(&socket_starts, &key, &state, BPF_ANY);
    return 0;
}

SEC("tracepoint/syscalls/sys_enter_accept")
int final_burst_accept_enter(void *context)
{
    (void)context;
    return socket_enter(1);
}

SEC("tracepoint/syscalls/sys_enter_accept4")
int final_burst_accept4_enter(void *context)
{
    (void)context;
    return socket_enter(1);
}

SEC("tracepoint/syscalls/sys_enter_connect")
int final_burst_connect_enter(void *context)
{
    (void)context;
    return socket_enter(2);
}

static __always_inline int socket_exit(long result)
{
    __u64 key = bpf_get_current_pid_tgid();
    struct socket_state *state =
        bpf_map_lookup_elem(&socket_starts, &key);

    if (state) {
        __u32 divisor = sampling_divisor(3);

        if (sampled(divisor)) {
            struct proberca_burst_event *wait = reserve_event(
                PROBERCA_BURST_SOCKET_WAIT, state->cgroup_id);
            if (wait) {
                wait->duration_ns =
                    bpf_ktime_get_ns() - state->started_ns;
                wait->direction = state->operation;
                wait->sampling_divisor = divisor;
            }
            submit_event(wait);
        }
        if (result < 0 && -result != EINTR_VALUE &&
            -result != EINPROGRESS_VALUE && -result != EALREADY_VALUE) {
            struct proberca_burst_event *failure = reserve_event(
                PROBERCA_BURST_SOCKET_FAILURE, state->cgroup_id);
            if (failure) {
                failure->value = -result;
                failure->direction = state->operation;
            }
            submit_event(failure);
        }
        bpf_map_delete_elem(&socket_starts, &key);
    }
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_accept")
int final_burst_accept_exit(struct trace_event_raw_sys_exit *context)
{
    return socket_exit(context->ret);
}

SEC("tracepoint/syscalls/sys_exit_accept4")
int final_burst_accept4_exit(struct trace_event_raw_sys_exit *context)
{
    return socket_exit(context->ret);
}

SEC("tracepoint/syscalls/sys_exit_connect")
int final_burst_connect_exit(struct trace_event_raw_sys_exit *context)
{
    return socket_exit(context->ret);
}

static __always_inline int socket_backlog(void)
{
    struct proberca_burst_event *event = reserve_event(
        PROBERCA_BURST_SOCKET_BACKLOG, bpf_get_current_cgroup_id());

    if (event)
        event->value = 1;
    submit_event(event);
    return 0;
}

SEC("tracepoint/sock/sock_exceed_buf_limit")
int final_burst_sock_exceed(void *context)
{
    (void)context;
    return socket_backlog();
}

SEC("tracepoint/sock/sock_rcvqueue_full")
int final_burst_sock_full(void *context)
{
    (void)context;
    return socket_backlog();
}

SEC("tracepoint/irq/softirq_entry")
int final_burst_softirq_entry(struct trace_event_raw_softirq *context)
{
    __u32 vector = context->vec;
    __u64 now = bpf_ktime_get_ns();

    if (vector == NET_RX_SOFTIRQ || vector == NET_TX_SOFTIRQ)
        bpf_map_update_elem(&softirq_starts, &vector, &now, BPF_ANY);
    return 0;
}

SEC("tracepoint/irq/softirq_exit")
int final_burst_softirq_exit(struct trace_event_raw_softirq *context)
{
    __u32 vector = context->vec;
    __u64 *started = bpf_map_lookup_elem(&softirq_starts, &vector);

    if (started && *started > 0) {
        __u32 divisor = sampling_divisor(4);

        if (sampled(divisor)) {
            struct proberca_burst_event *event = reserve_event(
                PROBERCA_BURST_SOFTIRQ, 0);
            if (event) {
                event->duration_ns = bpf_ktime_get_ns() - *started;
                event->value = vector;
                event->sampling_divisor = divisor;
            }
            submit_event(event);
        }
        *started = 0;
    }
    return 0;
}

SEC("tracepoint/skb/kfree_skb")
int final_burst_nic_drop(struct trace_event_raw_kfree_skb *context)
{
    struct proberca_burst_event *event;

    if ((__u32)context->reason == 0)
        return 0;
    event = reserve_event(PROBERCA_BURST_NIC_DROP, 0);
    if (event)
        event->value = context->reason;
    submit_event(event);
    return 0;
}

SEC("tracepoint/net/net_dev_xmit")
int final_burst_nic_error(struct trace_event_raw_net_dev_xmit *context)
{
    struct proberca_burst_event *event;

    if (context->rc == 0)
        return 0;
    event = reserve_event(PROBERCA_BURST_NIC_ERROR, 0);
    if (event)
        event->value = context->rc < 0 ? -context->rc : context->rc;
    submit_event(event);
    return 0;
}

static __always_inline void fill_tcp_trace_tuple(
    struct proberca_burst_event *event, __u16 family,
    __u16 source_port, __u16 destination_port,
    const void *source, const void *destination)
{
    if (!event)
        return;
    (void)family;
    bpf_probe_read_kernel(
        &event->src_ipv4, sizeof(event->src_ipv4), source);
    bpf_probe_read_kernel(
        &event->dst_ipv4, sizeof(event->dst_ipv4), destination);
    event->src_port = source_port;
    event->dst_port = destination_port;
    event->protocol = PROBERCA_BURST_IPPROTO_TCP;
    event->direction = 1;
}

SEC("tracepoint/tcp/tcp_retransmit_skb")
int final_burst_tcp_retransmit(
    struct trace_event_raw_tcp_event_sk_skb *context)
{
    __u64 cgroup_id;
    struct proberca_burst_event *event;

    if (context->family != PROBERCA_BURST_AF_INET)
        return 0;
    cgroup_id = socket_cgroup_id(context->skaddr);
    event = reserve_event(PROBERCA_BURST_TCP_RETRANSMIT, cgroup_id);
    fill_tcp_trace_tuple(
        event, context->family, context->sport, context->dport,
        context->saddr, context->daddr);
    if (event)
        event->value = 1;
    submit_event(event);
    return 0;
}

static __always_inline int emit_tcp_reset(
    struct trace_event_raw_tcp_event_sk *context)
{
    struct proberca_burst_event *event;

    if (context->family != PROBERCA_BURST_AF_INET)
        return 0;
    event = reserve_event(
        PROBERCA_BURST_TCP_RST, socket_cgroup_id(context->skaddr));
    fill_tcp_trace_tuple(
        event, context->family, context->sport, context->dport,
        context->saddr, context->daddr);
    if (event)
        event->value = 1;
    submit_event(event);
    return 0;
}

SEC("tracepoint/tcp/tcp_receive_reset")
int final_burst_tcp_receive_reset(
    struct trace_event_raw_tcp_event_sk *context)
{
    return emit_tcp_reset(context);
}

SEC("tracepoint/tcp/tcp_send_reset")
int final_burst_tcp_send_reset(
    struct trace_event_raw_tcp_event_sk *context)
{
    return emit_tcp_reset(context);
}

SEC("tracepoint/sock/inet_sock_set_state")
int final_burst_tcp_state(
    struct trace_event_raw_inet_sock_set_state *context)
{
    __u64 cgroup_id;
    __u32 divisor;
    struct proberca_burst_event *event;
    struct tuple_key key = {};

    if (context->family != PROBERCA_BURST_AF_INET ||
        context->protocol != PROBERCA_BURST_IPPROTO_TCP)
        return 0;
    cgroup_id = socket_cgroup_id(context->skaddr);
    bpf_probe_read_kernel(&key.source, sizeof(key.source), context->saddr);
    bpf_probe_read_kernel(
        &key.destination, sizeof(key.destination), context->daddr);
    key.source_port = context->sport;
    key.destination_port = context->dport;
    if (cgroup_id)
        bpf_map_update_elem(
            &tcp_tuple_cgroups, &key, &cgroup_id, BPF_ANY);
    if (context->newstate == TCP_SYN_SENT) {
        divisor = sampling_divisor(6);
        if (sampled(divisor)) {
            event = reserve_event(
                PROBERCA_BURST_TCP_CONNECTION, cgroup_id);
            fill_tcp_trace_tuple(
                event, context->family, context->sport, context->dport,
                context->saddr, context->daddr);
            if (event) {
                event->value = 1;
                event->sampling_divisor = divisor;
            }
            submit_event(event);
        }
    }
    if (context->newstate == TCP_CLOSE &&
        (context->oldstate == TCP_SYN_SENT ||
         context->oldstate == TCP_SYN_RECV)) {
        event = reserve_event(
            PROBERCA_BURST_TCP_CONNECT_FAILURE, cgroup_id);
        fill_tcp_trace_tuple(
            event, context->family, context->sport, context->dport,
            context->saddr, context->daddr);
        if (event)
            event->value = 1;
        submit_event(event);
    }
    return 0;
}

SEC("tracepoint/tcp/tcp_probe")
int final_burst_tcp_probe(struct trace_event_raw_tcp_probe *context)
{
    struct tuple_key key = {};
    __u64 *cgroup_id;
    struct proberca_burst_event *event;

    if (context->family != PROBERCA_BURST_AF_INET)
        return 0;
    bpf_probe_read_kernel(&key.source, sizeof(key.source), context->saddr);
    bpf_probe_read_kernel(
        &key.destination, sizeof(key.destination), context->daddr);
    key.source_port = context->sport;
    key.destination_port = context->dport;
    cgroup_id = bpf_map_lookup_elem(&tcp_tuple_cgroups, &key);
    __u32 divisor = sampling_divisor(5);

    if (!sampled(divisor))
        return 0;
    event = reserve_event(
        PROBERCA_BURST_TCP_RTT, cgroup_id ? *cgroup_id : 0);
    if (event) {
        event->src_ipv4 = key.source;
        event->dst_ipv4 = key.destination;
        event->src_port = key.source_port;
        event->dst_port = key.destination_port;
        event->protocol = PROBERCA_BURST_IPPROTO_TCP;
        event->direction = 1;
        event->duration_ns = (__u64)context->srtt * 1000ULL;
        event->sampling_divisor = divisor;
    }
    submit_event(event);
    return 0;
}

SEC("kprobe/tcp_retransmit_timer")
int BPF_KPROBE(final_burst_tcp_rto, struct sock *socket)
{
    struct proberca_burst_event *event = reserve_event(
        PROBERCA_BURST_TCP_RTO, socket_cgroup_id(socket));

    if (event) {
        tuple_from_socket(socket, event);
        event->value = 1;
    }
    submit_event(event);
    return 0;
}

static __always_inline int inspect_dns(
    struct __sk_buff *socket_buffer, bool egress)
{
    struct ipv4_header ip = {};
    struct udp_header udp = {};
    struct dns_header dns = {};
    struct proberca_burst_dns_key key = {};
    struct proberca_burst_dns_pending pending = {};
    struct proberca_burst_dns_pending *started;
    struct proberca_burst_event *event;
    __u32 header_length;
    __u32 divisor;
    __u16 source_port;
    __u16 destination_port;
    __u16 flags;
    __u64 now;

    if (bpf_skb_load_bytes(socket_buffer, 0, &ip, sizeof(ip)) < 0 ||
        (ip.version_ihl >> 4) != 4 ||
        ip.protocol != PROBERCA_BURST_IPPROTO_UDP)
        return 1;
    header_length = (ip.version_ihl & 0x0f) * 4;
    if (header_length < sizeof(ip) ||
        bpf_skb_load_bytes(
            socket_buffer, header_length, &udp, sizeof(udp)) < 0 ||
        bpf_skb_load_bytes(
            socket_buffer, header_length + sizeof(udp),
            &dns, sizeof(dns)) < 0)
        return 1;
    source_port = bpf_ntohs(udp.source);
    destination_port = bpf_ntohs(udp.destination);
    if (source_port != PROBERCA_BURST_DNS_PORT &&
        destination_port != PROBERCA_BURST_DNS_PORT)
        return 1;
    flags = bpf_ntohs(dns.flags);
    now = bpf_ktime_get_ns();
    key.cgroup_id = bpf_skb_cgroup_id(socket_buffer);
    if (!key.cgroup_id)
        return 1;
    if (egress && destination_port == PROBERCA_BURST_DNS_PORT &&
        !(flags & 0x8000)) {
        key.server_ipv4 = ip.destination;
        key.client_port = udp.source;
        key.transaction_id = dns.transaction_id;
        pending.started_ns = now;
        pending.client_ipv4 = ip.source;
        bpf_map_update_elem(
            &burst_dns_pending, &key, &pending, BPF_ANY);
        divisor = sampling_divisor(7);
        if (sampled(divisor)) {
            event = reserve_event(
                PROBERCA_BURST_DNS_QUERY, key.cgroup_id);
            if (event) {
                event->src_ipv4 = ip.source;
                event->dst_ipv4 = ip.destination;
                event->src_port = source_port;
                event->dst_port = destination_port;
                event->protocol = PROBERCA_BURST_IPPROTO_UDP;
                event->direction = 1;
                event->transaction_id = bpf_ntohs(
                    dns.transaction_id);
                event->value = 1;
                event->sampling_divisor = divisor;
            }
            submit_event(event);
        }
        return 1;
    }
    if (!egress && source_port == PROBERCA_BURST_DNS_PORT &&
        (flags & 0x8000)) {
        key.server_ipv4 = ip.source;
        key.client_port = udp.destination;
        key.transaction_id = dns.transaction_id;
        started = bpf_map_lookup_elem(&burst_dns_pending, &key);
        if (!started)
            return 1;
        divisor = (flags & 0x000f) ? 1 : sampling_divisor(8);
        if (sampled(divisor)) {
            event = reserve_event(
                PROBERCA_BURST_DNS_RESPONSE, key.cgroup_id);
            if (event) {
                event->src_ipv4 = started->client_ipv4;
                event->dst_ipv4 = key.server_ipv4;
                event->src_port = destination_port;
                event->dst_port = source_port;
                event->protocol = PROBERCA_BURST_IPPROTO_UDP;
                event->direction = 2;
                event->transaction_id = bpf_ntohs(
                    dns.transaction_id);
                event->rcode = flags & 0x000f;
                event->duration_ns = now - started->started_ns;
                event->value = 1;
                event->sampling_divisor = divisor;
            }
            submit_event(event);
        }
        bpf_map_delete_elem(&burst_dns_pending, &key);
    }
    return 1;
}

SEC("cgroup_skb/egress")
int final_burst_dns_egress(struct __sk_buff *socket_buffer)
{
    return inspect_dns(socket_buffer, true);
}

SEC("cgroup_skb/ingress")
int final_burst_dns_ingress(struct __sk_buff *socket_buffer)
{
    return inspect_dns(socket_buffer, false);
}

char LICENSE[] SEC("license") = "GPL";
