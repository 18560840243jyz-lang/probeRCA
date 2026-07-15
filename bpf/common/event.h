#ifndef PROBERCA_P12_EVENT_H
#define PROBERCA_P12_EVENT_H

#define P12_EVENT_SCHEMA_VERSION 1
#define P12_EVENT_ABI_SIZE 136
#define P12_TASK_COMM_LEN 16

enum p12_event_class {
    P12_CLASS_NODE = 1,
    P12_CLASS_EDGE = 2,
    P12_CLASS_UNMAPPED = 3,
    P12_CLASS_CONTROL = 4,
    P12_CLASS_LOSS = 5,
};

enum p12_event_quality {
    P12_QUALITY_EXACT = 1,
    P12_QUALITY_DERIVED = 2,
    P12_QUALITY_PARTIAL = 3,
    P12_QUALITY_UNMAPPED = 4,
};

enum p12_event_type {
    P12_PROCESS_FORK = 1,
    P12_PROCESS_EXEC = 2,
    P12_PROCESS_EXIT = 3,
    P12_PROCESS_CGROUP_MIGRATE = 4,
    P12_SCHED_OFFCPU = 10,
    P12_SCHED_RUNQUEUE = 11,
    P12_FUTEX_WAIT = 20,
    P12_FUTEX_WAKE = 21,
    P12_BLOCK_ISSUE = 30,
    P12_BLOCK_COMPLETE = 31,
    P12_BLOCK_LATENCY = 32,
    P12_TCP_RETRANSMIT = 40,
    P12_TCP_RESET = 41,
    P12_TCP_CONNECT_FAIL = 42,
    P12_TCP_RTT = 43,
    P12_DNS_QUERY = 50,
    P12_DNS_RESPONSE = 51,
    P12_PROBE_LOSS = 60,
};

struct p12_event {
    __u16 schema_version;
    __u16 event_type;
    __u16 event_class;
    __u16 quality;
    __u32 size;
    __u32 _padding;
    __u64 timestamp_ns;
    __u64 process_start_time_ns;
    __u64 cgroup_id;
    __u64 src_cgroup_id;
    __u64 dst_cgroup_id;
    __u64 value;
    __u64 duration_ns;
    __u64 attach_epoch;
    __u64 event_sequence;
    __u32 cpu;
    __u32 pid;
    __u32 tid;
    __u32 src_ipv4;
    __u32 dst_ipv4;
    __u16 src_port;
    __u16 dst_port;
    __u16 protocol;
    __u16 direction;
    __u16 mapping_status;
    __u16 reserved;
    char comm[P12_TASK_COMM_LEN];
};

_Static_assert(sizeof(struct p12_event) == P12_EVENT_ABI_SIZE, "P12 event ABI size changed");
_Static_assert(__alignof__(struct p12_event) == 8, "P12 event ABI alignment changed");

#endif
