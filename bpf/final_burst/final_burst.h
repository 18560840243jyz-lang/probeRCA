#ifndef PROBERCA_FINAL_BURST_H
#define PROBERCA_FINAL_BURST_H

#define PROBERCA_BURST_SCHEMA_VERSION 1
#define PROBERCA_BURST_RING_BYTES (16U * 1024U * 1024U)
#define PROBERCA_BURST_MAX_TASKS 65536
#define PROBERCA_BURST_MAX_REQUESTS 131072
#define PROBERCA_BURST_DNS_PORT 53
#define PROBERCA_BURST_IPPROTO_TCP 6
#define PROBERCA_BURST_IPPROTO_UDP 17
#define PROBERCA_BURST_AF_INET 2

enum proberca_burst_event_type {
    PROBERCA_BURST_SCHED_RUNQUEUE = 1,
    PROBERCA_BURST_RECLAIM_STALL = 2,
    PROBERCA_BURST_OOM_VICTIM = 3,
    PROBERCA_BURST_BLOCK_IO = 4,
    PROBERCA_BURST_FUTEX_WAIT = 5,
    PROBERCA_BURST_SOCKET_WAIT = 6,
    PROBERCA_BURST_SOCKET_BACKLOG = 7,
    PROBERCA_BURST_SOCKET_FAILURE = 8,
    PROBERCA_BURST_SOFTIRQ = 9,
    PROBERCA_BURST_NIC_DROP = 10,
    PROBERCA_BURST_NIC_ERROR = 11,
    PROBERCA_BURST_TCP_CONNECTION = 12,
    PROBERCA_BURST_TCP_RETRANSMIT = 13,
    PROBERCA_BURST_TCP_RTO = 14,
    PROBERCA_BURST_TCP_RTT = 15,
    PROBERCA_BURST_TCP_CONNECT_FAILURE = 16,
    PROBERCA_BURST_TCP_RST = 17,
    PROBERCA_BURST_DNS_QUERY = 18,
    PROBERCA_BURST_DNS_RESPONSE = 19,
    PROBERCA_BURST_DNS_TIMEOUT = 20,
};

struct proberca_burst_event {
    __u64 monotonic_ns;
    __u64 cgroup_id;
    __u64 value;
    __u64 duration_ns;
    __u64 auxiliary_ns;
    __u64 sequence;
    __u32 event_type;
    __u32 cpu;
    __be32 src_ipv4;
    __be32 dst_ipv4;
    __u32 device;
    __u16 src_port;
    __u16 dst_port;
    __u16 protocol;
    __u16 direction;
    __u16 transaction_id;
    __u16 rcode;
    __u32 sampling_divisor;
};

struct proberca_burst_loss_counters {
    __u64 emitted;
    __u64 reserve_failed;
};

struct proberca_burst_sampling_config {
    __u32 sched;
    __u32 futex;
    __u32 socket_wait;
    __u32 softirq;
    __u32 tcp_rtt;
    __u32 tcp_connection;
    __u32 dns_query;
    __u32 dns_response;
};

struct proberca_burst_dns_key {
    __u64 cgroup_id;
    __be32 server_ipv4;
    __be16 client_port;
    __be16 transaction_id;
};

struct proberca_burst_dns_pending {
    __u64 started_ns;
    __be32 client_ipv4;
    __u32 reserved;
};

#endif
