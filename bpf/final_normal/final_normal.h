#ifndef PROBERCA_FINAL_NORMAL_H
#define PROBERCA_FINAL_NORMAL_H

#define PROBERCA_FINAL_DNS_BUCKETS 16
#define PROBERCA_FINAL_MAX_CGROUPS 65536
#define PROBERCA_FINAL_MAX_DNS_EDGES 65536
#define PROBERCA_FINAL_MAX_DNS_PENDING 131072

struct proberca_final_cgroup_counters {
    __u64 futex_wait_ns_total;
    __u64 socket_backlog_overflow_total;
    __u64 socket_accept_fail_total;
    __u64 socket_local_rst_total;
    __u64 socket_local_drop_total;
    __u64 socket_ops_total;
};

struct proberca_final_futex_start {
    __u64 started_ns;
    __u64 cgroup_id;
};

struct proberca_final_dns_edge_key {
    __u64 cgroup_id;
    __be32 server_ipv4;
    __u32 reserved;
};

struct proberca_final_dns_pending_key {
    __u64 cgroup_id;
    __be32 server_ipv4;
    __be16 client_port;
    __be16 transaction_id;
};

struct proberca_final_dns_edge_counters {
    __u64 query_total;
    __u64 error_rcode_total;
    __u64 latency_buckets[PROBERCA_FINAL_DNS_BUCKETS];
};

#endif
