#ifndef PROBERCA_FINAL_NORMAL_H
#define PROBERCA_FINAL_NORMAL_H

#define PROBERCA_FINAL_DNS_BUCKETS 16
#define PROBERCA_FINAL_DNS_QNAME_MAX 96
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
    __be16 qtype;
    __u16 reserved;
    __u64 qname_hash;
    char qname[PROBERCA_FINAL_DNS_QNAME_MAX];
};

struct proberca_final_dns_pending_key {
    __u64 cgroup_id;
    __be32 client_ipv4;
    __be32 server_ipv4;
    __be16 client_port;
    __be16 transaction_id;
    __be16 qtype;
    __u16 reserved;
    __u64 qname_hash;
};

struct proberca_final_dns_pending_value {
    __u64 started_ns;
    __u64 retry_count;
    struct proberca_final_dns_edge_key edge;
};

struct proberca_final_dns_scratch {
    struct proberca_final_dns_pending_key pending_key;
    struct proberca_final_dns_pending_value pending;
};

struct proberca_final_dns_edge_counters {
    __u64 query_total;
    __u64 success_total;
    __u64 servfail_total;
    __u64 refused_total;
    __u64 nxdomain_total;
    __u64 transport_error_total;
    __u64 retry_total;
    __u64 truncated_total;
    __u64 latency_buckets[PROBERCA_FINAL_DNS_BUCKETS];
};

#endif
