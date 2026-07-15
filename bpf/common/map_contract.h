#ifndef PROBERCA_P12_MAP_CONTRACT_H
#define PROBERCA_P12_MAP_CONTRACT_H

#define P12_MAX_CANDIDATES 4096
#define P12_RINGBUF_BYTES (16 * 1024 * 1024)

struct p12_config {
    __u64 attach_epoch;
    __u64 candidate_version;
    __u32 enabled;
    __u32 edge_pair_count;
};
struct p12_edge_key { __u64 source_cgroup_id; __u64 target_cgroup_id; };
struct p12_loss_counters { __u64 reserve_failed; __u64 filtered; };

#endif
