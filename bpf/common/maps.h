#ifndef PROBERCA_P12_MAPS_H
#define PROBERCA_P12_MAPS_H
#include "map_contract.h"
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, P12_RINGBUF_BYTES);
} events SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, P12_MAX_CANDIDATES * 2);
    __type(key, __u64);
    __type(value, __u64);
} candidate_cgroups SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, P12_MAX_CANDIDATES * 2);
    __type(key, struct p12_edge_key);
    __type(value, __u64);
} candidate_edges SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct p12_config);
} p12_config_map SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct p12_loss_counters);
} loss_counters SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} event_sequences SEC(".maps");
#endif
