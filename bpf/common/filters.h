#ifndef PROBERCA_P12_FILTERS_H
#define PROBERCA_P12_FILTERS_H

static __always_inline struct p12_config *p12_get_config(void)
{
    __u32 zero = 0;
    return bpf_map_lookup_elem(&p12_config_map, &zero);
}

static __always_inline void p12_count_filtered(void)
{
    __u32 zero = 0;
    struct p12_loss_counters *counters = bpf_map_lookup_elem(&loss_counters, &zero);
    if (counters)
        counters->filtered++;
}

static __always_inline int p12_cgroup_allowed(__u64 cgroup_id)
{
    struct p12_config *cfg = p12_get_config();
    __u64 *generation;
    if (!cfg || !cfg->enabled)
        return 0;
    generation = bpf_map_lookup_elem(&candidate_cgroups, &cgroup_id);
    if (!generation || *generation != cfg->candidate_version) {
        p12_count_filtered();
        return 0;
    }
    return 1;
}

static __always_inline int p12_edge_allowed(__u64 source, __u64 target)
{
    struct p12_config *cfg = p12_get_config();
    struct p12_edge_key key = {
        .source_cgroup_id = source,
        .target_cgroup_id = target,
    };
    __u64 *generation;
    if (!cfg || !cfg->enabled)
        return 0;
    if (!cfg->edge_pair_count)
        return p12_cgroup_allowed(source);
    generation = bpf_map_lookup_elem(&candidate_edges, &key);
    if (!generation || *generation != cfg->candidate_version) {
        p12_count_filtered();
        return 0;
    }
    return 1;
}

#endif
