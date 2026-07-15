#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>
#include "common.h"

#define P12_IPPROTO_TCP 6
#define P12_IPPROTO_UDP 17
#define P12_DNS_PORT 53

struct p12_iphdr {
    __u8 version_ihl;
    __u8 tos;
    __be16 total_len;
    __be16 id;
    __be16 frag_off;
    __u8 ttl;
    __u8 protocol;
    __sum16 check;
    __be32 saddr;
    __be32 daddr;
};
struct p12_ports { __be16 source; __be16 dest; };

static __always_inline void fill_dns_common(struct p12_event *event, __u16 type, __u64 cgroup_id)
{
    struct p12_config *cfg = p12_get_config();
    event->schema_version = P12_EVENT_SCHEMA_VERSION;
    event->event_type = type;
    event->event_class = P12_CLASS_EDGE;
    event->quality = P12_QUALITY_PARTIAL;
    event->size = sizeof(*event);
    event->timestamp_ns = bpf_ktime_get_ns();
    event->cgroup_id = cgroup_id;
    event->attach_epoch = cfg ? cfg->attach_epoch : 0;
    event->event_sequence = p12_next_sequence();
    event->cpu = bpf_get_smp_processor_id();
}

static __always_inline int inspect_dns(struct __sk_buff *skb, __u16 direction)
{
    struct p12_iphdr ip = {};
    struct p12_ports ports = {};
    __u32 ihl;
    __u64 source;
    __u16 sport, dport, event_type;
    struct p12_event *event;
    if (bpf_skb_load_bytes(skb, 0, &ip, sizeof(ip)) < 0 || (ip.version_ihl >> 4) != 4)
        return 1;
    ihl = (ip.version_ihl & 0x0f) * 4;
    if (ihl < sizeof(ip) || bpf_skb_load_bytes(skb, ihl, &ports, sizeof(ports)) < 0)
        return 1;
    if (ip.protocol != P12_IPPROTO_UDP && ip.protocol != P12_IPPROTO_TCP)
        return 1;
    sport = bpf_ntohs(ports.source);
    dport = bpf_ntohs(ports.dest);
    if (sport != P12_DNS_PORT && dport != P12_DNS_PORT)
        return 1;
    source = bpf_skb_cgroup_id(skb);
    if (!p12_edge_allowed(source, (__u64)ip.daddr))
        return 1;
    event = p12_reserve();
    if (!event)
        return 1;
    event_type = dport == P12_DNS_PORT ? P12_DNS_QUERY : P12_DNS_RESPONSE;
    fill_dns_common(event, event_type, source);
    event->cgroup_id = source;
    event->src_cgroup_id = source;
    event->src_ipv4 = ip.saddr;
    event->dst_ipv4 = ip.daddr;
    event->src_port = sport;
    event->dst_port = dport;
    event->protocol = ip.protocol;
    event->direction = direction;
    event->value = skb->len;
    event->mapping_status = 1;
    p12_submit(event);
    return 1;
}

SEC("cgroup_skb/egress")
int dns_egress(struct __sk_buff *skb) { return inspect_dns(skb, 1); }
SEC("cgroup_skb/ingress")
int dns_ingress(struct __sk_buff *skb) { return inspect_dns(skb, 2); }

char LICENSE[] SEC("license") = "GPL";
