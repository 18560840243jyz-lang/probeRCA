"""Bounded generation-swapped cgroup and directed edge candidate filters."""
from __future__ import annotations

import time
from dataclasses import dataclass

from .contracts import EventClass, KernelEvent


@dataclass(frozen=True)
class CandidateSnapshot:
    version: int
    cgroup_ids: tuple[int, ...]
    service_pairs: tuple[tuple[int, int], ...]
    ttl_sec: float = 30.0
    max_candidates: int = 1024
    kernel_edge_keys: tuple[tuple[int, int], ...] = ()

    def __post_init__(self) -> None:
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version < 1:
            raise ValueError("candidate version must be a positive integer")
        if self.ttl_sec <= 0 or self.max_candidates < 1:
            raise ValueError("candidate TTL and maximum must be positive")
        cgroups = tuple(sorted(set(self.cgroup_ids)))
        pairs = tuple(sorted(set(self.service_pairs)))
        kernel_edges = tuple(sorted(set(self.kernel_edge_keys)))
        if not cgroups and not pairs:
            raise ValueError("at least one candidate cgroup or edge is required")
        if len(cgroups) + len(pairs) + len(kernel_edges) > self.max_candidates:
            raise ValueError("candidate set exceeds configured maximum")
        if any(isinstance(item, bool) or not isinstance(item, int) or item < 0
               for item in cgroups):
            raise ValueError("cgroup candidates must be non-negative integers")
        if any(len(pair) != 2 or any(
                isinstance(item, bool) or not isinstance(item, int) or item < 0
                for item in pair) for pair in (*pairs, *kernel_edges)):
            raise ValueError("edge candidates must contain directed identity pairs")
        object.__setattr__(self, "cgroup_ids", cgroups)
        object.__setattr__(self, "service_pairs", pairs)
        object.__setattr__(self, "kernel_edge_keys", kernel_edges)

    @classmethod
    def from_candidate_subgraph(
        cls, subgraph, *, service_cgroups, edge_target_identities,
        version, ttl_sec=30.0, max_candidates=1024,
    ):
        """Derive bounded filters from P3 services without naming any workload."""
        candidate_services = set(subgraph.candidate_services)
        unknown = candidate_services - set(service_cgroups)
        if unknown:
            raise ValueError(f"candidate services lack runtime cgroups: {sorted(unknown)}")
        cgroups = {
            int(cgroup_id) for service_id in candidate_services
            for cgroup_id in service_cgroups[service_id]
        }
        service_pairs = set()
        kernel_edges = set()
        for edge in subgraph.physical_edges:
            source = edge["src_service_id"]
            target = edge["dst_service_id"]
            source_cgroups = tuple(int(item) for item in service_cgroups[source])
            target_cgroups = tuple(int(item) for item in service_cgroups[target])
            service_pairs.update((left, right) for left in source_cgroups for right in target_cgroups)
            endpoint_ids = edge_target_identities.get((source, target), ())
            kernel_edges.update((left, int(endpoint)) for left in source_cgroups for endpoint in endpoint_ids)
        return cls(
            version=version, cgroup_ids=tuple(cgroups),
            service_pairs=tuple(service_pairs), ttl_sec=ttl_sec,
            max_candidates=max_candidates, kernel_edge_keys=tuple(kernel_edges),
        )


class CandidateFilter:
    def __init__(self, snapshot: CandidateSnapshot, *, monotonic=time.monotonic):
        self._clock = monotonic
        self._snapshot = snapshot
        self._expires_at = self._clock() + snapshot.ttl_sec
        self.accepted = 0
        self.filtered = 0

    @property
    def version(self) -> int:
        return self._snapshot.version

    @property
    def expired(self) -> bool:
        return self._clock() >= self._expires_at

    def replace(self, snapshot: CandidateSnapshot) -> None:
        if snapshot.max_candidates != self._snapshot.max_candidates:
            raise ValueError("candidate maximum cannot change during an attach epoch")
        if snapshot.version <= self._snapshot.version:
            raise ValueError("candidate version must increase")
        self._snapshot = snapshot
        self._expires_at = self._clock() + snapshot.ttl_sec

    def accepts_service_pair(self, source_cgroup_id: int, target_cgroup_id: int) -> bool:
        if self.expired:
            self.filtered += 1
            return False
        accepted = (source_cgroup_id, target_cgroup_id) in self._snapshot.service_pairs
        self.accepted += int(accepted)
        self.filtered += int(not accepted)
        return accepted

    def accepts(self, event: KernelEvent) -> bool:
        if self.expired:
            self.filtered += 1
            return False
        if event.event_class is EventClass.EDGE:
            accepted = (event.src_cgroup_id, event.dst_cgroup_id) in self._snapshot.service_pairs
        else:
            accepted = event.cgroup_id in self._snapshot.cgroup_ids
        if accepted:
            self.accepted += 1
        else:
            self.filtered += 1
        return accepted


__all__ = ["CandidateFilter", "CandidateSnapshot"]
