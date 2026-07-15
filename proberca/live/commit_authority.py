"""Kubernetes Lease commit authority for P11 live execution."""

from __future__ import annotations

from .run_state import KubernetesLeaseRunStateStore


class KubernetesLeaseCommitAuthority(KubernetesLeaseRunStateStore):
    """P11 Lease CAS backend; the only authority accepted by LiveRunner."""

    backend_name = "kubernetes_lease_run_state"
