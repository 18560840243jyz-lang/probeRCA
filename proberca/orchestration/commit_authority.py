"""Explicit local atomic commit authority for P10 Replay."""

from __future__ import annotations

from .checkpoint import restore_engine_checkpoint, save_engine_checkpoint


class LocalAtomicCommitAuthority:
    """P10 local generation backend; never used by the live runner."""

    backend_name = "local_atomic_current"

    @staticmethod
    def save(*args, **kwargs):
        return save_engine_checkpoint(*args, **kwargs)

    @staticmethod
    def restore(*args, **kwargs):
        return restore_engine_checkpoint(*args, **kwargs)
