"""Process diagnostics that do not mutate live state."""
from __future__ import annotations

import faulthandler
import signal
import sys


def install_thread_dump_handler(output=None) -> None:
    """Enable traceback output and reserve SIGUSR1 for all-thread dumps."""
    destination = output if output is not None else sys.stderr
    faulthandler.enable(file=destination, all_threads=True)
    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(
            signal.SIGUSR1, file=destination, all_threads=True, chain=False)


def dump_all_threads(output=None) -> bool:
    """Best-effort all-thread dump that never changes live state."""
    destination = output if output is not None else sys.stderr
    try:
        faulthandler.dump_traceback(file=destination, all_threads=True)
        if hasattr(destination, "flush"):
            destination.flush()
        return True
    except (OSError, RuntimeError, ValueError):
        return False
