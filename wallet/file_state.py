"""Small helpers for cross-process JSON state persistence.

Provides a minimal lock + read/modify/write utility for wallet state files.
Uses ``fcntl.flock`` on POSIX and gracefully degrades to best-effort writes on
platforms without flock support.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")

try:
    import fcntl  # type: ignore
    _HAS_FCNTL = True
except ImportError:  # pragma: no cover - Windows fallback
    _HAS_FCNTL = False


@contextmanager
def locked_file(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+b") as f:
        if _HAS_FCNTL:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield f
        finally:
            f.flush()
            os.fsync(f.fileno())
            if _HAS_FCNTL:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def update_json(path: Path, default, merge_fn: Callable[[object], T]) -> T:
    """Lock, load current JSON, compute new state, atomically replace file."""
    lock_path = path.with_suffix(path.suffix + ".lock")
    with locked_file(lock_path):
        current = read_json(path, default)
        new_state = merge_fn(current)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(new_state, indent=2))
        os.replace(tmp, path)
        return new_state
