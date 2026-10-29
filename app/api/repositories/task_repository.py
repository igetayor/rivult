"""
TaskRepository — in-memory + Celery state registry for task tracking.

Provides persistence of dispatched task metadata for listing, dashboard feeds,
and replay. Controllers should never access this directly — call TaskService.
"""

from typing import Any, Dict, List, Optional


class TaskRepository:
    """
    In-memory store for dispatched task metadata.

    In a production system this would be backed by Redis HASH/ZSET or
    a database table. The interface is the same regardless of backing store.
    """

    def __init__(self, max_size: int = 100) -> None:
        self._registry: List[Dict[str, Any]] = []
        self._max_size = max_size
        self._engine_stats: Dict[str, Any] = {
            "total_dispatched": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
        }

    # ------------------------------------------------------------------ #
    #  Writes                                                              #
    # ------------------------------------------------------------------ #

    def insert(self, entry: Dict[str, Any]) -> None:
        """Prepend a new task record, evicting oldest if over limit."""
        self._registry.insert(0, entry)
        if len(self._registry) > self._max_size:
            self._registry.pop()
        self._engine_stats["total_dispatched"] += 1

    def increment_stat(self, key: str) -> None:
        """Increment a specific engine stat counter (completed, failed, retried)."""
        if key in self._engine_stats:
            self._engine_stats[key] += 1

    # ------------------------------------------------------------------ #
    #  Reads                                                               #
    # ------------------------------------------------------------------ #

    def list(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Return the most recent N task records."""
        return self._registry[:limit]

    def get_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Find a task record by Celery task ID."""
        return next((t for t in self._registry if t.get("task_id") == task_id), None)

    def get_stats(self) -> Dict[str, Any]:
        """Return raw engine stat counters."""
        return dict(self._engine_stats)
