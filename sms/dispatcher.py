"""Central outbound dispatcher with rate limits and quiet-hour awareness."""

from __future__ import annotations

import heapq
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional

from . import spec
from .datastore import REPOSITORY


@dataclass
class OutboundMessage:
    to_number: str
    from_number: str
    body: str
    campaign_id: Optional[str] = None
    template_id: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class DispatchResult:
    queued: bool
    reason: Optional[str]
    available_at: datetime


class Dispatcher:
    """Queues outbound messages and enforces sending policy."""

    def __init__(self) -> None:
        limits = spec.rate_limits()
        self.rate_per_number = limits["rate_per_number"]
        self.global_per_min = limits["global_per_min"]
        self.daily_limit = limits["daily_limit"]
        self.jitter_seconds = limits["jitter_seconds"]

        self._per_number: Dict[str, Deque[datetime]] = defaultdict(deque)
        self._global_window: Deque[datetime] = deque()
        self._daily_totals: Dict[str, int] = defaultdict(int)
        self._queue: List[tuple[datetime, OutboundMessage]] = []

    # Internal helpers -----------------------------------------------------
    def _now(self) -> datetime:
        return datetime.utcnow()

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(minutes=1)
        while self._global_window and self._global_window[0] < cutoff:
            self._global_window.popleft()
        for window in self._per_number.values():
            while window and window[0] < cutoff:
                window.popleft()

    def _increment_counters(self, number: str, timestamp: datetime) -> None:
        digits = spec.normalize_phone(number) or number
        self._global_window.append(timestamp)
        self._per_number[digits].append(timestamp)
        self._daily_totals[digits] += 1

    def _daily_exhausted(self, number: str) -> bool:
        digits = spec.normalize_phone(number) or number
        return self._daily_totals[digits] >= self.daily_limit

    # Public API -----------------------------------------------------------
    def queue(self, message: OutboundMessage) -> DispatchResult:
        now = self._now()
        self._prune(now)

        reason: Optional[str] = None
        available_at = now
        digits = spec.normalize_phone(message.from_number) or message.from_number
        per_number_window = self._per_number[digits]

        if spec.is_quiet_hours():
            reason = "quiet_hours"
            available_at = now + timedelta(seconds=self.jitter_seconds)
        elif self._daily_exhausted(message.from_number):
            reason = "daily_cap"
            available_at = now + timedelta(minutes=10)
        elif len(per_number_window) >= self.rate_per_number:
            reason = "number_rate"
            available_at = per_number_window[0] + timedelta(minutes=1)
        elif len(self._global_window) >= self.global_per_min:
            reason = "global_rate"
            available_at = self._global_window[0] + timedelta(minutes=1)
        else:
            self._increment_counters(message.from_number, now)
            available_at = now + timedelta(seconds=self.jitter_seconds)

        heapq.heappush(self._queue, (available_at, message))
        REPOSITORY.increment_number_counters(
            message.from_number,
            sent_total=1,
            sent_today=1,
        )
        return DispatchResult(queued=True, reason=reason, available_at=available_at)

    def pop_ready(self, now: Optional[datetime] = None) -> List[OutboundMessage]:
        now = now or self._now()
        ready: List[OutboundMessage] = []
        while self._queue and self._queue[0][0] <= now:
            _, message = heapq.heappop(self._queue)
            ready.append(message)
        return ready

    def pending_count(self) -> int:
        return len(self._queue)


DISPATCHER = Dispatcher()

