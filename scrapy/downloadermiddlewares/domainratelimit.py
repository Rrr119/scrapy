from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC
from email.utils import parsedate_to_datetime
from random import uniform
from time import monotonic
from time import time as wall_time
from typing import TYPE_CHECKING, Self

from scrapy.exceptions import NotConfigured
from scrapy.utils.asyncio import sleep
from scrapy.utils.httpobj import urlparse_cached

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.http import Request, Response
    from scrapy.statscollectors import StatsCollector


_STATS_PREFIX = "domain_rate_limit"
_THROTTLE_STATUSES = {429, 503}
_RATE_DECREASE_FACTOR = 0.5


def _parse_retry_after(value: bytes | str | None) -> float | None:
    if value is None:
        return None
    text = value.decode("ascii", "ignore") if isinstance(value, bytes) else value
    text = text.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, retry_at.timestamp() - wall_time())
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


@dataclass
class _DomainState:
    rate: float
    tokens: float
    updated: float
    max_rate: float
    burst: float
    min_rate: float
    recovery_window: float
    recovery_factor: float
    paused_until: float = 0.0
    failures: int = 0
    outcomes: deque[tuple[float, bool]] = field(default_factory=deque)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class DomainRateLimitMiddleware:
    """Limit the request rate of every domain independently.

    Each domain gets a token bucket with the configured *rate* and *burst*
    capacity. A ``429`` or ``503`` response pauses that domain, using the
    ``Retry-After`` header when present and exponential backoff with jitter
    otherwise. Successful responses slowly restore the rate based on the
    success ratio observed during a sliding time window.

    .. versionadded:: VERSION

    Set :setting:`DOMAIN_RATE_LIMIT_ENABLED` to ``True`` to enable the
    middleware.
    """

    def __init__(self, crawler: Crawler) -> None:
        self.crawler = crawler
        settings = crawler.settings
        if not settings.getbool("DOMAIN_RATE_LIMIT_ENABLED"):
            raise NotConfigured

        rate = settings.getfloat("DOMAIN_RATE_LIMIT_RATE")
        burst = settings.getfloat("DOMAIN_RATE_LIMIT_BURST")
        min_rate = settings.getfloat("DOMAIN_RATE_LIMIT_MIN_RATE")
        recovery_window = settings.getfloat("DOMAIN_RATE_LIMIT_RECOVERY_WINDOW")
        recovery_factor = settings.getfloat("DOMAIN_RATE_LIMIT_RECOVERY_FACTOR")
        backoff_start = settings.getfloat("DOMAIN_RATE_LIMIT_BACKOFF_START")
        backoff_max = settings.getfloat("DOMAIN_RATE_LIMIT_BACKOFF_MAX")
        backoff_jitter = settings.getfloat("DOMAIN_RATE_LIMIT_BACKOFF_JITTER")

        if rate <= 0:
            raise NotConfigured("DOMAIN_RATE_LIMIT_RATE must be higher than 0.")
        if burst < 1:
            raise NotConfigured("DOMAIN_RATE_LIMIT_BURST must be at least 1.")
        if min_rate <= 0 or min_rate > rate:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_MIN_RATE must be higher than 0 and no higher "
                "than DOMAIN_RATE_LIMIT_RATE."
            )
        if recovery_window <= 0:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_RECOVERY_WINDOW must be higher than 0."
            )
        if not 0 < recovery_factor <= 1:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_RECOVERY_FACTOR must be higher than 0 and "
                "no higher than 1."
            )
        if backoff_start <= 0:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_BACKOFF_START must be higher than 0."
            )
        if backoff_max < backoff_start:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_BACKOFF_MAX must be no lower than "
                "DOMAIN_RATE_LIMIT_BACKOFF_START."
            )
        if not 0 <= backoff_jitter <= 1:
            raise NotConfigured(
                "DOMAIN_RATE_LIMIT_BACKOFF_JITTER must be between 0 and 1."
            )

        self._rate = rate
        self._burst = burst
        self._min_rate = min_rate
        self._recovery_window = recovery_window
        self._recovery_factor = recovery_factor
        self._backoff_start = backoff_start
        self._backoff_max = backoff_max
        self._backoff_jitter = backoff_jitter
        self._states: dict[str, _DomainState] = {}
        assert crawler.stats is not None
        self._stats: StatsCollector = crawler.stats

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(crawler)

    async def process_request(self, request: Request) -> None:
        domain = urlparse_cached(request).hostname
        if not domain:
            return
        state = self._get_state(domain)
        self._stats.inc_value(f"{_STATS_PREFIX}/request_count/{domain}")
        started = monotonic()
        async with state.lock:
            await self._wait_for_token(state)
        waited = monotonic() - started
        if waited > 0:
            self._stats.inc_value(f"{_STATS_PREFIX}/wait_time/{domain}", waited)

    def process_response(self, request: Request, response: Response) -> Response:
        domain = urlparse_cached(request).hostname
        if not domain:
            return response
        state = self._get_state(domain)
        if response.status in _THROTTLE_STATUSES:
            self._throttle(domain, state, response)
        elif 200 <= response.status < 300:
            self._recover(domain, state)
        return response

    def _get_state(self, domain: str) -> _DomainState:
        state = self._states.get(domain)
        if state is None:
            now = monotonic()
            state = _DomainState(
                rate=self._rate,
                tokens=self._burst,
                updated=now,
                max_rate=self._rate,
                burst=self._burst,
                min_rate=self._min_rate,
                recovery_window=self._recovery_window,
                recovery_factor=self._recovery_factor,
            )
            self._states[domain] = state
            self._set_rate_stat(domain, state)
        return state

    async def _wait_for_token(self, state: _DomainState) -> None:
        while True:
            now = monotonic()
            if state.paused_until > now:
                await sleep(state.paused_until - now)
                continue

            elapsed = now - state.updated
            if elapsed > 0:
                state.tokens = min(state.burst, state.tokens + elapsed * state.rate)
                state.updated = now

            if state.tokens >= 1 - 1e-12:
                state.tokens = max(0.0, state.tokens - 1)
                return
            await sleep((1 - state.tokens) / state.rate)

    def _throttle(self, domain: str, state: _DomainState, response: Response) -> None:
        now = monotonic()
        state.rate = max(state.min_rate, state.rate * _RATE_DECREASE_FACTOR)
        state.tokens = 0
        state.updated = now
        state.failures += 1
        self._record_outcome(state, now, success=False)

        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        if retry_after is None:
            delay = min(
                self._backoff_max,
                self._backoff_start * (2 ** (state.failures - 1)),
            )
            delay *= uniform(  # noqa: S311
                1 - self._backoff_jitter, 1 + self._backoff_jitter
            )
        else:
            delay = retry_after
        state.paused_until = max(state.paused_until, now + delay)

        self._stats.inc_value(f"{_STATS_PREFIX}/throttle_count/{domain}")
        self._set_rate_stat(domain, state)

    def _recover(self, domain: str, state: _DomainState) -> None:
        now = monotonic()
        state.failures = 0
        self._record_outcome(state, now, success=True)
        successes = sum(success for _, success in state.outcomes)
        success_ratio = successes / len(state.outcomes)
        state.rate = min(
            state.max_rate,
            state.rate
            + (state.max_rate - state.rate) * state.recovery_factor * success_ratio,
        )
        self._set_rate_stat(domain, state)

    @staticmethod
    def _record_outcome(state: _DomainState, now: float, *, success: bool) -> None:
        state.outcomes.append((now, success))
        window_start = now - state.recovery_window
        while state.outcomes and state.outcomes[0][0] < window_start:
            state.outcomes.popleft()

    def _set_rate_stat(self, domain: str, state: _DomainState) -> None:
        self._stats.set_value(f"{_STATS_PREFIX}/rate/{domain}", state.rate)
