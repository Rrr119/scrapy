from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from itertools import pairwise

import pytest

from scrapy.downloadermiddlewares import domainratelimit
from scrapy.downloadermiddlewares.domainratelimit import DomainRateLimitMiddleware
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response
from scrapy.settings.default_settings import DOWNLOADER_MIDDLEWARES_BASE
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test


class _Clock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


def _middleware(
    settings: dict[str, object] | None = None,
) -> DomainRateLimitMiddleware:
    crawler = get_crawler(
        settings_dict={"DOMAIN_RATE_LIMIT_ENABLED": True, **(settings or {})}
    )
    return build_from_crawler(DomainRateLimitMiddleware, crawler)


def _use_clock(monkeypatch: pytest.MonkeyPatch, clock: _Clock) -> None:
    monkeypatch.setattr(domainratelimit, "monotonic", clock.monotonic)
    monkeypatch.setattr(domainratelimit, "sleep", clock.sleep)


def test_disabled_by_default() -> None:
    crawler = get_crawler()
    with pytest.raises(NotConfigured):
        build_from_crawler(DomainRateLimitMiddleware, crawler)


def test_runs_before_retry_on_responses() -> None:
    domain_limit_order = DOWNLOADER_MIDDLEWARES_BASE[
        "scrapy.downloadermiddlewares.domainratelimit.DomainRateLimitMiddleware"
    ]
    retry_order = DOWNLOADER_MIDDLEWARES_BASE[
        "scrapy.downloadermiddlewares.retry.RetryMiddleware"
    ]
    assert domain_limit_order > retry_order


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        ("DOMAIN_RATE_LIMIT_RATE", 0),
        ("DOMAIN_RATE_LIMIT_BURST", 0.5),
        ("DOMAIN_RATE_LIMIT_MIN_RATE", 0),
        ("DOMAIN_RATE_LIMIT_MIN_RATE", 1.5),
        ("DOMAIN_RATE_LIMIT_RECOVERY_WINDOW", 0),
        ("DOMAIN_RATE_LIMIT_RECOVERY_FACTOR", 0),
        ("DOMAIN_RATE_LIMIT_RECOVERY_FACTOR", 1.5),
        ("DOMAIN_RATE_LIMIT_BACKOFF_START", 0),
        ("DOMAIN_RATE_LIMIT_BACKOFF_MAX", 0.5),
        ("DOMAIN_RATE_LIMIT_BACKOFF_JITTER", -0.1),
        ("DOMAIN_RATE_LIMIT_BACKOFF_JITTER", 1.1),
    ],
)
def test_invalid_settings(setting: str, value: float) -> None:
    crawler = get_crawler(
        settings_dict={
            "DOMAIN_RATE_LIMIT_ENABLED": True,
            "DOMAIN_RATE_LIMIT_RATE": 1.0,
            setting: value,
        }
    )
    with pytest.raises(NotConfigured):
        build_from_crawler(DomainRateLimitMiddleware, crawler)


@coroutine_test
async def test_token_bucket_burst_and_refill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware(
        {
            "DOMAIN_RATE_LIMIT_RATE": 10,
            "DOMAIN_RATE_LIMIT_BURST": 2,
            "DOMAIN_RATE_LIMIT_MIN_RATE": 1,
        }
    )
    request = Request("https://example.com")
    times = []

    for _ in range(3):
        await middleware.process_request(request)
        times.append(clock.now)

    assert times[:2] == [1_000.0, 1_000.0]
    assert times[2] == pytest.approx(1_000.1)
    assert clock.sleeps == [pytest.approx(0.1)]


@coroutine_test
async def test_requests_without_hostname_are_not_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware()
    request = Request("data:,hello")
    response = Response(request.url, status=404)

    await middleware.process_request(request)
    assert middleware.process_response(request, response) is response
    assert middleware.crawler.stats.get_stats() == {}


@coroutine_test
async def test_concurrent_requests_do_not_exceed_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware(
        {
            "DOMAIN_RATE_LIMIT_RATE": 20,
            "DOMAIN_RATE_LIMIT_BURST": 1,
            "DOMAIN_RATE_LIMIT_MIN_RATE": 1,
        }
    )
    request = Request("https://example.com")
    completion_times: list[float] = []

    async def send() -> None:
        await middleware.process_request(request)
        completion_times.append(clock.now)

    await asyncio.gather(*(send() for _ in range(5)))

    assert completion_times[0] == 1_000.0
    for previous, current in pairwise(completion_times):
        assert current - previous == pytest.approx(0.05)
    assert (
        middleware.crawler.stats.get_value("domain_rate_limit/wait_time/example.com")
        > 0
    )


@coroutine_test
async def test_slow_domain_does_not_block_another_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    release_slow_wait = asyncio.Event()

    async def controlled_sleep(seconds: float) -> None:
        clock.sleeps.append(seconds)
        clock.now += seconds
        if seconds >= 60:
            await release_slow_wait.wait()
        else:
            await asyncio.sleep(0)

    monkeypatch.setattr(domainratelimit, "monotonic", clock.monotonic)
    monkeypatch.setattr(domainratelimit, "sleep", controlled_sleep)
    middleware = _middleware()
    slow_request = Request("https://slow.example")
    other_request = Request("https://other.example")

    await middleware.process_request(slow_request)
    response = Response(
        slow_request.url,
        status=429,
        headers={"Retry-After": "60"},
    )
    middleware.process_response(slow_request, response)

    slow_done = False

    async def wait_for_slow_domain() -> None:
        nonlocal slow_done
        await middleware.process_request(slow_request)
        slow_done = True

    slow_task = asyncio.create_task(wait_for_slow_domain())
    await asyncio.sleep(0)
    assert not slow_done

    await middleware.process_request(other_request)
    assert not slow_done

    release_slow_wait.set()
    await slow_task
    assert slow_done


@coroutine_test
async def test_retry_after_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware()
    request = Request("https://example.com")
    await middleware.process_request(request)
    response = Response(request.url, status=429, headers={"Retry-After": "2"})

    assert middleware.process_response(request, response) is response
    await middleware.process_request(request)

    assert clock.sleeps == [pytest.approx(2)]
    assert (
        middleware.crawler.stats.get_value(
            "domain_rate_limit/throttle_count/example.com"
        )
        == 1
    )


@pytest.mark.parametrize("status", [302, 404])
@coroutine_test
async def test_non_success_responses_do_not_change_rate(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware()
    request = Request("https://example.com")
    await middleware.process_request(request)
    response = Response(request.url, status=status)

    middleware.process_response(request, response)

    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(1)
    assert (
        middleware.crawler.stats.get_value(
            "domain_rate_limit/throttle_count/example.com"
        )
        is None
    )


@coroutine_test
async def test_retry_after_http_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    monkeypatch.setattr(domainratelimit, "wall_time", clock.monotonic)
    middleware = _middleware()
    request = Request("https://example.com")
    await middleware.process_request(request)
    retry_at = datetime.fromtimestamp(clock.now + 3, UTC)
    response = Response(
        request.url,
        status=503,
        headers={"Retry-After": retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")},
    )

    middleware.process_response(request, response)
    await middleware.process_request(request)

    assert clock.sleeps == [pytest.approx(3)]


@coroutine_test
async def test_retry_after_without_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    monkeypatch.setattr(domainratelimit, "wall_time", clock.monotonic)
    middleware = _middleware()
    request = Request("https://example.com")
    await middleware.process_request(request)
    retry_at = datetime.fromtimestamp(clock.now + 2, UTC).replace(tzinfo=None)
    response = Response(
        request.url,
        status=429,
        headers={"Retry-After": retry_at.strftime("%a, %d %b %Y %H:%M:%S")},
    )

    middleware.process_response(request, response)
    await middleware.process_request(request)

    assert clock.sleeps == [pytest.approx(2)]


@coroutine_test
async def test_backoff_without_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware(
        {
            "DOMAIN_RATE_LIMIT_BACKOFF_START": 2,
            "DOMAIN_RATE_LIMIT_BACKOFF_MAX": 3,
            "DOMAIN_RATE_LIMIT_BACKOFF_JITTER": 0,
            "DOMAIN_RATE_LIMIT_MIN_RATE": 0.5,
        }
    )
    request = Request("https://example.com")

    await middleware.process_request(request)
    middleware.process_response(request, Response(request.url, status=429))
    await middleware.process_request(request)
    middleware.process_response(request, Response(request.url, status=503))
    await middleware.process_request(request)

    assert clock.sleeps == [pytest.approx(2), pytest.approx(3)]


@pytest.mark.parametrize("header", [None, "", "not-a-date", "nan"])
@coroutine_test
async def test_invalid_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    header: str | None,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware(
        {
            "DOMAIN_RATE_LIMIT_BACKOFF_JITTER": 0,
            "DOMAIN_RATE_LIMIT_MIN_RATE": 1,
        }
    )
    request = Request("https://example.com")
    await middleware.process_request(request)
    headers = {} if header is None else {"Retry-After": header}
    middleware.process_response(
        request, Response(request.url, status=429, headers=headers)
    )
    await middleware.process_request(request)

    assert clock.sleeps == [pytest.approx(1)]


@coroutine_test
async def test_rate_recovers_with_sliding_success_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware(
        {
            "DOMAIN_RATE_LIMIT_RATE": 4,
            "DOMAIN_RATE_LIMIT_MIN_RATE": 0.5,
            "DOMAIN_RATE_LIMIT_RECOVERY_WINDOW": 10,
            "DOMAIN_RATE_LIMIT_RECOVERY_FACTOR": 0.5,
        }
    )
    request = Request("https://example.com")
    await middleware.process_request(request)
    middleware.process_response(
        request,
        Response(request.url, status=429, headers={"Retry-After": "0"}),
    )

    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(2)

    middleware.process_response(request, Response(request.url, status=200))
    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(2.5)

    clock.now += 11
    middleware.process_response(request, Response(request.url, status=200))
    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(3.25)

    for _ in range(20):
        middleware.process_response(request, Response(request.url, status=200))
    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(4)


@coroutine_test
async def test_stats_per_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    middleware = _middleware()
    request = Request("https://example.com")

    await middleware.process_request(request)
    middleware.process_response(
        request,
        Response(request.url, status=429, headers={"Retry-After": "1"}),
    )
    await middleware.process_request(request)

    assert (
        middleware.crawler.stats.get_value(
            "domain_rate_limit/request_count/example.com"
        )
        == 2
    )
    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/wait_time/example.com"
    ) == pytest.approx(2)
    assert (
        middleware.crawler.stats.get_value(
            "domain_rate_limit/throttle_count/example.com"
        )
        == 1
    )
    assert middleware.crawler.stats.get_value(
        "domain_rate_limit/rate/example.com"
    ) == pytest.approx(0.5)
