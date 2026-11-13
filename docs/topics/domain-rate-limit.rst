.. _domain-rate-limit:

====================
Domain rate limiting
====================

.. currentmodule:: scrapy.downloadermiddlewares.domainratelimit

The :class:`DomainRateLimitMiddleware` limits the request rate of every domain
independently. It is useful for servers that publish explicit rate limits and
return ``429 Too Many Requests`` or ``503 Service Unavailable`` responses.

This middleware is disabled by default. Set
:setting:`DOMAIN_RATE_LIMIT_ENABLED` to ``True`` to enable it for the whole
crawler.

How it works
============

Each URL hostname gets its own token bucket. A bucket starts with
:setting:`DOMAIN_RATE_LIMIT_BURST` tokens and refills at
:setting:`DOMAIN_RATE_LIMIT_RATE` tokens per second. A request is sent only
after it takes one token, so concurrent requests to one domain cannot exceed
the configured rate. Buckets and their asynchronous locks are independent:
waiting for a slow or paused domain does not delay requests to other domains.

A ``429`` or ``503`` response pauses all later requests for that domain:

*   If the response has a ``Retry-After`` header, its value is used as the
    pause duration. Both delta-seconds, such as ``120``, and HTTP-date values,
    such as ``Fri, 13 Nov 2026 08:00:00 GMT``, are supported.
*   Otherwise, the pause uses exponential backoff with random jitter.

The paused domain also reduces its current rate. Throttled responses are
recorded as failures and later successful ``2xx`` responses are recorded as
successes in a sliding window; while that window shows a higher success ratio,
the rate moves gradually back toward :setting:`DOMAIN_RATE_LIMIT_RATE`. This
makes recovery proportional to recent server responses instead of immediately
returning to the original rate.

Requests without a hostname, such as ``data:`` URLs, are not limited.

Interaction with other components
=================================

With the default priorities, the middleware runs after
:class:`~scrapy.downloadermiddlewares.retry.RetryMiddleware` for requests and
before it for responses (middleware order ``555`` versus ``550``). This has two
consequences:

*   A ``429`` or ``503`` response updates the domain pause before
    ``RetryMiddleware`` creates its retry request. The retry request then waits
    for the same domain pause and token bucket instead of being sent
    immediately.
*   Disabling retries with :reqmeta:`dont_retry` or by removing
    :setting:`RETRY_HTTP_CODES` does not disable domain limits. The response
    still pauses the domain for the other queued requests.

The per-domain token bucket and
:ref:`AutoThrottle <topics-autothrottle>` are independent throttling
mechanisms and can be enabled together. AutoThrottle adjusts download slot
delays and effective concurrency from response latency, while this middleware
enforces a domain-level request rate before requests reach their download
slot. When both apply to the same request, both waits are respected and the
combined behavior can be more restrictive than either mechanism alone.

Settings
========

.. setting:: DOMAIN_RATE_LIMIT_ENABLED

DOMAIN_RATE_LIMIT_ENABLED
-------------------------

Default: ``False``

Whether the domain rate limiting middleware is enabled.

.. setting:: DOMAIN_RATE_LIMIT_RATE

DOMAIN_RATE_LIMIT_RATE
----------------------

Default: ``1.0``

The maximum number of requests per second sent to each domain once a bucket
has recovered. It must be higher than ``0``.

.. setting:: DOMAIN_RATE_LIMIT_BURST

DOMAIN_RATE_LIMIT_BURST
-----------------------

Default: ``1.0``

The token capacity of each domain bucket, which is the maximum number of
requests that can be sent immediately after the bucket is full. It must be at
least ``1``.

.. setting:: DOMAIN_RATE_LIMIT_MIN_RATE

DOMAIN_RATE_LIMIT_MIN_RATE
--------------------------

Default: ``0.1``

The lowest request rate per second to which a domain can be reduced after
``429`` or ``503`` responses. It must be higher than ``0`` and no higher than
:setting:`DOMAIN_RATE_LIMIT_RATE`.

.. setting:: DOMAIN_RATE_LIMIT_RECOVERY_WINDOW

DOMAIN_RATE_LIMIT_RECOVERY_WINDOW
---------------------------------

Default: ``30.0``

The duration in seconds of the sliding window used to calculate the recent
success ratio for rate recovery. It must be higher than ``0``.

.. setting:: DOMAIN_RATE_LIMIT_RECOVERY_FACTOR

DOMAIN_RATE_LIMIT_RECOVERY_FACTOR
---------------------------------

Default: ``0.1``

How far a successful response moves the current rate toward
:setting:`DOMAIN_RATE_LIMIT_RATE`, as a fraction multiplied by the sliding
window success ratio. It must be higher than ``0`` and no higher than ``1``.

.. setting:: DOMAIN_RATE_LIMIT_BACKOFF_START

DOMAIN_RATE_LIMIT_BACKOFF_START
-------------------------------

Default: ``1.0``

The first pause in seconds when a ``429`` or ``503`` response has no usable
``Retry-After`` header. The pause doubles for each consecutive throttled
response for that domain. It must be higher than ``0``.

.. setting:: DOMAIN_RATE_LIMIT_BACKOFF_MAX

DOMAIN_RATE_LIMIT_BACKOFF_MAX
-----------------------------

Default: ``60.0``

The maximum base pause in seconds before jitter is applied to exponential
backoff. It must be no lower than
:setting:`DOMAIN_RATE_LIMIT_BACKOFF_START`. This cap does not apply to a
``Retry-After`` value supplied by the server.

.. setting:: DOMAIN_RATE_LIMIT_BACKOFF_JITTER

DOMAIN_RATE_LIMIT_BACKOFF_JITTER
--------------------------------

Default: ``0.25``

The magnitude of random variation applied to exponential backoff pauses,
between ``0`` and ``1``. For example, ``0.25`` makes the pause vary between
75% and 125% of its calculated value.

Stats
=====

The middleware records the following stats for each domain:

``domain_rate_limit/request_count/<domain>``
    Number of requests that reached the rate limiter.

``domain_rate_limit/wait_time/<domain>``
    Total time in seconds that requests for the domain spent waiting for the
    token bucket or a pause.

``domain_rate_limit/throttle_count/<domain>``
    Number of ``429`` or ``503`` responses that paused the domain.

``domain_rate_limit/rate/<domain>``
    Current request rate in requests per second for the domain.

For example::

    DOMAIN_RATE_LIMIT_ENABLED = True
    DOMAIN_RATE_LIMIT_RATE = 5
    DOMAIN_RATE_LIMIT_BURST = 10

.. currentmodule:: None
