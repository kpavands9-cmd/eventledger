"""
Token-bucket rate limiter (stdlib only).

Each client IP gets its own bucket. Tokens refill continuously at `rate`
tokens per second up to `capacity`. A request that finds no tokens is
rejected with 429 Too Many Requests.

Configuration via environment variables:
  RATE_LIMIT_RPS      – requests per second per IP (default 20)
  RATE_LIMIT_BURST    – burst capacity (default 40)
  RATE_LIMIT_ENABLED  – set to "0" to disable (default "1")
"""
import os
import threading
import time


_ENABLED  = os.environ.get("RATE_LIMIT_ENABLED", "1") != "0"
_RATE     = float(os.environ.get("RATE_LIMIT_RPS",   "20"))   # tokens/sec
_CAPACITY = float(os.environ.get("RATE_LIMIT_BURST",  "40"))  # max tokens


class _Bucket:
    __slots__ = ("tokens", "last_refill", "lock")

    def __init__(self, capacity: float):
        self.tokens     = capacity
        self.last_refill = time.monotonic()
        self.lock       = threading.Lock()

    def consume(self, rate: float, capacity: float) -> bool:
        with self.lock:
            now     = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(capacity, self.tokens + elapsed * rate)
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


class RateLimiter:
    def __init__(self, rate: float = _RATE, capacity: float = _CAPACITY,
                 enabled: bool = _ENABLED):
        self._rate     = rate
        self._capacity = capacity
        self._enabled  = enabled
        self._buckets: dict[str, _Bucket] = {}
        self._lock     = threading.Lock()
        self._rejected = 0

    def allow(self, client_ip: str) -> bool:
        """Return True if request is allowed, False if rate-limited."""
        if not self._enabled:
            return True
        with self._lock:
            if client_ip not in self._buckets:
                self._buckets[client_ip] = _Bucket(self._capacity)
            bucket = self._buckets[client_ip]
        allowed = bucket.consume(self._rate, self._capacity)
        if not allowed:
            with self._lock:
                self._rejected += 1
        return allowed

    @property
    def rejected_total(self) -> int:
        with self._lock:
            return self._rejected

    def stats(self) -> dict:
        with self._lock:
            return {
                "enabled":           self._enabled,
                "rate_rps":          self._rate,
                "burst_capacity":    self._capacity,
                "tracked_ips":       len(self._buckets),
                "rate_limited_total": self._rejected,
            }
