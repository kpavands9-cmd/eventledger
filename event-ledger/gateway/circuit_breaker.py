"""
Circuit breaker with retry + exponential backoff + jitter (stdlib only).

States:
  CLOSED    — normal; calls pass through (with retry on transient failure)
  OPEN      — failing; calls rejected immediately with CircuitBreakerOpen
  HALF_OPEN — probing; one call let through to test recovery

Retry policy (CLOSED state only):
  - Up to `max_retries` attempts before counting as a CB failure
  - Delay doubles each attempt: base_delay * 2^attempt
  - Full jitter applied: actual_delay = random(0, computed_delay)
  - Capped at `max_delay` seconds
"""
import logging
import math
import random
import threading
import time
from typing import Callable, Any

logger = logging.getLogger("gateway.circuit_breaker")


class CircuitBreakerOpen(Exception):
    """Raised when a call is blocked because the circuit is OPEN."""


class CircuitBreaker:
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        failure_threshold:   int   = 3,
        recovery_timeout:    float = 15.0,
        half_open_max_calls: int   = 1,
        # Retry settings
        max_retries:   int   = 2,
        base_delay:    float = 0.1,   # seconds
        max_delay:     float = 2.0,   # seconds
        retry_enabled: bool  = True,
    ):
        self.failure_threshold   = failure_threshold
        self.recovery_timeout    = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.max_retries         = max_retries
        self.base_delay          = base_delay
        self.max_delay           = max_delay
        self.retry_enabled       = retry_enabled

        self._state           = self.CLOSED
        self._failure_count   = 0
        self._last_failure_at = 0.0
        self._half_open_calls = 0
        self._lock            = threading.Lock()

        # Stats
        self._total_retries = 0

    @property
    def state(self) -> str:
        return self._state

    @property
    def total_retries(self) -> int:
        with self._lock:
            return self._total_retries

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        """Execute fn(*args, **kwargs) subject to CB and retry logic."""
        with self._lock:
            self._maybe_transition()
            if self._state == self.OPEN:
                raise CircuitBreakerOpen(
                    "Circuit breaker OPEN — Account Service suspended"
                )
            if self._state == self.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitBreakerOpen(
                        "Circuit breaker HALF_OPEN — probe in progress"
                    )
                self._half_open_calls += 1

        # CLOSED or HALF_OPEN probe — attempt with retries
        last_exc = None
        attempts = self.max_retries + 1 if self.retry_enabled else 1

        for attempt in range(attempts):
            try:
                result = fn(*args, **kwargs)
                self._on_success()
                return result
            except CircuitBreakerOpen:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    delay = self._jittered_delay(attempt)
                    logger.warning(
                        f"CB retry {attempt + 1}/{self.max_retries} "
                        f"after {delay:.2f}s: {exc}"
                    )
                    with self._lock:
                        self._total_retries += 1
                    time.sleep(delay)

        # All attempts exhausted
        self._on_failure(last_exc)
        raise last_exc

    def _jittered_delay(self, attempt: int) -> float:
        """Full-jitter exponential backoff: random(0, min(cap, base * 2^attempt))."""
        cap = min(self.max_delay, self.base_delay * math.pow(2, attempt))
        return random.uniform(0, cap)

    def _maybe_transition(self):
        """Must be called while holding self._lock."""
        if self._state == self.OPEN:
            if time.monotonic() - self._last_failure_at >= self.recovery_timeout:
                logger.warning("Circuit breaker → HALF_OPEN (probing Account Service)")
                self._state = self.HALF_OPEN
                self._half_open_calls = 0

    def _on_success(self):
        with self._lock:
            if self._state != self.CLOSED:
                logger.info("Circuit breaker → CLOSED (Account Service recovered)")
            self._state = self.CLOSED
            self._failure_count = 0

    def _on_failure(self, exc: Exception):
        with self._lock:
            self._failure_count += 1
            self._last_failure_at = time.monotonic()
            logger.error(
                f"Circuit breaker failure #{self._failure_count}: {exc}"
            )
            if (self._state == self.HALF_OPEN or
                    self._failure_count >= self.failure_threshold):
                logger.warning(
                    f"Circuit breaker → OPEN after {self._failure_count} failure(s)"
                )
                self._state = self.OPEN
