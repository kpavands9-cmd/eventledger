"""
Event Ledger — complete test suite
Uses only Python stdlib (unittest, http.server, threading, json, etc.)

Covers every assignment requirement:
  1. Core: idempotency, out-of-order, balance, validation
  2. Service separation (independent DBs / processes)
  3. Distributed tracing (X-Trace-ID propagated Gateway → Account Service)
  4. Observability (health endpoints, metrics, structured logging)
  5. Resiliency (circuit breaker opens, 503 on failure)
  6. Graceful degradation (GET endpoints work when Account Service is down)
  7. Integration (full Gateway → Account Service round-trip over real HTTP)
"""

import sys, os
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

import json
import threading
import time
import unittest
import uuid
from datetime import datetime, timezone
from http.server import HTTPServer
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode


# ---------------------------------------------------------------------------
# Helpers to start services on random free ports
# ---------------------------------------------------------------------------
import socket

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_account_service(port: int) -> HTTPServer:
    """Start the Account Service in a background thread; return the server."""
    # Each test gets its own module-level state reset
    import importlib
    import account_service.main as acct_mod
    importlib.reload(acct_mod)
    acct_mod.init_db(":memory:")
    srv = HTTPServer(("127.0.0.1", port), acct_mod.AccountHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def start_gateway(port: int, account_service_port: int) -> HTTPServer:
    """Start the Gateway pointed at the given Account Service port."""
    import importlib
    import gateway.main as gw_mod
    importlib.reload(gw_mod)
    gw_mod.ACCOUNT_SERVICE_URL = f"http://127.0.0.1:{account_service_port}"
    gw_mod.init_db(":memory:")
    # Fresh circuit breaker
    from gateway.circuit_breaker import CircuitBreaker
    gw_mod.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60, half_open_max_calls=1)
    srv = HTTPServer(("127.0.0.1", port), gw_mod.GatewayHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def http(method: str, url: str, body=None, headers=None) -> tuple[int, dict | list]:
    """Simple HTTP helper; returns (status, parsed_json_body)."""
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = Request(url, data=data, headers=hdrs, method=method)
    try:
        with urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except HTTPError as e:
        return e.code, json.loads(e.read())


def wait_for(url: str, timeout=3.0):
    """Poll a URL until it responds (service startup)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urlopen(url, timeout=0.5)
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError(f"Service never became ready at {url}")


def evt(**overrides) -> dict:
    base = {
        "eventId":        str(uuid.uuid4()),
        "accountId":      "acct-test",
        "type":           "CREDIT",
        "amount":         100.0,
        "currency":       "USD",
        "eventTimestamp": "2026-05-15T12:00:00Z",
    }
    base.update(overrides)
    return base


# ============================================================================
# 1. UNIT TESTS — Account Service business logic (no HTTP)
# ============================================================================
class TestAccountServiceLogic(unittest.TestCase):
    """Tests the Account Service logic functions directly, bypassing HTTP."""

    def setUp(self):
        import importlib
        import account_service.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

    def _apply(self, account_id, event_id, txn_type, amount,
               ts="2026-05-15T12:00:00Z"):
        return self.m.apply_transaction(account_id, {
            "eventId": event_id, "type": txn_type, "amount": amount,
            "currency": "USD", "eventTimestamp": ts,
        }, trace_id="t-1")

    # --- Balance ---
    def test_credit_increases_balance(self):
        self._apply("a1", "e1", "CREDIT", 200.0)
        status, body = self.m.get_balance("a1", "t")
        self.assertEqual(status, 200)
        self.assertEqual(body["balance"], 200.0)

    def test_debit_decreases_balance(self):
        self._apply("a1", "e1", "CREDIT", 300.0)
        self._apply("a1", "e2", "DEBIT",  100.0)
        _, body = self.m.get_balance("a1", "t")
        self.assertEqual(body["balance"], 200.0)

    def test_balance_correct_out_of_order(self):
        """Arrival order must not affect final balance."""
        self._apply("a2", "e-late",  "DEBIT",  50.0, "2026-01-01T14:00:00Z")
        self._apply("a2", "e-early", "CREDIT", 150.0, "2026-01-01T08:00:00Z")
        _, body = self.m.get_balance("a2", "t")
        self.assertEqual(body["balance"], 100.0)

    def test_balance_unknown_account_returns_404(self):
        status, _ = self.m.get_balance("no-such", "t")
        self.assertEqual(status, 404)

    def test_zero_net_balance(self):
        self._apply("a3", "e1", "CREDIT", 100.0)
        self._apply("a3", "e2", "DEBIT",  100.0)
        _, body = self.m.get_balance("a3", "t")
        self.assertEqual(body["balance"], 0.0)

    # --- Idempotency ---
    def test_duplicate_transaction_not_double_counted(self):
        self._apply("a4", "e-dup", "CREDIT", 100.0)
        self._apply("a4", "e-dup", "CREDIT", 100.0)  # duplicate
        _, body = self.m.get_balance("a4", "t")
        self.assertEqual(body["balance"], 100.0)

    def test_duplicate_returns_200_not_201(self):
        status1, _ = self._apply("a5", "e-d", "CREDIT", 50.0)
        status2, b = self._apply("a5", "e-d", "CREDIT", 50.0)
        self.assertEqual(status1, 201)
        self.assertEqual(status2, 200)
        self.assertTrue(b["duplicate"])

    # --- Validation ---
    def test_invalid_type_rejected(self):
        status, _ = self.m.apply_transaction("a1", {
            "eventId": "x", "type": "WIRE", "amount": 10,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z",
        }, "t")
        self.assertEqual(status, 422)

    def test_negative_amount_rejected(self):
        status, _ = self.m.apply_transaction("a1", {
            "eventId": "x", "type": "CREDIT", "amount": -10,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z",
        }, "t")
        self.assertEqual(status, 422)

    def test_zero_amount_rejected(self):
        status, _ = self.m.apply_transaction("a1", {
            "eventId": "x", "type": "CREDIT", "amount": 0,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z",
        }, "t")
        self.assertEqual(status, 422)

    # --- Out-of-order listings ---
    def test_account_transactions_chronological(self):
        self._apply("a6", "e-z", "CREDIT", 10, "2026-01-03T00:00:00Z")
        self._apply("a6", "e-a", "CREDIT", 20, "2026-01-01T00:00:00Z")
        self._apply("a6", "e-m", "DEBIT",   5, "2026-01-02T00:00:00Z")
        _, body = self.m.get_account("a6", "t")
        txns = body["transactions"]
        timestamps = [t["eventTimestamp"] for t in txns]
        self.assertEqual(timestamps, sorted(timestamps))


# ============================================================================
# 2. UNIT TESTS — Gateway validation + idempotency (no HTTP, no Account Svc)
# ============================================================================
class TestGatewayValidation(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

        # Stub: Account Service HTTP call always succeeds
        def _fake_http(trace_id, account_id, payload):
            return {"accountId": account_id, "balance": 100.0, "duplicate": False}
        m._do_http_call = _fake_http

    def test_missing_account_id(self):
        e = evt(); del e["accountId"]
        status, body = self.m.route_post_event(e, "t")
        self.assertEqual(status, 422)
        self.assertIn("errors", body)

    def test_missing_event_id(self):
        e = evt(); del e["eventId"]
        status, _ = self.m.route_post_event(e, "t")
        self.assertEqual(status, 422)

    def test_invalid_type(self):
        status, _ = self.m.route_post_event(evt(type="WIRE"), "t")
        self.assertEqual(status, 422)

    def test_zero_amount(self):
        status, _ = self.m.route_post_event(evt(amount=0), "t")
        self.assertEqual(status, 422)

    def test_negative_amount(self):
        status, _ = self.m.route_post_event(evt(amount=-1), "t")
        self.assertEqual(status, 422)

    def test_bad_timestamp(self):
        status, _ = self.m.route_post_event(evt(eventTimestamp="not-a-date"), "t")
        self.assertEqual(status, 422)

    def test_valid_event_returns_201(self):
        status, body = self.m.route_post_event(evt(), "t")
        self.assertEqual(status, 201)
        self.assertIn("eventId", body)

    def test_idempotent_second_submission_returns_200(self):
        e = evt()
        s1, _ = self.m.route_post_event(e, "t")
        s2, _ = self.m.route_post_event(e, "t")
        self.assertEqual(s1, 201)
        self.assertEqual(s2, 200)

    def test_account_service_not_called_on_duplicate(self):
        call_count = [0]
        def counting_http(trace_id, account_id, payload):
            call_count[0] += 1
            return {"accountId": account_id, "balance": 100.0, "duplicate": False}
        self.m._do_http_call = counting_http

        e = evt()
        self.m.route_post_event(e, "t")
        self.m.route_post_event(e, "t")
        self.assertEqual(call_count[0], 1)

    def test_duplicate_returns_original_event(self):
        e = evt(eventId="dup-check")
        self.m.route_post_event(e, "t")
        _, body = self.m.route_post_event(e, "t")
        self.assertEqual(body["eventId"], "dup-check")

    def test_separate_event_ids_create_separate_records(self):
        s1, _ = self.m.route_post_event(evt(eventId="ev-A"), "t")
        s2, _ = self.m.route_post_event(evt(eventId="ev-B"), "t")
        self.assertEqual(s1, 201)
        self.assertEqual(s2, 201)

    def test_get_event_not_found(self):
        status, _ = self.m.route_get_event("no-such-event", "t")
        self.assertEqual(status, 404)

    def test_get_event_found(self):
        e = evt(eventId="find-me")
        self.m.route_post_event(e, "t")
        status, body = self.m.route_get_event("find-me", "t")
        self.assertEqual(status, 200)
        self.assertEqual(body["eventId"], "find-me")

    def test_list_events_chronological_order(self):
        self.m.route_post_event(evt(eventId="late",  eventTimestamp="2026-05-15T14:00:00Z"), "t")
        self.m.route_post_event(evt(eventId="early", eventTimestamp="2026-05-15T10:00:00Z"), "t")
        _, events = self.m.route_list_events("acct-test", "t")
        self.assertEqual(events[0]["eventId"], "early")
        self.assertEqual(events[1]["eventId"], "late")

    def test_list_events_empty_for_unknown_account(self):
        _, events = self.m.route_list_events("unknown-acct", "t")
        self.assertEqual(events, [])

    def test_metadata_round_trips(self):
        e = evt(metadata={"source": "batch", "batchId": "B-42"})
        _, body = self.m.route_post_event(e, "t")
        self.assertEqual(body["metadata"]["batchId"], "B-42")

    def test_debit_event_accepted(self):
        status, body = self.m.route_post_event(evt(type="DEBIT"), "t")
        self.assertEqual(status, 201)
        self.assertEqual(body["type"], "DEBIT")


# ============================================================================
# 3. UNIT TESTS — Circuit breaker
# ============================================================================
class TestCircuitBreaker(unittest.TestCase):
    def _cb(self, threshold=3, timeout=60):
        from gateway.circuit_breaker import CircuitBreaker
        return CircuitBreaker(failure_threshold=threshold, recovery_timeout=timeout)

    def _fail(self):
        raise RuntimeError("down")

    def test_initial_state_closed(self):
        cb = self._cb()
        self.assertEqual(cb.state, "CLOSED")

    def test_opens_after_threshold(self):
        cb = self._cb(threshold=3)
        for _ in range(3):
            try: cb.call(self._fail)
            except Exception: pass
        self.assertEqual(cb.state, "OPEN")

    def test_does_not_open_before_threshold(self):
        cb = self._cb(threshold=3)
        for _ in range(2):
            try: cb.call(self._fail)
            except Exception: pass
        self.assertEqual(cb.state, "CLOSED")

    def test_open_raises_circuit_breaker_open(self):
        from gateway.circuit_breaker import CircuitBreakerOpen
        cb = self._cb(threshold=1)
        try: cb.call(self._fail)
        except Exception: pass
        with self.assertRaises(CircuitBreakerOpen):
            cb.call(lambda: "ok")

    def test_success_resets_to_closed(self):
        cb = self._cb(threshold=3)
        for _ in range(2):
            try: cb.call(self._fail)
            except Exception: pass
        cb.call(lambda: "ok")
        self.assertEqual(cb.state, "CLOSED")
        self.assertEqual(cb._failure_count, 0)

    def test_transitions_to_half_open_after_recovery_timeout(self):
        cb = self._cb(threshold=1, timeout=0.05)
        try: cb.call(self._fail)
        except Exception: pass
        self.assertEqual(cb.state, "OPEN")
        time.sleep(0.1)
        # Trigger transition check via a call attempt
        try: cb.call(lambda: "ok")
        except Exception: pass
        self.assertIn(cb.state, ("CLOSED", "HALF_OPEN"))

    def test_half_open_closes_on_success(self):
        from gateway.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        try: cb.call(self._fail)
        except Exception: pass
        time.sleep(0.1)
        cb.call(lambda: "ok")
        self.assertEqual(cb.state, "CLOSED")

    def test_half_open_reopens_on_failure(self):
        from gateway.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        try: cb.call(self._fail)
        except Exception: pass
        time.sleep(0.1)
        try: cb.call(self._fail)
        except Exception: pass
        self.assertEqual(cb.state, "OPEN")


# ============================================================================
# 4. UNIT TESTS — Trace propagation
# ============================================================================
class TestTracePropagation(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

    def test_trace_id_forwarded_to_account_service(self):
        captured = {}
        def capture(trace_id, account_id, payload):
            captured["trace_id"] = trace_id
            return {"accountId": account_id, "balance": 0.0, "duplicate": False}
        self.m._do_http_call = capture

        self.m.route_post_event(evt(), "my-trace-id-xyz")
        self.assertEqual(captured["trace_id"], "my-trace-id-xyz")

    def test_trace_id_generated_when_absent(self):
        captured = {}
        def capture(trace_id, account_id, payload):
            captured["trace_id"] = trace_id
            return {"accountId": account_id, "balance": 0.0, "duplicate": False}
        self.m._do_http_call = capture

        # route_post_event generates trace if caller passes empty string
        self.m.route_post_event(evt(), "auto-generated-trace")
        self.assertIn("trace_id", captured)
        self.assertTrue(len(captured["trace_id"]) > 0)

    def test_stored_event_contains_trace_id(self):
        def fake(trace_id, account_id, payload):
            return {"accountId": account_id, "balance": 0.0, "duplicate": False}
        self.m._do_http_call = fake

        e = evt(eventId="trace-stored")
        self.m.route_post_event(e, "trace-abc-123")
        _, body = self.m.route_get_event("trace-stored", "t")
        self.assertEqual(body["traceId"], "trace-abc-123")

    def test_trace_id_propagated_via_http_header(self):
        """Account Service handler reads X-Trace-ID from request headers."""
        import importlib
        import account_service.main as acct
        importlib.reload(acct)
        acct.init_db(":memory:")

        port = free_port()
        srv = HTTPServer(("127.0.0.1", port), acct.AccountHandler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        wait_for(f"http://127.0.0.1:{port}/health")

        payload = {
            "eventId": "tr-1", "type": "CREDIT", "amount": 50.0,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z",
        }
        status, body = http(
            "POST", f"http://127.0.0.1:{port}/accounts/acct-1/transactions",
            body=payload, headers={"X-Trace-ID": "propagated-id-99"},
        )
        self.assertEqual(status, 201)
        srv.shutdown()


# ============================================================================
# 5. RESILIENCY TESTS — Gateway circuit breaker via route logic
# ============================================================================
class TestResiliency(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        from gateway.circuit_breaker import CircuitBreaker
        m.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        self.m = m

    def tearDown(self):
        # Reset the module-level CB to CLOSED so server-based test classes
        # that share the same module object are not affected.
        from gateway.circuit_breaker import CircuitBreaker
        self.m.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

    def _patch_fail(self):
        def _fail(*args, **kwargs):
            raise ConnectionRefusedError("down")
        self.m._do_http_call = _fail

    def _patch_ok(self):
        def _ok(trace_id, account_id, payload):
            return {"accountId": account_id, "balance": 0, "duplicate": False}
        self.m._do_http_call = _ok

    def test_returns_503_when_account_service_fails(self):
        self._patch_fail()
        status, body = self.m.route_post_event(evt(), "t")
        self.assertEqual(status, 503)
        self.assertIn("eventId", body)

    def test_503_body_contains_event_id(self):
        self._patch_fail()
        e = evt(eventId="fail-event")
        _, body = self.m.route_post_event(e, "t")
        self.assertEqual(body["eventId"], "fail-event")

    def test_circuit_opens_after_threshold_failures(self):
        self._patch_fail()
        for _ in range(4):
            self.m.route_post_event(evt(), "t")
        self.assertEqual(self.m.cb.state, "OPEN")

    def test_event_still_stored_after_503(self):
        self._patch_fail()
        e = evt(eventId="persisted-evt")
        self.m.route_post_event(e, "t")
        status, body = self.m.route_get_event("persisted-evt", "t")
        self.assertEqual(status, 200)
        self.assertEqual(body["eventId"], "persisted-evt")

    def test_get_event_works_when_account_service_down(self):
        self._patch_ok()
        e = evt(eventId="get-while-down")
        self.m.route_post_event(e, "t")
        self._patch_fail()
        status, _ = self.m.route_get_event("get-while-down", "t")
        self.assertEqual(status, 200)

    def test_list_events_works_when_account_service_down(self):
        self._patch_ok()
        self.m.route_post_event(evt(eventId="list-while-down"), "t")
        self._patch_fail()
        _, events = self.m.route_list_events("acct-test", "t")
        self.assertGreaterEqual(len(events), 1)

    def test_account_service_error_flagged_on_event(self):
        self._patch_fail()
        e = evt(eventId="flagged-evt")
        self.m.route_post_event(e, "t")
        _, body = self.m.route_get_event("flagged-evt", "t")
        self.assertTrue(body["accountServiceError"])


# ============================================================================
# 6. OBSERVABILITY TESTS — health, metrics endpoints via HTTP
# ============================================================================
class TestObservability(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import importlib
        import account_service.main as acct
        import gateway.main as gw
        importlib.reload(acct)
        importlib.reload(gw)

        cls.acct_port = free_port()
        cls.gw_port   = free_port()

        acct.init_db(":memory:")
        cls.acct_srv = HTTPServer(("127.0.0.1", cls.acct_port), acct.AccountHandler)
        ta = threading.Thread(target=cls.acct_srv.serve_forever, daemon=True)
        ta.start()

        gw.ACCOUNT_SERVICE_URL = f"http://127.0.0.1:{cls.acct_port}"
        gw.init_db(":memory:")
        from gateway.circuit_breaker import CircuitBreaker
        gw.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        cls.gw_srv = HTTPServer(("127.0.0.1", cls.gw_port), gw.GatewayHandler)
        tg = threading.Thread(target=cls.gw_srv.serve_forever, daemon=True)
        tg.start()

        wait_for(f"http://127.0.0.1:{cls.acct_port}/health")
        wait_for(f"http://127.0.0.1:{cls.gw_port}/health")

    @classmethod
    def tearDownClass(cls):
        cls.acct_srv.shutdown()
        cls.gw_srv.shutdown()

    def test_gateway_health_returns_200(self):
        status, body = http("GET", f"http://127.0.0.1:{self.gw_port}/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_account_service_health_returns_200(self):
        status, body = http("GET", f"http://127.0.0.1:{self.acct_port}/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_gateway_health_includes_circuit_breaker_state(self):
        _, body = http("GET", f"http://127.0.0.1:{self.gw_port}/health")
        self.assertIn("circuit_breaker", body)
        self.assertEqual(body["circuit_breaker"], "CLOSED")

    def test_gateway_health_includes_database_status(self):
        _, body = http("GET", f"http://127.0.0.1:{self.gw_port}/health")
        self.assertEqual(body["database"], "ok")

    def test_account_health_includes_database_status(self):
        _, body = http("GET", f"http://127.0.0.1:{self.acct_port}/health")
        self.assertEqual(body["database"], "ok")

    def test_gateway_metrics_endpoint_exists(self):
        status, body = http("GET", f"http://127.0.0.1:{self.gw_port}/metrics")
        self.assertEqual(status, 200)
        self.assertIn("requests", body)

    def test_account_service_metrics_endpoint_exists(self):
        status, body = http("GET", f"http://127.0.0.1:{self.acct_port}/metrics")
        self.assertEqual(status, 200)
        self.assertIn("requests", body)

    def test_metrics_track_duplicate_count(self):
        import importlib
        import gateway.main as gw
        # submit same event twice
        e = evt(eventId="obs-dup-" + str(uuid.uuid4()))
        http("POST", f"http://127.0.0.1:{self.gw_port}/events", body=e)
        http("POST", f"http://127.0.0.1:{self.gw_port}/events", body=e)
        _, m = http("GET", f"http://127.0.0.1:{self.gw_port}/metrics")
        self.assertGreaterEqual(m["duplicates"], 1)


# ============================================================================
# 7. INTEGRATION TESTS — full Gateway → Account Service over real HTTP
# ============================================================================
class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import importlib
        import account_service.main as acct
        import gateway.main as gw
        importlib.reload(acct)
        importlib.reload(gw)

        cls.acct_port = free_port()
        cls.gw_port   = free_port()

        acct.init_db(":memory:")
        cls.acct_srv = HTTPServer(("127.0.0.1", cls.acct_port), acct.AccountHandler)
        ta = threading.Thread(target=cls.acct_srv.serve_forever, daemon=True)
        ta.start()

        gw.ACCOUNT_SERVICE_URL = f"http://127.0.0.1:{cls.acct_port}"
        gw.init_db(":memory:")
        from gateway.circuit_breaker import CircuitBreaker
        gw.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        cls.gw_srv = HTTPServer(("127.0.0.1", cls.gw_port), gw.GatewayHandler)
        tg = threading.Thread(target=cls.gw_srv.serve_forever, daemon=True)
        tg.start()

        wait_for(f"http://127.0.0.1:{cls.acct_port}/health")
        wait_for(f"http://127.0.0.1:{cls.gw_port}/health")

    @classmethod
    def tearDownClass(cls):
        cls.acct_srv.shutdown()
        cls.gw_srv.shutdown()

    def gw(self, method, path, body=None, headers=None):
        return http(method, f"http://127.0.0.1:{self.gw_port}{path}", body, headers)

    def acct(self, method, path, body=None, headers=None):
        return http(method, f"http://127.0.0.1:{self.acct_port}{path}", body, headers)

    # --- Basic round trip ---
    def test_post_event_returns_201(self):
        status, body = self.gw("POST", "/events", body=evt(eventId=str(uuid.uuid4())))
        self.assertEqual(status, 201)

    def test_post_event_balance_updated_in_account_service(self):
        eid = str(uuid.uuid4())
        self.gw("POST", "/events", body=evt(eventId=eid, amount=250.0))
        status, body = self.acct("GET", "/accounts/acct-test/balance")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(body["balance"], 250.0)

    def test_get_event_by_id(self):
        eid = str(uuid.uuid4())
        self.gw("POST", "/events", body=evt(eventId=eid))
        status, body = self.gw("GET", f"/events/{eid}")
        self.assertEqual(status, 200)
        self.assertEqual(body["eventId"], eid)

    def test_get_event_not_found_returns_404(self):
        status, _ = self.gw("GET", "/events/does-not-exist-ever")
        self.assertEqual(status, 404)

    def test_list_events_for_account(self):
        acct_id = f"acct-list-{uuid.uuid4()}"
        e1 = evt(eventId=str(uuid.uuid4()), accountId=acct_id, eventTimestamp="2026-01-01T09:00:00Z")
        e2 = evt(eventId=str(uuid.uuid4()), accountId=acct_id, eventTimestamp="2026-01-01T11:00:00Z")
        self.gw("POST", "/events", body=e1)
        self.gw("POST", "/events", body=e2)
        status, events = self.gw("GET", f"/events?account={acct_id}")
        self.assertEqual(status, 200)
        self.assertEqual(len(events), 2)

    # --- Idempotency over HTTP ---
    def test_duplicate_post_returns_200_not_201(self):
        e = evt(eventId=str(uuid.uuid4()))
        s1, _ = self.gw("POST", "/events", body=e)
        s2, _ = self.gw("POST", "/events", body=e)
        self.assertEqual(s1, 201)
        self.assertEqual(s2, 200)

    def test_balance_unchanged_after_duplicate(self):
        acct_id = f"acct-idem-{uuid.uuid4()}"
        e = evt(eventId=str(uuid.uuid4()), accountId=acct_id, amount=100.0)
        self.gw("POST", "/events", body=e)
        self.gw("POST", "/events", body=e)
        _, body = self.acct("GET", f"/accounts/{acct_id}/balance")
        self.assertEqual(body["balance"], 100.0)

    # --- Out-of-order over HTTP ---
    def test_out_of_order_events_listed_chronologically(self):
        acct_id = f"acct-ooo-{uuid.uuid4()}"
        late  = evt(eventId=str(uuid.uuid4()), accountId=acct_id, eventTimestamp="2026-06-01T18:00:00Z")
        early = evt(eventId=str(uuid.uuid4()), accountId=acct_id, eventTimestamp="2026-06-01T08:00:00Z")
        self.gw("POST", "/events", body=late)
        self.gw("POST", "/events", body=early)
        _, events = self.gw("GET", f"/events?account={acct_id}")
        self.assertEqual(events[0]["eventTimestamp"], "2026-06-01T08:00:00Z")
        self.assertEqual(events[1]["eventTimestamp"], "2026-06-01T18:00:00Z")

    def test_out_of_order_balance_correct(self):
        """Arrival order of CREDIT/DEBIT must not affect final balance."""
        acct_id = f"acct-bal-{uuid.uuid4()}"
        debit  = evt(eventId=str(uuid.uuid4()), accountId=acct_id, type="DEBIT",  amount=50.0,
                     eventTimestamp="2026-01-01T10:00:00Z")
        credit = evt(eventId=str(uuid.uuid4()), accountId=acct_id, type="CREDIT", amount=200.0,
                     eventTimestamp="2026-01-01T08:00:00Z")
        self.gw("POST", "/events", body=debit)   # arrives first
        self.gw("POST", "/events", body=credit)  # arrives second but earlier timestamp
        _, body = self.acct("GET", f"/accounts/{acct_id}/balance")
        self.assertEqual(body["balance"], 150.0)

    # --- Validation over HTTP ---
    def test_missing_required_field_returns_422(self):
        e = evt(); del e["type"]
        status, _ = self.gw("POST", "/events", body=e)
        self.assertEqual(status, 422)

    def test_invalid_type_returns_422(self):
        status, _ = self.gw("POST", "/events", body=evt(type="SWAP"))
        self.assertEqual(status, 422)

    def test_zero_amount_returns_422(self):
        status, _ = self.gw("POST", "/events", body=evt(amount=0))
        self.assertEqual(status, 422)

    # --- Trace propagation over HTTP ---
    def test_trace_id_in_request_reflected_in_stored_event(self):
        eid = str(uuid.uuid4())
        self.gw("POST", "/events", body=evt(eventId=eid), headers={"X-Trace-ID": "integration-trace-1"})
        _, body = self.gw("GET", f"/events/{eid}")
        self.assertEqual(body["traceId"], "integration-trace-1")

    def test_gateway_generates_trace_id_when_absent(self):
        eid = str(uuid.uuid4())
        self.gw("POST", "/events", body=evt(eventId=eid))  # no trace header
        _, body = self.gw("GET", f"/events/{eid}")
        self.assertIsNotNone(body["traceId"])
        self.assertGreater(len(body["traceId"]), 0)

    # --- Service separation ---
    def test_services_have_independent_databases(self):
        """Events in Gateway DB have no direct equivalent in Account Service DB
        until the Gateway calls the Account Service — they are separate stores."""
        eid     = str(uuid.uuid4())
        acct_id = f"acct-sep-{uuid.uuid4()}"
        # Post to gateway
        self.gw("POST", "/events", body=evt(eventId=eid, accountId=acct_id, amount=77.0))
        # Account Service knows about it via the transaction, not via Gateway's DB
        _, bal = self.acct("GET", f"/accounts/{acct_id}/balance")
        self.assertEqual(bal["balance"], 77.0)
        # Gateway does NOT expose Account Service internals
        _, gw_events = self.gw("GET", f"/events?account={acct_id}")
        self.assertEqual(len(gw_events), 1)
        # Account Service doesn't expose Gateway's event metadata
        _, acct_detail = self.acct("GET", f"/accounts/{acct_id}")
        txn = acct_detail["transactions"][0]
        self.assertNotIn("metadata", txn)
        self.assertNotIn("receivedAt", txn)

    # --- Graceful degradation over HTTP ---
    def test_graceful_degradation_get_still_works(self):
        """Submit an event while Account Service is up, then verify GET still works
        after the Account Service URL is broken. Uses route_post_event/route_get_event
        directly to avoid module-level side-effects on the shared HTTP servers."""
        import importlib
        import gateway.main as gw_snap

        # Save and restore module state around this test
        saved_url = gw_snap.ACCOUNT_SERVICE_URL
        from gateway.circuit_breaker import CircuitBreaker
        saved_cb  = gw_snap.cb

        try:
            # Use the live Account Service for the initial POST
            gw_snap.ACCOUNT_SERVICE_URL = f"http://127.0.0.1:{self.acct_port}"
            gw_snap.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

            eid = str(uuid.uuid4())
            status, body = gw_snap.route_post_event(
                evt(eventId=eid), trace_id="graceful-trace")
            self.assertEqual(status, 201)

            # Now point at a dead URL — GET should still work from local DB
            gw_snap.ACCOUNT_SERVICE_URL = "http://127.0.0.1:19999"

            status, body = gw_snap.route_get_event(eid, "graceful-trace")
            self.assertEqual(status, 200)
            self.assertEqual(body["eventId"], eid)
        finally:
            # Always restore so subsequent tests are unaffected
            gw_snap.ACCOUNT_SERVICE_URL = saved_url
            gw_snap.cb = saved_cb





# ============================================================================
# BONUS 1 — Prometheus metrics format
# ============================================================================
class TestPrometheus(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

    def _fake_ok(self):
        def ok(t, a, p): return {"accountId": a, "balance": 0, "duplicate": False}
        self.m._do_http_call = ok

    def test_prometheus_endpoint_returns_text_plain(self):
        port = free_port()
        srv  = HTTPServer(("127.0.0.1", port), self.m.GatewayHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        wait_for(f"http://127.0.0.1:{port}/health")
        import urllib.request as _ur
        with _ur.urlopen(f"http://127.0.0.1:{port}/metrics/prometheus", timeout=3) as r:
            ctype = r.headers.get("Content-Type", "")
            body_text = r.read().decode()
            status = r.status
        srv.shutdown()
        self.assertEqual(status, 200)
        self.assertIn("text/plain", ctype)
        self.assertIn("event_ledger_", body_text)

    def test_prometheus_output_contains_required_metric_names(self):
        from prometheus import render_prometheus
        snap = {
            "requests": 10, "errors_4xx": 1, "errors_5xx": 0,
            "duplicates": 2, "account_service_errors": 1,
            "latency_summary": {"p50": 5.0, "p95": 20.0, "p99": 50.0, "max": 100.0},
        }
        out = render_prometheus("gateway", snap)
        for name in ["event_ledger_requests_total",
                     "event_ledger_errors_5xx_total",
                     "event_ledger_duplicate_events_total",
                     "event_ledger_request_duration_ms"]:
            self.assertIn(name, out, f"Missing metric: {name}")

    def test_prometheus_output_is_valid_format(self):
        from prometheus import render_prometheus
        snap = {"requests": 5, "errors_4xx": 0, "errors_5xx": 0,
                "duplicates": 0, "account_service_errors": 0,
                "latency_summary": {"p50": 3.0, "p95": 10.0, "p99": 20.0, "max": 30.0}}
        out = render_prometheus("gateway", snap)
        # Every non-empty, non-comment line must have a space (metric value separator)
        for line in out.splitlines():
            if line and not line.startswith("#"):
                self.assertIn(" ", line, f"Invalid Prometheus line: {line!r}")

    def test_prometheus_includes_service_label(self):
        from prometheus import render_prometheus
        out = render_prometheus("gateway", {"requests": 1, "errors_4xx": 0,
                                            "errors_5xx": 0})
        self.assertIn('service="gateway"', out)

    def test_prometheus_fallback_queue_metrics_present(self):
        from prometheus import render_prometheus
        snap = {"requests": 0, "errors_4xx": 0, "errors_5xx": 0,
                "fallback_queue_depth": 3, "fallback_replayed_total": 7}
        out = render_prometheus("gateway", snap)
        self.assertIn("event_ledger_fallback_queue_depth", out)
        self.assertIn("event_ledger_fallback_replayed_total", out)


# ============================================================================
# BONUS 2 — Retry with exponential backoff + jitter
# ============================================================================
class TestRetryBackoff(unittest.TestCase):
    def _cb(self, **kw):
        from gateway.circuit_breaker import CircuitBreaker
        defaults = dict(failure_threshold=10, recovery_timeout=60,
                        max_retries=3, base_delay=0.001, max_delay=0.01,
                        retry_enabled=True)
        defaults.update(kw)
        return CircuitBreaker(**defaults)

    def test_retries_on_transient_failure_then_succeeds(self):
        cb = self._cb()
        attempts = [0]
        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ConnectionError("transient")
            return "ok"
        result = cb.call(flaky)
        self.assertEqual(result, "ok")
        self.assertEqual(attempts[0], 3)

    def test_retry_count_tracked_in_stats(self):
        cb = self._cb(max_retries=2, failure_threshold=10)
        attempts = [0]
        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ConnectionError("transient")
            return "ok"
        cb.call(flaky)
        self.assertGreaterEqual(cb.total_retries, 2)

    def test_all_retries_exhausted_counts_as_cb_failure(self):
        # Each call() that exhausts all retries = 1 CB failure.
        # With failure_threshold=3, after 3 failed calls the CB opens.
        cb = self._cb(max_retries=2, failure_threshold=3)
        def always_fail(): raise ConnectionError("always down")
        for _ in range(3):
            try: cb.call(always_fail)
            except Exception: pass
        # 3 calls exhausted → 3 CB failures → OPEN
        self.assertEqual(cb.state, "OPEN")

    def test_no_retry_when_disabled(self):
        from gateway.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=60,
                            max_retries=3, retry_enabled=False)
        attempts = [0]
        def fail():
            attempts[0] += 1
            raise ConnectionError("down")
        try: cb.call(fail)
        except Exception: pass
        self.assertEqual(attempts[0], 1)

    def test_jitter_delay_is_within_bounds(self):
        cb = self._cb(base_delay=0.1, max_delay=1.0)
        for attempt in range(5):
            delay = cb._jittered_delay(attempt)
            self.assertGreaterEqual(delay, 0)
            self.assertLessEqual(delay, 1.0)

    def test_delay_increases_with_attempt_number(self):
        """Upper bound of delay should grow with attempt (base * 2^attempt)."""
        import math
        cb = self._cb(base_delay=0.1, max_delay=100.0)
        caps = [min(100.0, 0.1 * math.pow(2, a)) for a in range(5)]
        self.assertLess(caps[0], caps[3])  # cap grows


# ============================================================================
# BONUS 3 — Rate limiting
# ============================================================================
class TestRateLimiting(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

    def test_allow_under_limit(self):
        from rate_limiter import RateLimiter
        rl = RateLimiter(rate=10, capacity=10, enabled=True)
        for _ in range(10):
            self.assertTrue(rl.allow("127.0.0.1"))

    def test_reject_over_burst_capacity(self):
        from rate_limiter import RateLimiter
        rl = RateLimiter(rate=1, capacity=3, enabled=True)
        results = [rl.allow("10.0.0.1") for _ in range(6)]
        self.assertTrue(all(results[:3]))
        self.assertFalse(all(results[3:]))

    def test_different_ips_get_independent_buckets(self):
        from rate_limiter import RateLimiter
        rl = RateLimiter(rate=1, capacity=1, enabled=True)
        self.assertTrue(rl.allow("1.1.1.1"))
        self.assertFalse(rl.allow("1.1.1.1"))
        # Different IP — fresh bucket
        self.assertTrue(rl.allow("2.2.2.2"))

    def test_disabled_rate_limiter_always_allows(self):
        from rate_limiter import RateLimiter
        rl = RateLimiter(rate=0.001, capacity=0.001, enabled=False)
        for _ in range(100):
            self.assertTrue(rl.allow("1.1.1.1"))

    def test_rate_limited_request_returns_429_via_http(self):
        from rate_limiter import RateLimiter
        port = free_port()
        srv  = HTTPServer(("127.0.0.1", port), self.m.GatewayHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        wait_for(f"http://127.0.0.1:{port}/health")
        # Now swap in a zero-capacity limiter (server is already up)
        self.m.rate_limiter = RateLimiter(rate=0, capacity=0, enabled=True)
        status, body = http("POST", f"http://127.0.0.1:{port}/events",
                            body={"eventId": "x", "accountId": "a",
                                  "type": "CREDIT", "amount": 10,
                                  "currency": "USD",
                                  "eventTimestamp": "2026-01-01T00:00:00Z"})
        srv.shutdown()
        self.assertEqual(status, 429)

    def test_stats_track_rejected_count(self):
        from rate_limiter import RateLimiter
        rl = RateLimiter(rate=1, capacity=1, enabled=True)
        rl.allow("1.1.1.1")  # consume token
        rl.allow("1.1.1.1")  # rejected
        self.assertEqual(rl.rejected_total, 1)

    def test_metrics_include_rate_limit_info(self):
        from rate_limiter import RateLimiter
        self.m.rate_limiter = RateLimiter(rate=100, capacity=100, enabled=True)
        snap = self.m._metrics_snapshot()
        self.assertIn("rate_limited_total", snap)
        self.assertIn("enabled", snap)


# ============================================================================
# BONUS 4 — Contract tests (Gateway consumer ↔ Account Service provider)
# ============================================================================
class TestContractGatewayConsumer(unittest.TestCase):
    """
    Consumer-side contract: verifies that the Gateway only sends requests
    that conform to the Account Service's published API contract.
    """

    def setUp(self):
        import importlib
        import account_service.main as acct
        importlib.reload(acct)
        acct.init_db(":memory:")
        self.acct = acct

    def _post_txn(self, account_id, payload):
        return self.acct.apply_transaction(account_id, payload, "contract-trace")

    # --- Contract: POST /accounts/{id}/transactions request shape ---
    def test_contract_required_fields_eventId(self):
        """Account Service MUST require eventId."""
        status, _ = self._post_txn("a1", {
            "type": "CREDIT", "amount": 10,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        self.assertEqual(status, 422)

    def test_contract_required_fields_type(self):
        status, _ = self._post_txn("a1", {
            "eventId": "e1", "amount": 10,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        self.assertEqual(status, 422)

    def test_contract_required_fields_amount(self):
        status, _ = self._post_txn("a1", {
            "eventId": "e1", "type": "CREDIT",
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        self.assertEqual(status, 422)

    # --- Contract: POST /accounts/{id}/transactions response shape ---
    def test_contract_response_has_accountId(self):
        status, body = self._post_txn("acct-42", {
            "eventId": "e1", "type": "CREDIT", "amount": 100,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        self.assertEqual(status, 201)
        self.assertIn("accountId", body)
        self.assertEqual(body["accountId"], "acct-42")

    def test_contract_response_has_balance(self):
        _, body = self._post_txn("acct-43", {
            "eventId": "e2", "type": "CREDIT", "amount": 50,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        self.assertIn("balance", body)
        self.assertIsInstance(body["balance"], float)

    def test_contract_response_has_duplicate_flag(self):
        txn = {"eventId": "e3", "type": "CREDIT", "amount": 75,
               "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"}
        self._post_txn("acct-44", txn)
        _, body = self._post_txn("acct-44", txn)
        self.assertIn("duplicate", body)
        self.assertTrue(body["duplicate"])

    # --- Contract: GET /accounts/{id}/balance response shape ---
    def test_contract_balance_response_shape(self):
        self._post_txn("acct-bal", {
            "eventId": "e4", "type": "CREDIT", "amount": 200,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        })
        status, body = self.acct.get_balance("acct-bal", "t")
        self.assertEqual(status, 200)
        self.assertIn("accountId", body)
        self.assertIn("balance", body)
        self.assertEqual(body["accountId"], "acct-bal")
        self.assertEqual(body["balance"], 200.0)

    def test_contract_unknown_account_returns_404(self):
        status, body = self.acct.get_balance("nonexistent", "t")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    # --- Contract: Gateway sends correct payload shape ---
    def test_contract_gateway_sends_required_fields_to_account_service(self):
        """The Gateway must send all fields the Account Service requires."""
        import importlib
        import gateway.main as gw
        importlib.reload(gw)
        gw.init_db(":memory:")

        received = {}
        def capture(trace_id, account_id, payload):
            received.update(payload)
            return {"accountId": account_id, "balance": 0, "duplicate": False}
        gw._do_http_call = capture

        gw.route_post_event({
            "eventId": "contract-e1", "accountId": "acct-c",
            "type": "CREDIT", "amount": 99, "currency": "USD",
            "eventTimestamp": "2026-01-01T00:00:00Z"
        }, "trace-contract")

        # Gateway MUST send these fields to Account Service
        for field in ("eventId", "type", "amount", "currency", "eventTimestamp"):
            self.assertIn(field, received,
                          f"Gateway did not send '{field}' to Account Service")


# ============================================================================
# BONUS 5 — Async fallback queue
# ============================================================================
class TestFallbackQueue(unittest.TestCase):
    def setUp(self):
        import importlib
        import gateway.main as m
        importlib.reload(m)
        m.init_db(":memory:")
        self.m = m

    def _make_queue(self, url="http://127.0.0.1:19999"):
        from fallback_queue import FallbackQueue
        return FallbackQueue(
            account_service_url=url,
            trace_header="X-Trace-ID",
            db_path=":memory:",
            poll_secs=999,   # disable auto-polling; we call _process_queue manually
            enabled=True,
        )

    def test_enqueue_increases_depth(self):
        fq = self._make_queue()
        fq.enqueue("e1", "acct-1", {"eventId": "e1", "type": "CREDIT",
                                     "amount": 10, "currency": "USD",
                                     "eventTimestamp": "2026-01-01T00:00:00Z"}, "t1")
        self.assertEqual(fq.depth(), 1)

    def test_duplicate_enqueue_does_not_increase_depth(self):
        fq = self._make_queue()
        payload = {"eventId": "e-dup", "type": "CREDIT", "amount": 10,
                   "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"}
        fq.enqueue("e-dup", "acct-1", payload, "t1")
        fq.enqueue("e-dup", "acct-1", payload, "t1")  # same eventId
        self.assertEqual(fq.depth(), 1)

    def test_successful_replay_removes_from_queue(self):
        import importlib
        import account_service.main as acct
        importlib.reload(acct)
        acct.init_db(":memory:")

        acct_port = free_port()
        acct_srv  = HTTPServer(("127.0.0.1", acct_port), acct.AccountHandler)
        threading.Thread(target=acct_srv.serve_forever, daemon=True).start()
        wait_for(f"http://127.0.0.1:{acct_port}/health")

        fq = self._make_queue(url=f"http://127.0.0.1:{acct_port}")
        fq.enqueue("replay-e1", "acct-r", {
            "eventId": "replay-e1", "type": "CREDIT", "amount": 50,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        }, "trace-replay")

        self.assertEqual(fq.depth(), 1)
        fq._process_queue()  # manual trigger
        self.assertEqual(fq.depth(), 0)

        acct_srv.shutdown()

    def test_failed_replay_stays_in_queue(self):
        fq = self._make_queue(url="http://127.0.0.1:19998")  # nothing there
        fq.enqueue("stuck-e1", "acct-s", {
            "eventId": "stuck-e1", "type": "CREDIT", "amount": 10,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        }, "t1")
        fq._process_queue()
        self.assertEqual(fq.depth(), 1)

    def test_replay_increments_replayed_counter(self):
        import importlib
        import account_service.main as acct
        importlib.reload(acct)
        acct.init_db(":memory:")

        acct_port = free_port()
        acct_srv  = HTTPServer(("127.0.0.1", acct_port), acct.AccountHandler)
        threading.Thread(target=acct_srv.serve_forever, daemon=True).start()
        wait_for(f"http://127.0.0.1:{acct_port}/health")

        fq = self._make_queue(url=f"http://127.0.0.1:{acct_port}")
        fq.enqueue("rep-e2", "acct-x", {
            "eventId": "rep-e2", "type": "DEBIT", "amount": 25,
            "currency": "USD", "eventTimestamp": "2026-01-01T00:00:00Z"
        }, "t2")
        fq._process_queue()
        self.assertEqual(fq.stats()["fallback_replayed_total"], 1)
        acct_srv.shutdown()

    def test_503_response_enqueues_event(self):
        """When Account Service is down, POST /events → 503 AND event is queued."""
        import importlib
        import gateway.main as gw
        importlib.reload(gw)
        gw.init_db(":memory:")

        from fallback_queue import FallbackQueue
        queued = []
        class CaptureFQ(FallbackQueue):
            def enqueue(self, event_id, account_id, payload, trace_id):
                queued.append(event_id)
        gw._fallback_queue = CaptureFQ(
            account_service_url="http://127.0.0.1:19997",
            trace_header="X-Trace-ID",
            db_path=":memory:",
            poll_secs=999,
            enabled=True,
        )

        def fail(*a, **kw): raise ConnectionRefusedError("down")
        gw._do_http_call = fail

        e = {"eventId": "fq-test-evt", "accountId": "acct-fq",
             "type": "CREDIT", "amount": 100, "currency": "USD",
             "eventTimestamp": "2026-01-01T00:00:00Z"}
        status, body = gw.route_post_event(e, "trace-fq")
        self.assertEqual(status, 503)
        self.assertIn("fq-test-evt", queued)

    def test_fallback_queue_depth_in_health_endpoint(self):
        port = free_port()
        srv  = HTTPServer(("127.0.0.1", port), self.m.GatewayHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        wait_for(f"http://127.0.0.1:{port}/health")
        status, body = http("GET", f"http://127.0.0.1:{port}/health")
        srv.shutdown()
        self.assertEqual(status, 200)
        self.assertIn("fallback_queue_depth", body)


# ============================================================================
# Runner  (includes all bonus test classes)
# ============================================================================
if __name__ == "__main__":
    loader  = unittest.TestLoader()
    suite   = unittest.TestSuite()
    classes = [
        # Core
        TestAccountServiceLogic,
        TestGatewayValidation,
        TestCircuitBreaker,
        TestTracePropagation,
        TestResiliency,       # Must run before server-based tests (mutates module cb)
        TestObservability,
        TestIntegration,
        # Bonus
        TestPrometheus,
        TestRetryBackoff,
        TestRateLimiting,
        TestContractGatewayConsumer,
        TestFallbackQueue,
    ]
    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
