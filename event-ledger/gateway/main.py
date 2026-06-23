"""
Event Gateway — public-facing API (port 8000).
Zero external dependencies — stdlib only.

Bonus features included:
  - Prometheus text exposition at GET /metrics/prometheus
  - Token-bucket rate limiting (per IP, configurable via env vars)
  - Retry with exponential backoff + jitter (in circuit breaker)
  - Async fallback queue (events retried when Account Service recovers)
  - W3C traceparent-compatible trace IDs (16-byte hex)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import json
import logging
import re
import sqlite3
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from gateway.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from shared import TRACE_HEADER, new_trace_id
from rate_limiter import RateLimiter
from fallback_queue import FallbackQueue
from prometheus import render_prometheus

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

def _log(trace_id, level, msg):
    entry = json.dumps({
        "time":     datetime.now(timezone.utc).isoformat(),
        "level":    level,
        "service":  "gateway",
        "trace_id": trace_id,
        "message":  msg,
    })
    getattr(logger, level)(entry)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ACCOUNT_SERVICE_URL = os.environ.get("ACCOUNT_SERVICE_URL", "http://localhost:8001")
REQUEST_TIMEOUT     = float(os.environ.get("ACCOUNT_TIMEOUT", "5"))

# Shared circuit breaker (with retry + backoff)
cb = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=15.0,
    half_open_max_calls=1,
    max_retries=2,
    base_delay=0.1,
    max_delay=2.0,
)

# Rate limiter (20 RPS per IP, burst 40)
rate_limiter = RateLimiter()

# Fallback queue (started lazily after init_db so we know the URL)
_fallback_queue: FallbackQueue | None = None

def get_fallback_queue() -> FallbackQueue:
    return _fallback_queue

# ---------------------------------------------------------------------------
# Database — single shared connection
# ---------------------------------------------------------------------------
_db_conn = None
_db_lock = threading.Lock()

def init_db(path=":memory:"):
    global _db_conn, _fallback_queue
    with _db_lock:
        _db_conn = sqlite3.connect(path, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
        _db_conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id              TEXT PRIMARY KEY,
                account_id            TEXT NOT NULL,
                type                  TEXT NOT NULL,
                amount                REAL NOT NULL,
                currency              TEXT NOT NULL,
                event_timestamp       TEXT NOT NULL,
                metadata              TEXT,
                received_at           TEXT NOT NULL,
                trace_id              TEXT,
                account_service_error INTEGER DEFAULT 0
            )
        """)
        _db_conn.commit()
    # Initialise fallback queue (uses its own DB file)
    fq_path = path if path == ":memory:" else os.path.join(
        os.path.dirname(path), "fallback_queue.db"
    )
    _fallback_queue = FallbackQueue(
        account_service_url=ACCOUNT_SERVICE_URL,
        trace_header=TRACE_HEADER,
        db_path=fq_path,
    )

def get_db():
    return _db_conn

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
_metrics = {
    "requests": 0, "errors_4xx": 0, "errors_5xx": 0,
    "duplicates": 0, "account_service_errors": 0,
    "latency_ms": [],
    "started_at": datetime.now(timezone.utc).isoformat(),
}
_metrics_lock = threading.Lock()

def _record_req(status, latency_ms):
    with _metrics_lock:
        _metrics["requests"] += 1
        _metrics["latency_ms"].append(latency_ms)
        if 400 <= status < 500:
            _metrics["errors_4xx"] += 1
        elif status >= 500:
            _metrics["errors_5xx"] += 1

def _record_duplicate():
    with _metrics_lock:
        _metrics["duplicates"] += 1

def _record_acct_error():
    with _metrics_lock:
        _metrics["account_service_errors"] += 1

def _metrics_snapshot():
    with _metrics_lock:
        lats = sorted(_metrics["latency_ms"])
    total = len(lats)
    def pct(p):
        if not lats: return None
        return round(lats[max(0, int(total * p / 100) - 1)], 2)
    snap = {k: v for k, v in _metrics.items() if k != "latency_ms"}
    snap["latency_summary"] = {
        "p50": pct(50), "p95": pct(95), "p99": pct(99),
        "max": round(max(lats), 2) if lats else None,
    }
    snap["retries_total"] = cb.total_retries
    snap.update(rate_limiter.stats())
    fq = get_fallback_queue()
    if fq:
        snap.update(fq.stats())
    return snap

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _validate_event(body):
    errors = []
    for f in ("eventId", "accountId", "type", "amount", "currency", "eventTimestamp"):
        if f not in body:
            errors.append(f"Missing required field: '{f}'")
    if errors:
        return errors
    if body.get("type") not in ("CREDIT", "DEBIT"):
        errors.append("'type' must be 'CREDIT' or 'DEBIT'")
    amt = body.get("amount")
    if not isinstance(amt, (int, float)) or amt <= 0:
        errors.append("'amount' must be a number greater than 0")
    ts = body.get("eventTimestamp", "")
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        errors.append("'eventTimestamp' must be a valid ISO 8601 datetime")
    for f in ("eventId", "accountId", "currency"):
        if not str(body.get(f, "")).strip():
            errors.append(f"'{f}' must not be empty")
    return errors

# ---------------------------------------------------------------------------
# Account Service call (CB + retry + backoff inside CircuitBreaker.call)
# ---------------------------------------------------------------------------
def _do_http_call(trace_id, account_id, payload):
    """Raw HTTP POST to Account Service — wrapped by CB (with retry) in _call_account_service."""
    url  = f"{ACCOUNT_SERVICE_URL}/accounts/{account_id}/transactions"
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", TRACE_HEADER: trace_id},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read())

def _call_account_service(trace_id, account_id, payload):
    """Call Account Service through the circuit breaker (which handles retries)."""
    return cb.call(_do_http_call, trace_id, account_id, payload)

# ---------------------------------------------------------------------------
# Route logic
# ---------------------------------------------------------------------------
def _row_to_dict(row):
    meta = None
    if row["metadata"]:
        try:
            meta = json.loads(row["metadata"])
        except Exception:
            pass
    return {
        "eventId":             row["event_id"],
        "accountId":           row["account_id"],
        "type":                row["type"],
        "amount":              row["amount"],
        "currency":            row["currency"],
        "eventTimestamp":      row["event_timestamp"],
        "metadata":            meta,
        "receivedAt":          row["received_at"],
        "traceId":             row["trace_id"],
        "accountServiceError": bool(row["account_service_error"]),
    }

def route_post_event(body, trace_id):
    errs = _validate_event(body)
    if errs:
        return 422, {"errors": errs}

    db  = get_db()
    now = datetime.now(timezone.utc).isoformat()

    with _db_lock:
        existing = db.execute(
            "SELECT * FROM events WHERE event_id = ?", (body["eventId"],)
        ).fetchone()
        if existing:
            _log(trace_id, "info", f"Duplicate event {body['eventId']}")
            _record_duplicate()
            return 200, _row_to_dict(existing)

        meta_json = json.dumps(body["metadata"]) if body.get("metadata") else None
        db.execute(
            """INSERT INTO events
               (event_id, account_id, type, amount, currency,
                event_timestamp, metadata, received_at, trace_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (body["eventId"], body["accountId"], body["type"], body["amount"],
             body["currency"], body["eventTimestamp"], meta_json, now, trace_id),
        )
        db.commit()

    _log(trace_id, "info", f"Stored event {body['eventId']}")

    acct_payload = {
        "eventId":        body["eventId"],
        "type":           body["type"],
        "amount":         body["amount"],
        "currency":       body["currency"],
        "eventTimestamp": body["eventTimestamp"],
    }

    try:
        _call_account_service(trace_id, body["accountId"], acct_payload)
        _log(trace_id, "info", f"Account Service applied {body['eventId']}")
    except Exception as exc:
        _log(trace_id, "error", f"Account Service unavailable: {exc}")
        _record_acct_error()
        with _db_lock:
            db.execute(
                "UPDATE events SET account_service_error=1 WHERE event_id=?",
                (body["eventId"],),
            )
            db.commit()
        # Enqueue for async retry
        fq = get_fallback_queue()
        if fq:
            fq.enqueue(body["eventId"], body["accountId"], acct_payload, trace_id)
        return 503, {
            "error":   "Account Service unavailable",
            "eventId": body["eventId"],
            "message": "Event recorded and queued for retry when service recovers.",
        }

    with _db_lock:
        row = db.execute(
            "SELECT * FROM events WHERE event_id=?", (body["eventId"],)
        ).fetchone()
    return 201, _row_to_dict(row)

def route_get_event(event_id, trace_id):
    db = get_db()
    with _db_lock:
        row = db.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
    if not row:
        return 404, {"error": f"Event '{event_id}' not found"}
    return 200, _row_to_dict(row)

def route_list_events(account_id, trace_id):
    db = get_db()
    with _db_lock:
        rows = db.execute(
            "SELECT * FROM events WHERE account_id=? ORDER BY event_timestamp ASC",
            (account_id,),
        ).fetchall()
    return 200, [_row_to_dict(r) for r in rows]

# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class GatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _trace(self):
        return self.headers.get(TRACE_HEADER) or new_trace_id()

    def _client_ip(self):
        return self.client_address[0]

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def _send(self, status, body, start=None):
        if start is not None:
            _record_req(status, (time.perf_counter() - start) * 1000)
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_text(self, status, text, start=None):
        if start is not None:
            _record_req(status, (time.perf_counter() - start) * 1000)
        payload = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _check_rate_limit(self, start):
        if not rate_limiter.allow(self._client_ip()):
            self._send(429, {
                "error": "Too Many Requests",
                "message": "Rate limit exceeded. Slow down and retry.",
                "retry_after_secs": 1,
            }, start)
            return False
        return True

    def do_GET(self):
        t      = time.perf_counter()
        trace  = self._trace()
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)

        if not self._check_rate_limit(t):
            return

        if path == "/health":
            try:
                get_db().execute("SELECT 1")
                db_ok = True
            except Exception:
                db_ok = False
            fq = get_fallback_queue()
            self._send(200, {
                "service":        "gateway",
                "status":         "ok" if db_ok else "degraded",
                "database":       "ok" if db_ok else "error",
                "circuit_breaker": cb.state,
                "fallback_queue_depth": fq.depth() if fq else 0,
                "timestamp":      datetime.now(timezone.utc).isoformat(),
            }, t)

        elif path == "/metrics":
            self._send(200, _metrics_snapshot(), t)

        elif path == "/metrics/prometheus":
            snap = _metrics_snapshot()
            text = render_prometheus("gateway", snap)
            self._send_text(200, text, t)

        elif path == "/events":
            account = qs.get("account", [None])[0]
            if not account:
                self._send(400, {"error": "Missing required query param: account"}, t)
                return
            status, body = route_list_events(account, trace)
            self._send(status, body, t)

        elif re.fullmatch(r"/events/(.+)", path):
            m = re.fullmatch(r"/events/(.+)", path)
            status, body = route_get_event(m.group(1), trace)
            self._send(status, body, t)

        else:
            self._send(404, {"error": "Not found"}, t)

    def do_POST(self):
        t     = time.perf_counter()
        trace = self._trace()
        path  = self.path.split("?")[0]

        if not self._check_rate_limit(t):
            return

        if path == "/events":
            body = self._read_json()
            status, resp = route_post_event(body, trace)
            self._send(status, resp, t)
        else:
            self._send(404, {"error": "Not found"}, t)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def create_server(port=8000, db_path=":memory:"):
    init_db(db_path)
    return HTTPServer(("0.0.0.0", port), GatewayHandler)

if __name__ == "__main__":
    port    = int(os.environ.get("PORT", 8000))
    db_path = os.environ.get("DB_PATH", "gateway.db")
    _log("-", "info", f"Gateway starting on port {port}")
    srv = create_server(port, db_path)
    srv.serve_forever()
