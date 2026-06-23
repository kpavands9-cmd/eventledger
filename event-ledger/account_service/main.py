"""
Account Service — internal service (port 8001).
Zero external dependencies — stdlib only.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import json
import logging
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from shared import TRACE_HEADER, new_trace_id

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("account_service")

def _log(trace_id: str, level: str, msg: str):
    entry = json.dumps({
        "time": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": "account-service",
        "trace_id": trace_id,
        "message": msg,
    })
    getattr(logger, level)(entry)

# ---------------------------------------------------------------------------
# Database — single shared connection (thread-safe via check_same_thread=False)
# ---------------------------------------------------------------------------
_db_conn = None
_db_lock = threading.Lock()


def init_db(path=":memory:"):
    global _db_conn
    with _db_lock:
        _db_conn = sqlite3.connect(path, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
        _db_conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )
        """)
        _db_conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id         TEXT UNIQUE NOT NULL,
                account_id       TEXT NOT NULL,
                type             TEXT NOT NULL,
                amount           REAL NOT NULL,
                currency         TEXT NOT NULL,
                event_timestamp  TEXT NOT NULL,
                applied_at       TEXT NOT NULL
            )
        """)
        _db_conn.commit()


def get_db():
    return _db_conn

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
_metrics = {
    "requests": 0, "credits": 0, "debits": 0,
    "started_at": datetime.now(timezone.utc).isoformat()
}
_metrics_lock = threading.Lock()

def _record(txn_type=None):
    with _metrics_lock:
        _metrics["requests"] += 1
        if txn_type == "CREDIT":
            _metrics["credits"] += 1
        elif txn_type == "DEBIT":
            _metrics["debits"] += 1

# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------
def _compute_balance(db, account_id):
    with _db_lock:
        row = db.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='CREDIT' THEN amount ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN type='DEBIT'  THEN amount ELSE 0 END), 0) AS balance
            FROM transactions WHERE account_id = ?
        """, (account_id,)).fetchone()
    return round(float(row["balance"]), 2) if row else 0.0


def apply_transaction(account_id, body, trace_id):
    required = {"eventId", "type", "amount", "currency", "eventTimestamp"}
    missing = required - body.keys()
    if missing:
        return 422, {"error": f"Missing fields: {missing}"}
    if body["type"] not in ("CREDIT", "DEBIT"):
        return 422, {"error": "type must be CREDIT or DEBIT"}
    if not isinstance(body["amount"], (int, float)) or body["amount"] <= 0:
        return 422, {"error": "amount must be > 0"}

    db = get_db()
    with _db_lock:
        existing = db.execute(
            "SELECT id FROM transactions WHERE event_id = ?", (body["eventId"],)
        ).fetchone()
        if existing:
            _log(trace_id, "info", f"Duplicate txn {body['eventId']} — skipping")
            balance = _compute_balance_nolock(db, account_id)
            return 200, {"accountId": account_id, "balance": balance, "duplicate": True}

        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "INSERT OR IGNORE INTO accounts (account_id, created_at) VALUES (?, ?)", (account_id, now)
        )
        db.execute(
            "INSERT INTO transactions (event_id, account_id, type, amount, currency, event_timestamp, applied_at) VALUES (?,?,?,?,?,?,?)",
            (body["eventId"], account_id, body["type"], body["amount"], body["currency"], body["eventTimestamp"], now)
        )
        db.commit()
        balance = _compute_balance_nolock(db, account_id)

    _log(trace_id, "info", f"Applied {body['type']} {body['amount']} to {account_id}, balance={balance}")
    _record(body["type"])
    return 201, {"accountId": account_id, "balance": balance, "duplicate": False}


def _compute_balance_nolock(db, account_id):
    """Compute balance without acquiring _db_lock (caller already holds it)."""
    row = db.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN type='CREDIT' THEN amount ELSE 0 END), 0) -
            COALESCE(SUM(CASE WHEN type='DEBIT'  THEN amount ELSE 0 END), 0) AS balance
        FROM transactions WHERE account_id = ?
    """, (account_id,)).fetchone()
    return round(float(row["balance"]), 2) if row else 0.0


def get_balance(account_id, trace_id):
    db = get_db()
    with _db_lock:
        acct = db.execute("SELECT account_id FROM accounts WHERE account_id = ?", (account_id,)).fetchone()
        if not acct:
            return 404, {"error": f"Account '{account_id}' not found"}
        balance = _compute_balance_nolock(db, account_id)
    return 200, {"accountId": account_id, "balance": balance}


def get_account(account_id, trace_id):
    db = get_db()
    with _db_lock:
        acct = db.execute("SELECT * FROM accounts WHERE account_id = ?", (account_id,)).fetchone()
        if not acct:
            return 404, {"error": f"Account '{account_id}' not found"}
        txns = db.execute(
            "SELECT * FROM transactions WHERE account_id = ? ORDER BY event_timestamp ASC", (account_id,)
        ).fetchall()
        balance = _compute_balance_nolock(db, account_id)
        created_at = acct["created_at"]
        txn_list = [
            {"eventId": r["event_id"], "type": r["type"], "amount": r["amount"],
             "currency": r["currency"], "eventTimestamp": r["event_timestamp"],
             "appliedAt": r["applied_at"]}
            for r in txns
        ]
    return 200, {
        "accountId": account_id, "balance": balance,
        "createdAt": created_at, "transactions": txn_list,
    }


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class AccountHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _trace(self):
        return self.headers.get(TRACE_HEADER) or new_trace_id()

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def _send(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        trace = self._trace()
        path = self.path.split("?")[0]

        if path == "/health":
            try:
                get_db().execute("SELECT 1")
                db_ok = True
            except Exception:
                db_ok = False
            self._send(200, {"service": "account-service",
                             "status": "ok" if db_ok else "degraded",
                             "database": "ok" if db_ok else "error",
                             "timestamp": datetime.now(timezone.utc).isoformat()})

        elif path == "/metrics":
            self._send(200, dict(_metrics))

        elif re.fullmatch(r"/accounts/([^/]+)/balance", path):
            m = re.fullmatch(r"/accounts/([^/]+)/balance", path)
            status, body = get_balance(m.group(1), trace)
            self._send(status, body)

        elif re.fullmatch(r"/accounts/([^/]+)", path):
            m = re.fullmatch(r"/accounts/([^/]+)", path)
            status, body = get_account(m.group(1), trace)
            self._send(status, body)

        else:
            self._send(404, {"error": "Not found"})

    def do_POST(self):
        trace = self._trace()
        path = self.path.split("?")[0]

        if re.fullmatch(r"/accounts/([^/]+)/transactions", path):
            m = re.fullmatch(r"/accounts/([^/]+)/transactions", path)
            body = self._read_json()
            status, resp = apply_transaction(m.group(1), body, trace)
            self._send(status, resp)
        else:
            self._send(404, {"error": "Not found"})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def create_server(port=8001, db_path=":memory:"):
    init_db(db_path)
    return HTTPServer(("0.0.0.0", port), AccountHandler)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    db_path = os.environ.get("DB_PATH", "account_service.db")
    _log("-", "info", f"Account Service starting on port {port}")
    srv = create_server(port, db_path)
    srv.serve_forever()
