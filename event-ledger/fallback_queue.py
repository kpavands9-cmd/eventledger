"""
Async fallback queue (stdlib only).

When the Account Service is unavailable, events are written to a persistent
SQLite queue instead of being lost. A background worker polls the queue and
replays each event to the Account Service once it recovers.

The queue is separate from the main Gateway DB so it can be inspected,
drained, or cleared independently.

Configuration via environment variables:
  FALLBACK_DB_PATH      – path to queue DB (default fallback_queue.db)
  FALLBACK_POLL_SECS    – how often the worker polls (default 5)
  FALLBACK_ENABLED      – set to "0" to disable (default "1")
"""
import json
import logging
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger("gateway.fallback_queue")

_ENABLED     = os.environ.get("FALLBACK_ENABLED", "1") != "0"
_DB_PATH     = os.environ.get("FALLBACK_DB_PATH", "fallback_queue.db")
_POLL_SECS   = float(os.environ.get("FALLBACK_POLL_SECS", "5"))


class FallbackQueue:
    """
    Persistent queue for events that could not be forwarded to the Account
    Service. A background thread continuously retries them.
    """

    def __init__(self,
                 account_service_url: str,
                 trace_header: str,
                 db_path: str = _DB_PATH,
                 poll_secs: float = _POLL_SECS,
                 enabled: bool = _ENABLED):
        self._url          = account_service_url
        self._trace_header = trace_header
        self._db_path      = db_path
        self._poll_secs    = poll_secs
        self._enabled      = enabled
        self._lock         = threading.Lock()
        self._replayed     = 0
        self._conn         = None
        self._worker       = None

        if self._enabled:
            self._init_db()
            self._start_worker()

    # ── DB ────────────────────────────────────────────────────────────
    def _init_db(self):
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS fallback_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id    TEXT NOT NULL UNIQUE,
                account_id  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                trace_id    TEXT NOT NULL,
                enqueued_at TEXT NOT NULL,
                attempts    INTEGER DEFAULT 0
            )
        """)
        self._conn.commit()

    # ── Public API ────────────────────────────────────────────────────
    def enqueue(self, event_id: str, account_id: str,
                payload: dict, trace_id: str) -> None:
        """Add a failed event to the retry queue."""
        if not self._enabled:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO fallback_queue
                   (event_id, account_id, payload, trace_id, enqueued_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_id, account_id, json.dumps(payload), trace_id, now),
            )
            self._conn.commit()
        logger.info(json.dumps({
            "service": "gateway", "level": "info", "trace_id": trace_id,
            "message": f"Enqueued event {event_id} in fallback queue",
        }))

    def depth(self) -> int:
        """Number of events still pending in the queue."""
        if not self._enabled or self._conn is None:
            return 0
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM fallback_queue"
            ).fetchone()
        return row["n"] if row else 0

    def stats(self) -> dict:
        return {
            "enabled":               self._enabled,
            "fallback_queue_depth":  self.depth(),
            "fallback_replayed_total": self._replayed,
            "poll_interval_secs":    self._poll_secs,
        }

    # ── Background worker ─────────────────────────────────────────────
    def _start_worker(self):
        self._worker = threading.Thread(
            target=self._run_worker, daemon=True, name="fallback-queue-worker"
        )
        self._worker.start()
        logger.info(json.dumps({
            "service": "gateway", "level": "info", "trace_id": "-",
            "message": "Fallback queue worker started",
        }))

    def _run_worker(self):
        while True:
            time.sleep(self._poll_secs)
            try:
                self._process_queue()
            except Exception as exc:
                logger.error(json.dumps({
                    "service": "gateway", "level": "error", "trace_id": "-",
                    "message": f"Fallback queue worker error: {exc}",
                }))

    def _process_queue(self):
        if self._conn is None:
            return
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM fallback_queue ORDER BY id ASC LIMIT 10"
            ).fetchall()

        for row in rows:
            success = self._replay(row)
            if success:
                with self._lock:
                    self._conn.execute(
                        "DELETE FROM fallback_queue WHERE id = ?", (row["id"],)
                    )
                    self._conn.commit()
                    self._replayed += 1
            else:
                with self._lock:
                    self._conn.execute(
                        "UPDATE fallback_queue SET attempts = attempts + 1 WHERE id = ?",
                        (row["id"],),
                    )
                    self._conn.commit()
                # Stop processing on first failure — Account Service still down
                break

    def _replay(self, row) -> bool:
        """Try to replay one queued event. Returns True on success."""
        payload  = json.loads(row["payload"])
        account_id = row["account_id"]
        trace_id   = row["trace_id"]
        event_id   = row["event_id"]

        url  = f"{self._url}/accounts/{account_id}/transactions"
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json",
                     self._trace_header: trace_id},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                resp.read()
            logger.info(json.dumps({
                "service": "gateway", "level": "info", "trace_id": trace_id,
                "message": f"Fallback queue replayed event {event_id} "
                           f"(attempt {row['attempts'] + 1})",
            }))
            return True
        except Exception as exc:
            logger.warning(json.dumps({
                "service": "gateway", "level": "warning", "trace_id": trace_id,
                "message": f"Fallback queue replay failed for {event_id}: {exc}",
            }))
            return False
