# Event Ledger

A distributed financial transaction event processing system built as two independent microservices. Handles out-of-order delivery, duplicate events, and Account Service failures gracefully.

---

## Quick start (TL;DR)

```bash
# 1. Run the full test suite (no setup, proves everything works)
python3 tests/test_event_ledger.py        # → Ran 105 tests ... OK

# 2a. Run both services with Docker
docker compose up --build

# 2b. …or run manually in two terminals (no pip install needed)
python3 account_service/main.py                                    # terminal 1
ACCOUNT_SERVICE_URL=http://localhost:8001 python3 gateway/main.py  # terminal 2
```

Then open your browser to **http://localhost:8000/health** to confirm the Gateway is up. See [Checking it in the browser](#checking-it-in-the-browser) for the full list of URLs you can visit.

---

## Architecture

```
Browser / Client ──→  Event Gateway  (port 8000)
                            │
                            │  REST + X-Trace-ID header
                            │  (Circuit Breaker protected)
                            ▼
                      Account Service  (port 8001)
```

### Event Gateway (port 8000)
Public-facing entry point. Validates events, enforces idempotency, persists events to its own SQLite database, and forwards transactions to the Account Service via HTTP. Uses a **circuit breaker** to protect against Account Service failures — GET endpoints remain available even when the circuit is open.

### Account Service (port 8001)
Internal service (not exposed to external clients). Owns account state: balances and transaction history, in its own separate SQLite database. Balance is computed as an aggregate `SUM(CREDITs) − SUM(DEBITs)`, so out-of-order event arrival never affects correctness.

### Key design decisions

- **Idempotency** is enforced at both layers: the Gateway deduplicates by `eventId` before persisting and before forwarding; the Account Service deduplicates again before applying. This means retries at any layer are safe.
- **Out-of-order tolerance**: balances use aggregate sums rather than running totals — arrival order is irrelevant. Event listings sort by `eventTimestamp` at query time.
- **Zero external dependencies**: both services use Python stdlib only (`http.server`, `sqlite3`, `urllib`). No pip install required.

---

## Prerequisites

- Python 3.12+  *(for manual startup or tests)*
- Docker + Docker Compose  *(for containerised startup)*

---

## Setup & running

### Option A: Docker Compose (recommended)

```bash
docker compose up --build
```

The Gateway will be available at `http://localhost:8000` and the Account Service at `http://localhost:8001`. The Gateway waits for the Account Service health check to pass before starting.

### Option B: Manual (no Docker required)

The two services are independent processes, so you run each in its own terminal. **No `pip install` is needed** — both services use only the Python standard library.

Open two terminal windows (or two tabs / split panes in VSCode) and `cd` into the `event-ledger` folder in both.

#### Step 1 — Start the Account Service first

The Gateway depends on the Account Service, so start this one first.

**macOS / Linux:**
```bash
cd event-ledger
python3 account_service/main.py
```

**Windows (PowerShell):**
```powershell
cd event-ledger
python account_service/main.py
```

You should see a log line confirming it's up:
```json
{"time": "...", "level": "info", "service": "account-service", "trace_id": "-", "message": "Account Service starting on port 8001"}
```
Leave this terminal running.

#### Step 2 — Start the Gateway

In the **second** terminal, point the Gateway at the Account Service via the `ACCOUNT_SERVICE_URL` environment variable.

**macOS / Linux:**
```bash
cd event-ledger
ACCOUNT_SERVICE_URL=http://localhost:8001 python3 gateway/main.py
```

**Windows (PowerShell):**
```powershell
cd event-ledger
$env:ACCOUNT_SERVICE_URL = "http://localhost:8001"
python gateway/main.py
```

**Windows (Command Prompt / cmd.exe):**
```cmd
cd event-ledger
set ACCOUNT_SERVICE_URL=http://localhost:8001
python gateway/main.py
```

You should see:
```json
{"time": "...", "level": "info", "service": "gateway", "trace_id": "-", "message": "Gateway starting on port 8000"}
```

The Gateway is now at `http://localhost:8000` and the Account Service at `http://localhost:8001`.

#### Step 3 — Verify it works

In a **third** terminal, send a test event and check the balance:

```bash
# Submit a CREDIT of 500
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1","accountId":"acct-42","type":"CREDIT","amount":500,"currency":"USD","eventTimestamp":"2026-05-15T10:00:00Z"}'

# Check the balance on the Account Service
curl http://localhost:8001/accounts/acct-42/balance
# → {"accountId": "acct-42", "balance": 500.0}

# Check the Gateway health (note the circuit breaker state)
curl http://localhost:8000/health
```

To stop either service, press `Ctrl+C` in its terminal.

#### Configuration (environment variables)

Both services accept optional environment variables — the defaults work out of the box:

| Variable | Service | Default | Purpose |
|---|---|---|---|
| `PORT` | both | `8000` / `8001` | Port to listen on |
| `DB_PATH` | both | `gateway.db` / `account_service.db` | SQLite database file path |
| `ACCOUNT_SERVICE_URL` | Gateway | `http://localhost:8001` | Where to reach the Account Service |
| `ACCOUNT_TIMEOUT` | Gateway | `5` | Per-request timeout (seconds) for Account Service calls |
| `RATE_LIMIT_RPS` | Gateway | `20` | Requests/second per client IP before 429 |
| `RATE_LIMIT_BURST` | Gateway | `40` | Burst capacity for the rate limiter |
| `RATE_LIMIT_ENABLED` | Gateway | `1` | Set to `0` to disable rate limiting |
| `FALLBACK_ENABLED` | Gateway | `1` | Set to `0` to disable the async retry queue |
| `FALLBACK_POLL_SECS` | Gateway | `5` | How often the fallback worker retries queued events |

> **Note on database files:** running manually creates `gateway.db`, `account_service.db`, and `fallback_queue.db` in the project folder — these are the embedded SQLite databases. Delete them anytime to reset all state. They are safe to exclude from version control.

#### Running in VSCode

A `.vscode/launch.json` is included with ready-made run configurations. Open the **Run and Debug** panel (`Ctrl+Shift+D`), then from the dropdown choose:

- **Both Services** — launches the Account Service and Gateway together with one click (press `F5`)
- **Account Service** / **Gateway** — launch either one individually
- **Run All Tests** — run the full test suite

Each runs in its own integrated-terminal tab with the correct environment variables already set, and you can set breakpoints by clicking in the gutter.

---

## Checking it in the browser

Once both services are running, you can inspect the system directly in any web browser. **All `GET` endpoints work by simply typing the URL** — they return JSON, which browsers display nicely (use Firefox, or a Chrome JSON-viewer extension, for pretty formatting).

> **Important:** A browser address bar can only make `GET` requests. To **submit** an event (`POST /events`) or apply a transaction you need curl, Postman, or the browser console (examples below). So the usual flow is: POST an event with curl, then *view the results* in the browser.

### URLs you can open directly

**Event Gateway** — `http://localhost:8000`

| Open this URL | What you'll see |
|---|---|
| http://localhost:8000/health | Gateway status, database connectivity, **circuit breaker state**, fallback-queue depth |
| http://localhost:8000/metrics | Request counts, latency p50/p95/p99, error counts, duplicate count |
| http://localhost:8000/metrics/prometheus | The same metrics in Prometheus scrape format (plain text) |
| http://localhost:8000/events?account=acct-42 | All events for `acct-42`, in chronological order |
| http://localhost:8000/events/evt-1 | A single event by its ID (includes its trace ID) |

**Account Service** — `http://localhost:8001`

| Open this URL | What you'll see |
|---|---|
| http://localhost:8001/health | Account Service status + database connectivity |
| http://localhost:8001/metrics | Request counts, credits/debits applied, latency |
| http://localhost:8001/accounts/acct-42/balance | Current balance for `acct-42` |
| http://localhost:8001/accounts/acct-42 | Full account detail + transaction history |

*(Replace `acct-42` and `evt-1` with your own IDs.)*

### A complete walkthrough: POST with curl, then view in the browser

**Step 1 — submit a couple of events** (terminal, since browsers can't POST easily):

```bash
# A CREDIT of 500
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1","accountId":"acct-42","type":"CREDIT","amount":500,"currency":"USD","eventTimestamp":"2026-05-15T10:00:00Z"}'

# A DEBIT of 120 — note the EARLIER timestamp, even though it arrives second
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-2","accountId":"acct-42","type":"DEBIT","amount":120,"currency":"USD","eventTimestamp":"2026-05-15T08:00:00Z"}'
```

**Step 2 — now view the results in your browser:**

- Visit **http://localhost:8001/accounts/acct-42/balance** → you'll see `{"accountId": "acct-42", "balance": 380.0}` (500 − 120, correct despite out-of-order arrival).
- Visit **http://localhost:8000/events?account=acct-42** → both events appear, with `evt-2` listed *first* because its `eventTimestamp` is earlier (08:00 before 10:00) — proving chronological ordering.
- Visit **http://localhost:8001/accounts/acct-42** → full transaction history for the account.
- Visit **http://localhost:8000/metrics** → watch the request counter climb each time you refresh.

**Step 3 — see idempotency in action:**

Submit `evt-1` a second time:
```bash
curl -i -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1","accountId":"acct-42","type":"CREDIT","amount":500,"currency":"USD","eventTimestamp":"2026-05-15T10:00:00Z"}'
```
The response status is **200** (not 201) and the balance in the browser **stays at 380** — the duplicate was ignored.

### Posting from the browser console (no curl needed)

If you'd rather not use a terminal, open the browser to any Gateway page, press **F12** to open DevTools, go to the **Console** tab, and paste:

```javascript
fetch("http://localhost:8000/events", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    eventId: "evt-99", accountId: "acct-42", type: "CREDIT",
    amount: 250, currency: "USD", eventTimestamp: "2026-05-15T12:00:00Z"
  })
}).then(r => r.json()).then(console.log);
```

Then refresh the balance URL to see the change.

### Watching graceful degradation in the browser

1. Stop **only** the Account Service (`Ctrl+C` in its terminal); leave the Gateway running.
2. Try to POST a new event — you'll get a **503** with the `eventId` in the body (the event is still saved and queued for retry).
3. But refresh **http://localhost:8000/events?account=acct-42** in the browser — it **still works**, because GET reads only from the Gateway's own database.
4. Refresh **http://localhost:8000/health** a few times — after 3 failed POSTs you'll see `"circuit_breaker": "OPEN"` and a non-zero `fallback_queue_depth`.
5. Restart the Account Service. Within a few seconds the background worker replays the queued events, the circuit returns to `CLOSED`, and the balance catches up automatically.

---

## Running the tests

No dependencies to install — stdlib only.

```bash
cd event-ledger
python3 tests/test_event_ledger.py
```

Expected output:
```
Ran 105 tests in ~7s
OK
```

### What's tested

| Test class | Coverage |
|---|---|
| `TestAccountServiceLogic` | Balance computation, out-of-order, idempotency, validation (Account Service logic layer) |
| `TestGatewayValidation` | All validation rules, idempotency, event listing/retrieval (Gateway logic layer) |
| `TestCircuitBreaker` | State transitions: CLOSED→OPEN→HALF_OPEN→CLOSED, threshold, recovery timeout |
| `TestTracePropagation` | Trace ID forwarded to Account Service, generated when absent, stored on event |
| `TestResiliency` | 503 on Account Service failure, event still stored after 503, GET works while Account Service is down, circuit opens after threshold |
| `TestObservability` | Health endpoints (both services), database status, circuit breaker state in health, metrics endpoints, duplicate count tracking |
| `TestIntegration` | Full Gateway→Account Service round-trip over real HTTP: balance updates, idempotency, out-of-order, validation, trace headers, service separation, graceful degradation |
| `TestPrometheus` *(bonus)* | Prometheus text exposition format, required metric names, valid output, service labels |
| `TestRetryBackoff` *(bonus)* | Retry on transient failure, exponential backoff, jitter bounds, retry counter, disable switch |
| `TestRateLimiting` *(bonus)* | Token bucket allow/reject, per-IP isolation, 429 over HTTP, rejection stats |
| `TestContractGatewayConsumer` *(bonus)* | Request/response shape contracts between Gateway and Account Service |
| `TestFallbackQueue` *(bonus)* | Enqueue on 503, background replay on recovery, queue depth, replay counter |

---

## API reference

### Event Gateway (port 8000)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/events` | Submit a transaction event |
| `GET` | `/events/{id}` | Retrieve a single event by ID |
| `GET` | `/events?account={accountId}` | List events for an account (chronological by `eventTimestamp`) |
| `GET` | `/health` | Health check — database status + circuit breaker state |
| `GET` | `/metrics` | Request counts, latency percentiles (p50/p95/p99), error counts, duplicate count |

### Account Service (port 8001)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/accounts/{accountId}/transactions` | Apply a transaction to an account |
| `GET` | `/accounts/{accountId}/balance` | Get current balance |
| `GET` | `/accounts/{accountId}` | Account details + full transaction history |
| `GET` | `/health` | Health check — database status |
| `GET` | `/metrics` | Request counts, credits/debits applied, latency |

### Event payload

```json
{
  "eventId":        "evt-001",
  "accountId":      "acct-123",
  "type":           "CREDIT",
  "amount":         150.00,
  "currency":       "USD",
  "eventTimestamp": "2026-05-15T14:02:11Z",
  "metadata": {
    "source":  "mainframe-batch",
    "batchId": "B-9042"
  }
}
```

### Example curl session

```bash
# Submit an event
curl -s -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1","accountId":"acct-42","type":"CREDIT","amount":500,"currency":"USD","eventTimestamp":"2026-05-15T10:00:00Z"}'

# Submit another (out of order — earlier timestamp, arrives second)
curl -s -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-2","accountId":"acct-42","type":"DEBIT","amount":120,"currency":"USD","eventTimestamp":"2026-05-15T08:00:00Z"}'

# Submit the first event again — idempotent, returns original with HTTP 200
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1","accountId":"acct-42","type":"CREDIT","amount":500,"currency":"USD","eventTimestamp":"2026-05-15T10:00:00Z"}'
# → 200

# List events for account — returned in eventTimestamp order (evt-2 first)
curl -s http://localhost:8000/events?account=acct-42

# Check balance directly on Account Service
curl -s http://localhost:8001/accounts/acct-42/balance
# → {"accountId": "acct-42", "balance": 380.0}

# Check Gateway health (includes circuit breaker state)
curl -s http://localhost:8000/health
```

---

## Resiliency: Circuit Breaker

The Gateway wraps every call to the Account Service in a circuit breaker with three states:

| State | Behaviour |
|---|---|
| **CLOSED** | Normal — calls pass through |
| **OPEN** | Account Service is failing; calls are rejected immediately with `503 Service Unavailable`. Open for `recovery_timeout` seconds (default 15s). |
| **HALF_OPEN** | After the timeout, one probe call is allowed. Success → CLOSED; failure → OPEN again. |

Default thresholds: `failure_threshold=3`, `recovery_timeout=15s`.

**Why circuit breaker over the alternatives:**

- *Timeout + retry* keeps hammering a struggling service until exhaustion, amplifying load at the worst possible moment.
- *Bulkhead* limits concurrency but doesn't stop calls entirely when the service is down.
- *Circuit breaker* fails fast: once the threshold is reached it stops all calls and gives the Account Service space to recover. It also provides observable state — `GET /health` reports `"circuit_breaker": "OPEN"` so the on-call engineer immediately knows what's happening without reading logs.

**Graceful degradation behaviour:**

- `POST /events` → `503` with `eventId` in the response body. The event **is stored** in the Gateway's database, so it can be retried later.
- `GET /events/{id}` and `GET /events?account=…` → **always work** regardless of circuit state (Gateway-local reads only).
- Balance queries via the Account Service → clear `503` when unreachable.

---

## Observability

- **Structured JSON logging** in both services on every request and significant event:
  ```json
  {"time": "2026-05-15T14:02:11Z", "level": "info", "service": "gateway",
   "trace_id": "a1b2c3d4-...", "message": "Stored event evt-001"}
  ```
- **`X-Trace-ID` propagation**: every request generates (or forwards) a UUID trace ID. Both services log it, and the Gateway stores it on the event record — giving a complete traceable path across both services from a single client request.
- **`/health`** endpoints report database connectivity and (Gateway only) circuit breaker state.
- **`/metrics`** endpoints expose request counts by status, p50/p95/p99 latency, error counts, and (Gateway) duplicate event count and Account Service error count.

---

## Bonus features (implemented)

All six bonus opportunities from the brief are implemented and tested:

| Feature | How to see it |
|---|---|
| **Prometheus metrics** | Open http://localhost:8000/metrics/prometheus — proper text exposition format with `# HELP` / `# TYPE` / labels, ready for a Prometheus scraper |
| **Retry + exponential backoff + jitter** | Built into the circuit breaker (`gateway/circuit_breaker.py`); transient failures are retried with `random(0, base · 2^attempt)` delays before counting as a failure. The `retries_total` counter appears in `/metrics` |
| **Rate limiting** | Token-bucket per client IP on the Gateway; returns `429 Too Many Requests` when exceeded. Tunable via `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` |
| **Contract tests** | `TestContractGatewayConsumer` verifies the Gateway and Account Service agree on request/response shapes |
| **Async fallback queue** | When the Account Service is down, failed events are persisted to a SQLite queue (`fallback_queue.py`) and a background worker replays them on recovery. Queue depth is shown in `/health` |
| **OpenTelemetry-ready tracing** | Trace IDs use a W3C-compatible format and propagate via the `X-Trace-ID` header, so wiring up an OTLP exporter → Jaeger/Zipkin is a drop-in change |

### Further ideas (not implemented)

- Swap the in-process tracing for the full `opentelemetry-sdk` with an OTLP exporter and a Jaeger sidecar for visual trace timelines.
- Replace the hand-rolled Prometheus emitter with the official `prometheus-client` library.
- Add Pact-based consumer/provider contract verification with a shared broker.

