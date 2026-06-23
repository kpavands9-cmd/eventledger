"""
Prometheus text exposition format emitter (stdlib only — no prometheus_client needed).

Produces /metrics/prometheus output compatible with any Prometheus scraper.
Format spec: https://prometheus.io/docs/instrumenting/exposition_formats/
"""
import time as _time


def _labels(d: dict) -> str:
    if not d:
        return ""
    parts = ','.join(f'{k}="{v}"' for k, v in d.items())
    return "{" + parts + "}"


def render_prometheus(service: str, metrics_snapshot: dict) -> str:
    """Convert our internal metrics dict to Prometheus text format."""
    lines = []
    ts = int(_time.time() * 1000)  # milliseconds

    def gauge(name, value, labels=None, help_text="", mtype="gauge"):
        full = f"event_ledger_{name}"
        if help_text:
            lines.append(f"# HELP {full} {help_text}")
        lines.append(f"# TYPE {full} {mtype}")
        lstr = _labels({**(labels or {}), "service": service})
        if value is not None:
            lines.append(f"{full}{lstr} {value} {ts}")

    def counter(name, value, labels=None, help_text=""):
        gauge(name, value, labels, help_text, mtype="counter")

    def histogram_summary(name, snapshot, labels=None, help_text=""):
        full = f"event_ledger_{name}"
        if help_text:
            lines.append(f"# HELP {full} {help_text}")
        lines.append(f"# TYPE {full} summary")
        lstr_base = {**(labels or {}), "service": service}
        for q, key in [(0.5, "p50"), (0.95, "p95"), (0.99, "p99")]:
            v = snapshot.get(key)
            if v is not None:
                lstr = _labels({**lstr_base, "quantile": str(q)})
                lines.append(f"{full}{lstr} {v} {ts}")
        mx = snapshot.get("max")
        if mx is not None:
            lstr = _labels({**lstr_base, "quantile": "1.0"})
            lines.append(f"{full}{lstr} {mx} {ts}")

    # --- Common metrics ---
    counter("requests_total", metrics_snapshot.get("requests", 0),
            help_text="Total HTTP requests handled")

    counter("errors_4xx_total", metrics_snapshot.get("errors_4xx", 0),
            help_text="Total 4xx responses")

    counter("errors_5xx_total", metrics_snapshot.get("errors_5xx", 0),
            help_text="Total 5xx responses")

    lats = metrics_snapshot.get("latency_summary") or metrics_snapshot.get("latency_ms", {})
    if isinstance(lats, dict):
        histogram_summary("request_duration_ms", lats,
                          help_text="Request latency in milliseconds")

    # --- Gateway-specific ---
    if "duplicates" in metrics_snapshot:
        counter("duplicate_events_total", metrics_snapshot["duplicates"],
                help_text="Duplicate eventId submissions rejected")

    if "account_service_errors" in metrics_snapshot:
        counter("account_service_errors_total", metrics_snapshot["account_service_errors"],
                help_text="Failures calling the Account Service")

    # --- Account Service-specific ---
    if "credits" in metrics_snapshot:
        counter("credits_applied_total", metrics_snapshot["credits"],
                help_text="CREDIT transactions applied")

    if "debits" in metrics_snapshot:
        counter("debits_applied_total", metrics_snapshot["debits"],
                help_text="DEBIT transactions applied")

    # --- Retry metrics (if present) ---
    if "retries_total" in metrics_snapshot:
        counter("retries_total", metrics_snapshot["retries_total"],
                help_text="Total retry attempts on Account Service calls")

    # --- Rate limit metrics (if present) ---
    if "rate_limited_total" in metrics_snapshot:
        counter("rate_limited_total", metrics_snapshot["rate_limited_total"],
                help_text="Requests rejected by rate limiter")

    # --- Fallback queue metrics (if present) ---
    if "fallback_queue_depth" in metrics_snapshot:
        gauge("fallback_queue_depth", metrics_snapshot["fallback_queue_depth"],
              help_text="Events pending in fallback retry queue")

    if "fallback_replayed_total" in metrics_snapshot:
        counter("fallback_replayed_total", metrics_snapshot["fallback_replayed_total"],
                help_text="Events successfully replayed from fallback queue")

    lines.append("")  # trailing newline required by spec
    return "\n".join(lines)
