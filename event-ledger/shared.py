"""Shared constants and utilities used by both services."""
import uuid

TRACE_HEADER = "X-Trace-ID"


def new_trace_id() -> str:
    return str(uuid.uuid4())
