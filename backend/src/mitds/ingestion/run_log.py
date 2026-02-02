"""Per-run log capture for ingestion jobs.

Provides an in-memory buffer that captures log lines during active runs
and flushes them to the database when the run completes. For live runs,
the API reads from the in-memory buffer; for completed runs, from the
log_output column in ingestion_runs.
"""

import logging
from datetime import datetime, timezone

# Active run buffers: run_id (str) -> list of formatted log lines
_active_logs: dict[str, list[str]] = {}

# Safety bound to prevent unbounded memory growth
MAX_LOG_LINES = 5000


def start_capture(run_id: str) -> None:
    """Begin capturing logs for a run."""
    _active_logs[run_id] = []


def append_log(run_id: str, level: str, message: str) -> None:
    """Append a formatted log line to the run's buffer."""
    buf = _active_logs.get(run_id)
    if buf is None:
        return
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level:>7s}] {message}"
    if len(buf) < MAX_LOG_LINES:
        buf.append(line)
    elif len(buf) == MAX_LOG_LINES:
        buf.append(f"{timestamp} [WARNING] Log output truncated at {MAX_LOG_LINES} lines")


def get_live_logs(run_id: str, offset: int = 0) -> list[str] | None:
    """Get log lines for an active run starting from offset.

    Returns None if the run is not active (caller should fall back to DB).
    """
    buf = _active_logs.get(run_id)
    if buf is None:
        return None
    return buf[offset:]


def finish_capture(run_id: str) -> str:
    """End capture, remove from memory, return the full log text."""
    buf = _active_logs.pop(run_id, [])
    return "\n".join(buf)


class RunLogHandler(logging.Handler):
    """Logging handler that captures output into a per-run buffer."""

    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            append_log(self.run_id, record.levelname, message)
        except Exception:
            pass  # Never let logging failures propagate
