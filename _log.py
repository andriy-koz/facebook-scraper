"""
Shared logging setup for pipeline scripts.

Config via env:
    LOG_LEVEL   DEBUG|INFO|WARNING|ERROR  (default INFO)
    LOG_FILE    path/to/file               (default: stderr only)
    STATUS_FILE set by run.sh — when set together with LOG_FILE, stderr
                output is suppressed so the rich dashboard stays clean

Log lines go to both stderr and LOG_FILE unless the dashboard is running,
in which case they go only to the file (tail -f it in another pane).
"""

import logging
import os
import sys


def setup(name, debug=False):
    level = logging.DEBUG if debug else getattr(
        logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)

    log_file = os.environ.get("LOG_FILE")
    dashboard = bool(os.environ.get("STATUS_FILE"))

    handlers = []
    if log_file:
        try:
            handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))
        except OSError as exc:
            # Always surface this one — logging is broken otherwise
            print(f"warning: cannot open LOG_FILE={log_file!r}: {exc}", file=sys.stderr)

    # Suppress stderr handler only when we have a working file *and* the dashboard
    # is active. Otherwise always keep stderr so errors stay visible.
    if not (dashboard and any(isinstance(h, logging.FileHandler) for h in handlers)):
        handlers.append(logging.StreamHandler(sys.stderr))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
        force=True,
    )
    return logging.getLogger(name)
