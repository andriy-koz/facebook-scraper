#!/usr/bin/env python3
"""
progress — Live dashboard filter for the enrichment pipeline.

Reads JSONL from stdin, passes through to stdout unchanged.
Displays a live progress dashboard on stderr using rich.

Usage:
    ... | progress.py --total N [--done D] | tee checkpoint.jsonl
"""

import argparse
import json
import os
import select
import signal
import sys
import time

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

signal.signal(signal.SIGPIPE, signal.SIG_DFL)

LOGO = r"""
 _____ ___   ___
|  ___| _ ) / __| __ _ _ __ _ _ __  ___ _ _
| |_  | _ \ \__ \/ _| '_/ _` | '_ \/ -_) '_|
|_|   |___/ |___/\__|_| \__,_| .__/\___|_|
                              |_|
"""

_STATUS_FILE = os.environ.get("STATUS_FILE")


def _read_status():
    if _STATUS_FILE:
        try:
            with open(_STATUS_FILE) as f:
                return f.read().strip()
        except (OSError, FileNotFoundError):
            pass
    return ""


def fmt_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def fmt_rate(count, seconds):
    if seconds < 1:
        return "--"
    return f"{count / (seconds / 60):.1f}/min"


def pct(n, total):
    return n / total * 100 if total else 0


def make_bar_row(table, label, value, total, color):
    p = pct(value, total)
    bar = ProgressBar(total=100, completed=p, width=30)
    bar.style = "bar.back"
    bar.complete_style = color
    bar.finished_style = color
    table.add_row(
        Text(f" {label}", style="bold"),
        bar,
        Text(f"{value}/{total}", style="bold"),
        Text(f"{p:5.1f}%", style="dim"),
    )


def build_dashboard(total, processed, fb_urls, emails, elapsed, status=""):
    logo = Text(LOGO.strip("\n"), style="bold cyan")

    bars = Table(box=None, show_header=False, padding=(0, 1))
    bars.add_column(width=12)
    bars.add_column(width=32)
    bars.add_column(width=8, justify="right")
    bars.add_column(width=7, justify="right")

    make_bar_row(bars, "Processed", processed, total, "green")
    make_bar_row(bars, "Emails", emails, processed, "magenta")

    stats = Table(box=None, show_header=False, padding=(0, 2))
    stats.add_column(style="dim", width=16)
    stats.add_column(width=12)
    stats.add_column(style="dim", width=16)
    stats.add_column(width=12)

    stats.add_row(
        " FB URLs found", Text(str(fb_urls), style="bold cyan"),
        "Elapsed", Text(fmt_time(elapsed), style="bold"),
    )
    stats.add_row(
        " Emails found", Text(str(emails), style="bold magenta"),
        "Rate", Text(fmt_rate(processed, elapsed), style="bold"),
    )

    status_text = Text(f" {status}", style="dim italic") if status else Text("")

    return Panel(
        Group(logo, Text(""), bars, Text(""), stats, Text(""), status_text),
        border_style="cyan",
        padding=(0, 1),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--total", type=int, required=True)
    ap.add_argument("--done", type=int, default=0)
    args = ap.parse_args()

    total = args.total
    processed = args.done
    fb_urls = 0
    emails = 0
    start = time.time()
    console = Console(stderr=True)

    # Non-TTY: just passthrough, no dashboard
    if not sys.stderr.isatty():
        for line in sys.stdin:
            sys.stdout.write(line)
            sys.stdout.flush()
        return

    try:
        with Live(
            build_dashboard(total, processed, fb_urls, emails, 0),
            console=console,
            refresh_per_second=4,
            redirect_stdout=False,
        ) as live:
            while True:
                ready, _, _ = select.select([sys.stdin], [], [], 1.0)
                if ready:
                    line = sys.stdin.readline()
                    if not line:
                        break
                    sys.stdout.write(line)
                    sys.stdout.flush()

                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, ValueError):
                        elapsed = time.time() - start
                        live.update(
                            build_dashboard(total, processed, fb_urls, emails, elapsed, _read_status())
                        )
                        continue

                    processed += 1
                    if record.get("fb_url"):
                        fb_urls += 1
                    if record.get("fb_email"):
                        emails += 1

                elapsed = time.time() - start
                live.update(
                    build_dashboard(total, processed, fb_urls, emails, elapsed, _read_status())
                )
    except (KeyboardInterrupt, BrokenPipeError):
        pass

    elapsed = time.time() - start
    console.print(
        f"\n[bold green]Done:[/] {processed}/{total} leads"
        f" | {fb_urls} FB URLs | {emails} emails"
        f" | {fmt_time(elapsed)}"
    )


if __name__ == "__main__":
    main()
