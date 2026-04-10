#!/usr/bin/env python3
"""
jsonl2csv — Convert JSONL stream to CSV.

Reads JSONL from stdin, writes CSV to stdout. Field order is fixed
to match the pipeline output.

Usage:
    fb_scrape.py < input.jsonl | jsonl2csv.py > enriched.csv
    cat results.jsonl | jsonl2csv.py > enriched.csv
"""

import csv
import json
import signal
import sys

import _log

log = _log.setup("jsonl2csv")

if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


FIELDS = [
    "business", "city", "state", "phone", "address",
    "fb_url", "fb_email", "fb_instagram", "fb_followers",
    "fb_likes", "fb_intro", "fb_creation_date", "fb_ad_status",
]


def main():
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()

    total = emitted = parse_errors = write_errors = 0

    for line_num, line in enumerate(sys.stdin, start=1):
        line = line.strip()
        if not line:
            continue
        total += 1
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as exc:
            parse_errors += 1
            log.error("line %d: JSON decode error — %s", line_num, exc)
            continue
        if not isinstance(rec, dict):
            parse_errors += 1
            log.error("line %d: expected JSON object, got %s",
                      line_num, type(rec).__name__)
            continue
        try:
            writer.writerow(rec)
            sys.stdout.flush()
            emitted += 1
        except (csv.Error, TypeError, ValueError) as exc:
            write_errors += 1
            log.error("line %d: CSV write error — %s", line_num, exc)
        except BrokenPipeError:
            return

    log.info("wrote %d rows (%d processed, %d parse errors, %d write errors)",
             emitted, total, parse_errors, write_errors)


if __name__ == "__main__":
    main()
