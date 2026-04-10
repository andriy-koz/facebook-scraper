#!/usr/bin/env python3
"""
csv2jsonl — Convert Outscraper CSV to JSONL for the pipeline.

Reads CSV from stdin (or file arg), emits one JSON object per line to stdout.
Selects and renames columns to match what downstream tools expect.

Usage:
    csv2jsonl.py < leads.csv | ddg_search.py
    csv2jsonl.py leads.csv | ddg_search.py
"""

import csv
import json
import signal
import sys

import _log

log = _log.setup("csv2jsonl")

if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


def _open(path):
    try:
        # utf-8-sig strips BOM if present (common from Excel exports)
        return open(path, "r", encoding="utf-8-sig", newline="")
    except FileNotFoundError:
        log.error("input file not found: %s", path)
    except PermissionError:
        log.error("permission denied reading %s", path)
    except OSError as exc:
        log.error("cannot open %s: %s", path, exc)
    sys.exit(1)


def main():
    if len(sys.argv) > 1:
        src = _open(sys.argv[1])
        log.debug("reading %s", sys.argv[1])
    else:
        src = sys.stdin
        log.debug("reading stdin")

    try:
        reader = csv.DictReader(src)
    except csv.Error as exc:
        log.error("CSV parse error: %s", exc)
        sys.exit(1)

    if not reader.fieldnames:
        log.error("CSV has no header row (empty file?)")
        sys.exit(1)

    log.debug("columns: %s", reader.fieldnames)
    if "name" not in reader.fieldnames:
        log.warning(
            "CSV missing 'name' column — all rows will be skipped. Found: %s",
            reader.fieldnames,
        )

    total = emitted = skipped = 0
    try:
        for row_num, row in enumerate(reader, start=2):  # +1 for header, +1 for 1-index
            total += 1
            name = (row.get("name") or "").strip()
            if not name:
                skipped += 1
                log.debug("row %d: skipped (no name)", row_num)
                continue
            try:
                record = {
                    "business": name,
                    "city": (row.get("city") or "").strip(),
                    "state": (row.get("us_state") or row.get("state") or "").strip(),
                    "phone": (row.get("phone") or "").strip(),
                    "address": (row.get("full_address") or "").strip(),
                }
                print(json.dumps(record), flush=True)
                emitted += 1
            except (TypeError, ValueError) as exc:
                skipped += 1
                log.warning("row %d: %s", row_num, exc)
    except UnicodeDecodeError as exc:
        log.error("encoding error (file is not valid UTF-8): %s", exc)
        sys.exit(1)
    except csv.Error as exc:
        log.error("CSV parse error at row ~%d: %s", total + 1, exc)
        sys.exit(1)
    finally:
        if src is not sys.stdin:
            src.close()

    log.info("converted %d rows (%d emitted, %d skipped)", total, emitted, skipped)
    if total > 0 and emitted == 0:
        log.error("no rows emitted — check CSV has a 'name' column with values")
        sys.exit(2)


if __name__ == "__main__":
    main()
