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
import sys


def main():
    src = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    reader = csv.DictReader(src)

    for row in reader:
        name = (row.get("name") or "").strip()
        city = (row.get("city") or "").strip()
        state = (row.get("us_state") or row.get("state") or "").strip()

        if not name:
            continue

        print(json.dumps({
            "business": name,
            "city": city,
            "state": state,
            "phone": (row.get("phone") or "").strip(),
            "address": (row.get("full_address") or "").strip(),
        }), flush=True)


if __name__ == "__main__":
    main()
