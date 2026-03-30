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
import sys


FIELDS = [
    "business", "city", "state", "phone", "address",
    "fb_url", "fb_email", "fb_instagram", "fb_followers",
    "fb_likes", "fb_intro", "fb_creation_date", "fb_ad_status",
]


def main():
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        writer.writerow(rec)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
