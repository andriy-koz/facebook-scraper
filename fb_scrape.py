#!/usr/bin/env python3
"""
fb_scrape — Scrape Facebook pages via Apify's Facebook Pages Scraper actor.

Reads JSONL with "fb_url" from stdin, calls Apify to scrape each page,
merges scraped fields into the record, writes JSONL to stdout.

Usage:
    ddg_search.py < input.jsonl | fb_scrape.py [-d]
    echo '{"business":"Test","fb_url":"https://www.facebook.com/PipeDreamzStPete/"}' | fb_scrape.py -d
"""

import json
import os
import random
import sys

import requests

APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
if not APIFY_TOKEN:
    print("error: APIFY_TOKEN not set — add it to .env or export it", file=sys.stderr)
    sys.exit(1)

APIFY_URL = (
    "https://api.apify.com/v2/acts/apify~facebook-pages-scraper"
    "/run-sync-get-dataset-items"
)

# Fields we care about from the Apify response
KEEP_FIELDS = [
    "email", "instagram", "followers", "likes",
    "intro", "creation_date", "ad_status",
]


def _status(msg):
    p = os.environ.get("STATUS_FILE")
    if p:
        try:
            with open(p, "w") as f:
                f.write(msg)
        except OSError:
            pass


_APIFY_MSGS = [
    "Extracting the good stuff from {biz}...",
    "Cracking open {biz}'s page for the premium data...",
    "Hitting {biz}'s page for the top-shelf info...",
    "Unwrapping the goods from {biz}...",
    "Getting a hit off {biz}'s Facebook page...",
]


def scrape_fb_page(fb_url, debug=False):
    """Call Apify actor for a single FB page URL. Returns dict or None."""
    payload = {"startUrls": [{"url": fb_url}]}

    if debug:
        print(f"[fb] scraping: {fb_url}", file=sys.stderr)

    try:
        resp = requests.post(
            APIFY_URL,
            params={"token": APIFY_TOKEN},
            json=payload,
            timeout=120,
        )
    except requests.RequestException as exc:
        print(f"[fb] error: {exc}", file=sys.stderr)
        return None

    if debug:
        print(f"[fb] status: {resp.status_code}", file=sys.stderr)

    if resp.status_code not in (200, 201):
        print(f"[fb] bad status {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
        return None

    items = resp.json()
    if not items:
        if debug:
            print("[fb] no items returned", file=sys.stderr)
        return None

    return items[0]


def main():
    debug = "-d" in sys.argv or "--debug" in sys.argv

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        record = json.loads(line)
        fb_url = record.get("fb_url")

        if not fb_url:
            print(json.dumps(record), flush=True)
            continue

        _status(f"[Apify] {random.choice(_APIFY_MSGS).format(biz=record.get('business', fb_url))}")
        data = scrape_fb_page(fb_url, debug=debug)

        if data:
            for field in KEEP_FIELDS:
                if field in data and data[field]:
                    record[f"fb_{field}"] = data[field]

            if debug:
                kept = [f for f in KEEP_FIELDS if f"fb_{f}" in record]
                print(f"[fb] got: {', '.join(kept)}", file=sys.stderr)

        print(json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
