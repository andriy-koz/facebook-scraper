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
import logging
import os
import random
import signal
import sys

import requests

import _log

log = logging.getLogger("fb_scrape")

APIFY_TOKEN = os.environ.get("APIFY_TOKEN")

APIFY_URL = (
    "https://api.apify.com/v2/acts/apify~facebook-pages-scraper"
    "/run-sync-get-dataset-items"
)

# Fields we care about from the Apify response
KEEP_FIELDS = [
    "email", "instagram", "followers", "likes",
    "intro", "creation_date", "ad_status",
]

if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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


def scrape_fb_page(fb_url):
    """Call Apify actor for a single FB page URL. Returns dict or None."""
    payload = {"startUrls": [{"url": fb_url}]}

    log.debug("scraping: %s", fb_url)

    try:
        resp = requests.post(
            APIFY_URL,
            params={"token": APIFY_TOKEN},
            json=payload,
            timeout=180,
        )
    except requests.exceptions.ConnectTimeout as exc:
        log.error("apify connect timeout for %s — %s", fb_url, exc)
        return None
    except requests.exceptions.ReadTimeout:
        log.error("apify read timeout for %s — actor may be stuck (>180s)", fb_url)
        return None
    except requests.exceptions.SSLError as exc:
        log.error("apify SSL error — %s", exc)
        return None
    except requests.exceptions.ConnectionError as exc:
        log.error("apify network error for %s — %s", fb_url, exc)
        log.error("  check internet connectivity to api.apify.com")
        return None
    except requests.exceptions.RequestException as exc:
        log.error("apify request error for %s — %s: %s",
                  fb_url, type(exc).__name__, exc)
        return None
    except Exception:
        log.exception("apify unexpected error for %s", fb_url)
        return None

    status = resp.status_code
    log.debug("status: %d", status)

    if status == 401:
        log.error("apify auth failed (401) — APIFY_TOKEN invalid or revoked")
        log.error("  body: %s", resp.text[:200])
        return None
    if status == 402:
        log.error("apify payment required (402) — account credits exhausted")
        return None
    if status == 403:
        log.error("apify forbidden (403) — token lacks access to actor")
        return None
    if status == 404:
        log.error("apify actor not found (404) — actor renamed or removed?")
        return None
    if status == 429:
        log.error("apify rate-limited (429) for %s", fb_url)
        return None
    if 500 <= status < 600:
        log.error("apify server error (%d) for %s — %s",
                  status, fb_url, resp.text[:200])
        return None
    if status not in (200, 201):
        log.error("apify unexpected status %d for %s — %s",
                  status, fb_url, resp.text[:200])
        return None

    try:
        items = resp.json()
    except ValueError as exc:
        log.error("apify response not JSON for %s — %s", fb_url, exc)
        log.debug("  body: %s", resp.text[:500])
        return None

    if not isinstance(items, list):
        log.error("apify returned non-list response for %s — %s", fb_url, type(items).__name__)
        return None
    if not items:
        log.warning("apify returned 0 items for %s (page private/removed?)", fb_url)
        return None

    item = items[0]
    if not isinstance(item, dict):
        log.error("apify item not a dict for %s — %s", fb_url, type(item).__name__)
        return None

    return item


def main():
    debug = "-d" in sys.argv or "--debug" in sys.argv
    _log.setup("fb_scrape", debug=debug)

    if not APIFY_TOKEN:
        log.error("APIFY_TOKEN not set — add it to .env or export it")
        sys.exit(1)
    log.debug("APIFY_TOKEN set (%d chars)", len(APIFY_TOKEN))

    total = scraped = no_url = parse_errors = scrape_errors = 0

    for line_num, line in enumerate(sys.stdin, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            parse_errors += 1
            log.error("line %d: JSON decode error — %s", line_num, exc)
            continue

        total += 1
        fb_url = record.get("fb_url")

        if not fb_url:
            no_url += 1
            print(json.dumps(record), flush=True)
            continue

        _status(f"[Apify] {random.choice(_APIFY_MSGS).format(biz=record.get('business', fb_url))}")

        try:
            data = scrape_fb_page(fb_url)
        except Exception:
            scrape_errors += 1
            log.exception("line %d: unexpected error scraping %s", line_num, fb_url)
            data = None

        if data:
            kept = []
            for field in KEEP_FIELDS:
                if data.get(field):
                    record[f"fb_{field}"] = data[field]
                    kept.append(field)
            if kept:
                scraped += 1
                log.debug("line %d: got %s", line_num, ", ".join(kept))
            else:
                log.debug("line %d: apify returned item but no useful fields", line_num)
        else:
            scrape_errors += 1

        print(json.dumps(record), flush=True)

    log.info(
        "done: %d total, %d scraped, %d no-url, %d parse errors, %d scrape errors",
        total, scraped, no_url, parse_errors, scrape_errors,
    )


if __name__ == "__main__":
    main()
