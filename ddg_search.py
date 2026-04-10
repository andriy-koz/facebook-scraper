"""
DuckDuckGo HTML search — finds Facebook pages for businesses.
Uses the HTML endpoint with rotating user agents and optional proxy.
"""

import os
import random
import sys
import time
from urllib.parse import urlencode, urlparse, parse_qs, unquote

import requests
from lxml import html

PROXY_URL = os.environ.get("PROXY_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


def _status(msg):
    p = os.environ.get("STATUS_FILE")
    if p:
        try:
            with open(p, "w") as f:
                f.write(msg)
        except OSError:
            pass


_DDG_MSGS = [
    "Rolling through the web for {biz}...",
    "Hunting down {biz}'s Facebook page...",
    "Lighting up a search for {biz}...",
    "Puffing through results for {biz}...",
    "Blazing a trail to {biz}'s page...",
]


def _build_url(query, region="us-en", start="0", num_results="30"):
    params = urlencode({"q": query, "kl": region, "s": start, "dc": num_results})
    return f"https://duckduckgo.com/html/?{params}"


def _resolve_ddg_redirect(href):
    """Extract real URL from DuckDuckGo's //duckduckgo.com/l/?uddg= redirects."""
    if "uddg=" not in href:
        return href
    try:
        qs = parse_qs(urlparse(href).query)
        return unquote(qs["uddg"][0])
    except (KeyError, IndexError):
        return href


def _parse_results(page_html):
    tree = html.fromstring(page_html)
    results = []

    for el in tree.cssselect(".result, .web-result, .results_links"):
        link = el.cssselect(".result__title a, .result__a, a.result__url")
        if not link:
            continue
        link = link[0]

        title = link.text_content().strip()
        url = _resolve_ddg_redirect(link.get("href", ""))
        if not title or not url:
            continue

        snippet_el = el.cssselect(".result__snippet, .result__body")
        snippet = " ".join(snippet_el[0].text_content().split()) if snippet_el else ""

        results.append({"title": title, "url": url, "snippet": snippet})

    return results


def ddg_search(query, proxy=None, debug=False, retries=10, business=None):
    """Run a DuckDuckGo HTML search. Returns list of result dicts.

    Uses GET against the HTML endpoint. When a proxy is used, some IPs
    may trigger DDG's captcha (HTTP 202). The proxy rotates IPs on each
    new connection, so retrying usually gets a clean one.
    """
    url = _build_url(query)
    proxies = {"https": proxy, "http": proxy} if proxy else None
    status_msg = random.choice(_DDG_MSGS).format(biz=business) if business else None

    if debug:
        print(f"[ddg] query: {query}", file=sys.stderr)
        print(f"[ddg] url:   {url}", file=sys.stderr)
        print(f"[ddg] proxy: {proxy or 'direct'}", file=sys.stderr)

    for attempt in range(retries):
        if status_msg:
            _status(f"[DuckDuckGo] {status_msg} [Puff {attempt + 1}]")
        time.sleep(random.uniform(0.5, 2.0))
        headers = {**HEADERS_BASE, "User-Agent": random.choice(USER_AGENTS)}

        try:
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=30)
        except requests.RequestException as exc:
            print(f"[ddg] attempt {attempt + 1} error: {exc}", file=sys.stderr)
            continue

        if debug:
            print(
                f"[ddg] attempt {attempt + 1}: status {resp.status_code}, "
                f"size {len(resp.text)}",
                file=sys.stderr,
            )

        if resp.status_code == 200:
            results = _parse_results(resp.text)
            if results:
                return results

    return []


def find_facebook_page(business, city, state, proxy=PROXY_URL, debug=False):
    """Search DDG for a business's Facebook page. Returns URL or None."""
    query = f"{business} {city} {state} site:facebook.com"
    results = ddg_search(query, proxy=proxy, debug=debug, business=business)

    if debug:
        print(f"[ddg] {len(results)} results", file=sys.stderr)

    for r in results:
        url = r["url"]
        if "facebook.com" in url and "/posts/" not in url and "/photos/" not in url:
            if debug:
                print(f"[ddg] matched: {url}", file=sys.stderr)
            return url

    return None


if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Find Facebook page via DuckDuckGo")
    ap.add_argument("business", nargs="?", help="Business name")
    ap.add_argument("city", nargs="?", help="City")
    ap.add_argument("state", nargs="?", help="State")
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--raw", action="store_true", help="Print all results as JSON")
    ap.add_argument("--no-proxy", action="store_true", help="Disable proxy")
    args = ap.parse_args()

    proxy = None if args.no_proxy else PROXY_URL

    # JSONL pipe mode: reads {"business", "city", "state"} from stdin
    if not args.business:
        for line in sys.stdin:
            record = json.loads(line)
            fb_url = find_facebook_page(
                record["business"], record["city"], record["state"],
                proxy=proxy, debug=args.debug,
            )
            record["fb_url"] = fb_url
            print(json.dumps(record), flush=True)
    elif args.raw:
        query = f"{args.business} {args.city} {args.state} site:facebook.com"
        results = ddg_search(query, proxy=proxy, debug=args.debug)
        print(json.dumps(results, indent=2))
    else:
        fb_url = find_facebook_page(args.business, args.city, args.state, proxy=proxy, debug=args.debug)
        if fb_url:
            print(fb_url)
        else:
            print("No Facebook page found.", file=sys.stderr)
            sys.exit(1)
