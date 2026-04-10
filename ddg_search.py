"""
DuckDuckGo HTML search — finds Facebook pages for businesses.
Uses the HTML endpoint with rotating user agents and optional proxy.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from urllib.parse import urlencode, urlparse, parse_qs, unquote

import requests
from lxml import etree, html

import _log

log = logging.getLogger("ddg")

PROXY_URL = os.environ.get("PROXY_URL")

if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

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


def _mask_url(url):
    """Mask proxy credentials so they don't leak into logs."""
    if not url:
        return "(none)"
    try:
        p = urlparse(url)
        if p.username or p.password:
            port = f":{p.port}" if p.port else ""
            return f"{p.scheme}://***:***@{p.hostname}{port}"
        return url
    except Exception:
        return "(unparseable)"


def _validate_proxy(url):
    """Sanity-check PROXY_URL format. Returns (ok, reason)."""
    if not url:
        return False, "not set"
    try:
        p = urlparse(url)
    except Exception as exc:
        return False, f"parse error: {exc}"
    if p.scheme not in ("http", "https", "socks5", "socks5h", "socks4"):
        return False, f"unexpected scheme {p.scheme!r} (want http/https/socks)"
    if not p.hostname:
        return False, "missing hostname"
    if not p.port:
        return False, "missing port"
    return True, "ok"


def _preflight(proxy):
    if proxy:
        log.info("proxy: %s", _mask_url(proxy))
        ok, reason = _validate_proxy(proxy)
        if not ok:
            log.error("PROXY_URL looks invalid: %s", reason)
            log.error("  searches will probably fail — check .env PROXY_URL format")
            log.error("  expected: http://user:pass@host:port")
    else:
        log.warning("PROXY_URL not set — direct DDG requests will likely be captcha'd")


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


def _do_request(url, proxy, attempt):
    """One HTTP attempt. Returns Response or None (and logs the reason)."""
    proxies = {"https": proxy, "http": proxy} if proxy else None
    headers = {**HEADERS_BASE, "User-Agent": random.choice(USER_AGENTS)}

    try:
        return requests.get(url, headers=headers, proxies=proxies, timeout=30)
    except requests.exceptions.ProxyError as exc:
        log.error("attempt %d: proxy error — %s", attempt, exc)
        log.error("  check PROXY_URL creds and that the proxy host is reachable")
    except requests.exceptions.SSLError as exc:
        log.error("attempt %d: SSL error — %s", attempt, exc)
        log.error("  cert issue or MITM proxy intercepting TLS; try a different proxy")
    except requests.exceptions.ConnectTimeout as exc:
        log.error("attempt %d: connect timeout (30s) — %s", attempt, exc)
        log.error("  proxy or DDG unreachable; check firewall/VPN/network")
    except requests.exceptions.ReadTimeout as exc:
        log.error("attempt %d: read timeout (30s) — %s", attempt, exc)
    except requests.exceptions.ConnectionError as exc:
        log.error("attempt %d: connection error — %s", attempt, exc)
        log.error("  network or DNS failure; verify internet access")
    except requests.exceptions.TooManyRedirects as exc:
        log.error("attempt %d: too many redirects — %s", attempt, exc)
    except requests.exceptions.RequestException as exc:
        log.error("attempt %d: %s — %s", attempt, type(exc).__name__, exc)
    except Exception as exc:
        log.exception("attempt %d: unexpected error: %s", attempt, exc)
    return None


def ddg_search(query, proxy=None, retries=10, business=None):
    """Run a DuckDuckGo HTML search. Returns list of result dicts.

    When a proxy is used, DDG may still captcha some IPs (HTTP 202).
    The proxy rotates IPs per connection, so retrying usually lands on a
    clean one. Each failure is classified in the logs.
    """
    url = _build_url(query)
    status_msg = random.choice(_DDG_MSGS).format(biz=business) if business else None

    log.debug("query: %s", query)

    last_status = None
    for attempt in range(1, retries + 1):
        if status_msg:
            _status(f"[DuckDuckGo] {status_msg} [Puff {attempt}]")
        time.sleep(random.uniform(0.5, 2.0))

        resp = _do_request(url, proxy, attempt)
        if resp is None:
            continue

        last_status = resp.status_code
        log.debug("attempt %d: status %d, size %d",
                  attempt, resp.status_code, len(resp.text))

        if resp.status_code == 200:
            try:
                results = _parse_results(resp.text)
            except (etree.XMLSyntaxError, etree.ParserError) as exc:
                log.error("attempt %d: HTML parse error — %s", attempt, exc)
                continue
            except Exception:
                log.exception("attempt %d: unexpected parse error", attempt)
                continue
            if results:
                return results
            log.debug("attempt %d: 200 but 0 parsed results", attempt)
            continue

        if resp.status_code == 202:
            log.debug("attempt %d: DDG captcha (202) — rotating IP", attempt)
            continue
        if resp.status_code == 407:
            log.error("attempt %d: proxy auth required (407) — PROXY_URL creds wrong", attempt)
            continue
        if resp.status_code == 429:
            log.warning("attempt %d: rate-limited (429) — backing off 5s", attempt)
            time.sleep(5)
            continue
        if 500 <= resp.status_code < 600:
            log.warning("attempt %d: DDG server error (%d)", attempt, resp.status_code)
            continue
        log.error("attempt %d: unexpected HTTP %d — body: %s",
                  attempt, resp.status_code, resp.text[:200])

    log.error("search failed after %d attempts (last status: %s) — query: %s",
              retries, last_status, query)
    return []


def find_facebook_page(business, city, state, proxy=PROXY_URL):
    """Search DDG for a business's Facebook page. Returns URL or None."""
    query = f"{business} {city} {state} site:facebook.com"
    results = ddg_search(query, proxy=proxy, business=business)

    log.debug("%d results for %s", len(results), business)

    for r in results:
        url = r["url"]
        if "facebook.com" in url and "/posts/" not in url and "/photos/" not in url:
            log.debug("matched: %s", url)
            return url

    return None


def _stdin_mode(proxy):
    """Stream mode: JSONL in, enriched JSONL out."""
    total = found = parse_errors = search_errors = missing = 0

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
        business = (record.get("business") or "").strip()
        city = (record.get("city") or "").strip()
        state = (record.get("state") or "").strip()

        if not business:
            missing += 1
            log.warning("line %d: missing 'business' field — passing through", line_num)
            record["fb_url"] = None
            print(json.dumps(record), flush=True)
            continue

        try:
            fb_url = find_facebook_page(business, city, state, proxy=proxy)
        except Exception:
            search_errors += 1
            log.exception("line %d: unexpected search failure for %s", line_num, business)
            fb_url = None

        record["fb_url"] = fb_url
        if fb_url:
            found += 1
        print(json.dumps(record), flush=True)

    log.info(
        "done: %d processed, %d found, %d missing business, "
        "%d parse errors, %d search errors",
        total, found, missing, parse_errors, search_errors,
    )


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Find Facebook page via DuckDuckGo")
    ap.add_argument("business", nargs="?", help="Business name")
    ap.add_argument("city", nargs="?", help="City")
    ap.add_argument("state", nargs="?", help="State")
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--raw", action="store_true", help="Print all results as JSON")
    ap.add_argument("--no-proxy", action="store_true", help="Disable proxy")
    args = ap.parse_args()

    _log.setup("ddg", debug=args.debug)
    proxy = None if args.no_proxy else PROXY_URL
    _preflight(proxy)

    # JSONL pipe mode
    if not args.business:
        _stdin_mode(proxy)
        return

    if args.raw:
        query = f"{args.business} {args.city} {args.state} site:facebook.com"
        results = ddg_search(query, proxy=proxy, business=args.business)
        print(json.dumps(results, indent=2))
        return

    fb_url = find_facebook_page(args.business, args.city, args.state, proxy=proxy)
    if fb_url:
        print(fb_url)
    else:
        log.error("no Facebook page found for %s / %s / %s",
                  args.business, args.city, args.state)
        sys.exit(1)


if __name__ == "__main__":
    main()
