#!/bin/sh
# run.sh — Run the full enrichment pipeline with resume support.
#
# Usage:
#   ./run.sh leads.csv          # first run
#   ./run.sh leads.csv          # resumes if interrupted
#   ./run.sh --fresh leads.csv  # force clean start
#   ./run.sh --check            # connectivity preflight (no pipeline run)
#
# Checkpoints to {base}.jsonl after each lead. Logs to {base}.log.
# Final output: {base}_enriched.csv

set -e

DIR="$(cd "$(dirname "$0")" && pwd)"

die()  { printf 'error: %s\n'   "$*" >&2; exit 1; }
warn() { printf 'warning: %s\n' "$*" >&2; }
info() { printf '%s\n'          "$*" >&2; }

# --- Load .env -----------------------------------------------------------
if [ -f "$DIR/.env" ]; then
    set -a
    . "$DIR/.env"
    set +a
else
    warn ".env not found at $DIR/.env (using shell environment only)"
fi

# --- Locate Python -------------------------------------------------------
if [ -x "$DIR/.venv/bin/python3" ]; then
    PY="$DIR/.venv/bin/python3"
else
    PY=$(command -v python3 || true)
    [ -n "$PY" ] || die "python3 not found and no venv at $DIR/.venv"
    warn "no venv at $DIR/.venv — falling back to system python3 ($PY)"
fi

# --- Verify pipeline scripts --------------------------------------------
for s in csv2jsonl.py ddg_search.py fb_scrape.py progress.py jsonl2csv.py _log.py; do
    [ -f "$DIR/$s" ] || die "missing pipeline file: $DIR/$s"
done

# --- Verify Python deps --------------------------------------------------
if ! "$PY" -c 'import requests, lxml, rich' 2>/dev/null; then
    die "missing Python deps — install with: $PY -m pip install requests lxml rich"
fi

# --- Verify env vars -----------------------------------------------------
[ -n "$APIFY_TOKEN" ] || die "APIFY_TOKEN not set — add it to $DIR/.env"
if [ -z "$PROXY_URL" ]; then
    warn "PROXY_URL not set — DuckDuckGo will rate-limit direct requests"
fi

# --- --check mode: connectivity test ------------------------------------
if [ "$1" = "--check" ]; then
    info "=== preflight check ==="
    info "python: $PY"
    info "deps:   ok"
    info "apify token: set (${#APIFY_TOKEN} chars)"
    if [ -n "$PROXY_URL" ]; then
        # Hide creds in echoed value
        masked=$(printf '%s' "$PROXY_URL" | sed -E 's#://[^@]*@#://***:***@#')
        info "proxy:  $masked"
    else
        info "proxy:  (not set)"
    fi

    info ""
    info "--- testing Apify token (api.apify.com/v2/users/me) ---"
    if command -v curl >/dev/null 2>&1; then
        code=$(curl -s -o /dev/null -w '%{http_code}' \
            "https://api.apify.com/v2/users/me?token=$APIFY_TOKEN" || echo 000)
        case "$code" in
            200) info "  OK (200)" ;;
            401) warn "  FAIL (401) — token invalid or revoked" ;;
            000) warn "  FAIL — could not reach api.apify.com" ;;
            *)   warn "  FAIL (HTTP $code)" ;;
        esac
    else
        warn "  curl not installed — skipping HTTP check"
    fi

    info ""
    info "--- testing DuckDuckGo via proxy ---"
    LOG_LEVEL=DEBUG "$PY" "$DIR/ddg_search.py" "Apple Inc" "Cupertino" "CA" \
        >/dev/null 2>&1 && info "  OK (found Facebook page)" \
        || warn "  FAIL — re-run with: LOG_LEVEL=DEBUG $PY ddg_search.py 'Apple Inc' Cupertino CA"

    info ""
    info "preflight done"
    exit 0
fi

# --- Parse args ----------------------------------------------------------
fresh=0
if [ "$1" = "--fresh" ]; then
    fresh=1
    shift
fi

[ -n "$1" ] || die "usage: run.sh [--fresh] <input.csv>  (or --check)"

input="$1"
[ -f "$input" ] || die "input file not found: $input"
[ -r "$input" ] || die "cannot read input file: $input"

base="${input%.csv}"
checkpoint="${base}.jsonl"
output="${base}_enriched.csv"
LOG_FILE="${base}.log"
export LOG_FILE

# --- Reset checkpoint if --fresh or input is newer ----------------------
if [ -f "$checkpoint" ]; then
    if [ "$fresh" = 1 ]; then
        info "fresh start: removing $checkpoint"
        rm "$checkpoint"
    elif [ "$input" -nt "$checkpoint" ]; then
        info "input changed: resetting checkpoint"
        rm "$checkpoint"
    fi
fi

# --- Count total leads (silent — csv2jsonl logs to LOG_FILE) ------------
total=$(LOG_LEVEL=WARNING "$PY" "$DIR/csv2jsonl.py" < "$input" 2>/dev/null | wc -l | tr -d ' ')
[ -n "$total" ] && [ "$total" -gt 0 ] || die "no leads parsed from $input — see $LOG_FILE"

# --- Resume: count already-processed lines -------------------------------
done=0
if [ -f "$checkpoint" ]; then
    done=$(wc -l < "$checkpoint" | tr -d ' ')
    info "resuming: $done/$total already done"
fi
remaining=$((total - done))

# --- Status file for live dashboard --------------------------------------
STATUS_FILE=$(mktemp)
export STATUS_FILE
trap 'rm -f "$STATUS_FILE"' EXIT INT TERM

# --- Log run header to LOG_FILE so it's always captured ------------------
{
    printf '\n=== pipeline run: %s ===\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf 'input:     %s\n' "$input"
    printf 'total:     %d\n' "$total"
    printf 'resuming:  %d\n' "$done"
    printf 'remaining: %d\n' "$remaining"
    printf 'proxy:     %s\n' "${PROXY_URL:+set}${PROXY_URL:-unset}"
    printf 'apify:     set\n'
    printf 'python:    %s\n\n' "$PY"
} >> "$LOG_FILE"

info "logs: $LOG_FILE  (tail -f to watch)"

# --- Run pipeline --------------------------------------------------------
# Enable pipefail where available so a failed stage trips set -e
( set -o pipefail ) 2>/dev/null && set -o pipefail

if [ "$remaining" -gt 0 ]; then
    info "processing $remaining of $total leads..."
    if ! "$PY" "$DIR/csv2jsonl.py" < "$input" \
        | tail -n +$((done + 1)) \
        | "$PY" "$DIR/ddg_search.py" \
        | "$PY" "$DIR/fb_scrape.py" \
        | "$PY" "$DIR/progress.py" --total "$total" --done "$done" \
        | tee -a "$checkpoint" > /dev/null
    then
        warn "pipeline exited with error — see $LOG_FILE"
    fi
fi

# --- Convert checkpoint to CSV -------------------------------------------
if ! "$PY" "$DIR/jsonl2csv.py" < "$checkpoint" > "$output"; then
    die "jsonl2csv failed — see $LOG_FILE"
fi

info "done: $output ($total leads, log: $LOG_FILE)"
