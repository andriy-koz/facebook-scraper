#!/bin/sh
# scrape.sh — One-command wrapper for the full pipeline.
#
# Usage:
#   ./scrape.sh leads.csv          # run (builds image on first use)
#   ./scrape.sh leads.csv --fresh  # force clean start
#   ./scrape.sh --check            # test connectivity
#
# Setup: copy env.example to .env and fill in your credentials.

set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="fb-scraper"

die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n'        "$*" >&2; }

# --- Check Docker -----------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    die "Docker not found — run: ./setup.sh"
fi
if ! docker info >/dev/null 2>&1; then
    die "Docker is not running — run: sudo systemctl start docker"
fi

# --- Check .env -------------------------------------------------------------
[ -f "$DIR/.env" ] || die ".env not found — copy env.example to .env and add your credentials"

# --- Build image if missing -------------------------------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    info "First run — building image (this takes ~30 seconds)..."
    docker build -t "$IMAGE" "$DIR" >&2
    info "Done."
fi

# --- Handle --check (no file needed) ----------------------------------------
if [ "$1" = "--check" ]; then
    exec docker run --rm --network host --env-file "$DIR/.env" "$IMAGE" --check
fi

# --- Parse args: support both "file --fresh" and "--fresh file" -------------
fresh=""
input=""
for arg in "$@"; do
    case "$arg" in
        --fresh) fresh="--fresh" ;;
        *)       input="$arg" ;;
    esac
done

[ -n "$input" ] || die "usage: ./scrape.sh <leads.csv>  (or --check)"
[ -f "$input" ] || die "file not found: $input"

# Resolve to absolute path for the bind mount
input="$(cd "$(dirname "$input")" && pwd)/$(basename "$input")"
data_dir="$(dirname "$input")"
filename="$(basename "$input")"

# --- Check for already-running container ------------------------------------
CONTAINER="fb-scraper-run"
if docker ps -q -f "name=$CONTAINER" | grep -q .; then
    die "Already running. Use 'docker stop $CONTAINER' to cancel it first."
fi

# --- Run --------------------------------------------------------------------
tty_flag=""
[ -t 1 ] && tty_flag="-t"

exec docker run --rm $tty_flag \
    --name "$CONTAINER" \
    --network host \
    --env-file "$DIR/.env" \
    -v "$data_dir:/data" \
    -w /data \
    "$IMAGE" $fresh "$filename"
