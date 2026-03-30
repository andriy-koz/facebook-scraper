#!/bin/sh
# run.sh — Run the full enrichment pipeline with resume support.
#
# Usage: ./run.sh [--fresh] leads.csv
#
# Checkpoints to {base}.jsonl after each lead. If interrupted,
# run the same command again to resume where it left off.
# Auto-resets if the input CSV is newer than the checkpoint.
# Use --fresh to force a clean start.
# Final output: {base}_enriched.csv

set -e

# Load .env if present
DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$DIR/.env" ]; then
    set -a
    . "$DIR/.env"
    set +a
fi

# Use venv if available
if [ -x "$DIR/.venv/bin/python3" ]; then
    PY="$DIR/.venv/bin/python3"
else
    PY=python3
fi

fresh=0
if [ "$1" = "--fresh" ]; then
    fresh=1
    shift
fi

if [ -z "$1" ]; then
    echo "usage: run.sh [--fresh] <input.csv>" >&2
    exit 1
fi

input="$1"
base="${input%.csv}"
checkpoint="${base}.jsonl"
output="${base}_enriched.csv"

# Reset checkpoint if --fresh or input is newer
if [ -f "$checkpoint" ]; then
    if [ "$fresh" = 1 ]; then
        echo "Fresh start: removing $checkpoint" >&2
        rm "$checkpoint"
    elif [ "$input" -nt "$checkpoint" ]; then
        echo "Input changed: resetting checkpoint" >&2
        rm "$checkpoint"
    fi
fi

# Count total leads
total=$($PY csv2jsonl.py < "$input" | wc -l)

# Resume: count already-processed lines
done=0
if [ -f "$checkpoint" ]; then
    done=$(wc -l < "$checkpoint")
    echo "Resuming: $done/$total already done" >&2
fi

remaining=$((total - done))

# Status file for live dashboard updates
STATUS_FILE=$(mktemp)
export STATUS_FILE
trap 'rm -f "$STATUS_FILE"' EXIT

if [ "$remaining" -gt 0 ]; then
    echo "Processing $remaining of $total leads..." >&2
    $PY csv2jsonl.py < "$input" \
        | tail -n +$((done + 1)) \
        | $PY ddg_search.py \
        | $PY fb_scrape.py \
        | $PY progress.py --total "$total" --done "$done" \
        | tee -a "$checkpoint" > /dev/null
fi

# Convert full checkpoint to CSV
$PY jsonl2csv.py < "$checkpoint" > "$output"
echo "Done: $output ($total leads)" >&2
