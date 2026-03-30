# Facebook Lead Enricher

![screenshot](screenshot.png)

Enrich a CSV of business leads with Facebook page data (emails, Instagram, follower counts, ad status) using DuckDuckGo search and Apify.

## How it works

```
csv2jsonl.py < leads.csv | ddg_search.py | fb_scrape.py | jsonl2csv.py > enriched.csv
```

Each tool reads JSONL from stdin and writes JSONL to stdout. `run.sh` wraps the full pipeline with checkpointing and resume support.

## Setup (macOS)

### 1. Clone and enter the repo

```sh
git clone <repo-url>
cd facebook-scraper
```

### 2. Create a Python virtual environment

Requires Python 3.10+. macOS ships with Python 3 since Monterey. If you don't have it, install via Homebrew:

```sh
brew install python3
```

Then create the venv and install dependencies:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install requests lxml rich
```

### 3. Configure credentials

Copy the example env file and fill in your values:

```sh
cp .env.example .env
```

Edit `.env`:

```
APIFY_TOKEN=your_apify_token_here
PROXY_URL=http://user:pass@proxy.example.com:8080
```

- **APIFY_TOKEN** (required) — get one at [apify.com](https://apify.com) under Settings > Integrations > API Tokens
- **PROXY_URL** (optional) — a rotating proxy for DuckDuckGo searches. Without it, DDG may rate-limit you after a few dozen queries

### 4. Prepare your input CSV

The input CSV needs at minimum a `name` column. Supported columns:

| CSV column | Used as | Required |
|---|---|---|
| `name` | Business name | yes |
| `city` | City | yes |
| `us_state` or `state` | State | yes |
| `phone` | Phone number | no |
| `full_address` | Address | no |

This matches the export format from [Outscraper](https://outscraper.com).

### 5. Run

```sh
./run.sh leads.csv
```

Output goes to `leads_enriched.csv`.

If interrupted, run the same command again to resume where it left off. Use `--fresh` to start over:

```sh
./run.sh --fresh leads.csv
```

## Output fields

The enriched CSV includes all input fields plus:

| Field | Description |
|---|---|
| `fb_url` | Facebook page URL |
| `fb_email` | Email listed on the FB page |
| `fb_instagram` | Linked Instagram handle |
| `fb_followers` | Follower count |
| `fb_likes` | Like count |
| `fb_intro` | Page intro/about text |
| `fb_creation_date` | Page creation date |
| `fb_ad_status` | Whether the page runs ads |

## Running tools individually

Each script works standalone:

```sh
# Search DDG for a single business
./ddg_search.py "Joe's Coffee" "Tampa" "FL"

# Process a JSONL file through just the FB scraper
cat partial.jsonl | ./fb_scrape.py > scraped.jsonl

# Convert between formats
./csv2jsonl.py < leads.csv > leads.jsonl
cat results.jsonl | ./jsonl2csv.py > results.csv
```

## Troubleshooting

**`error: APIFY_TOKEN not set`** — your `.env` file is missing or doesn't have `APIFY_TOKEN`. Check that `.env` exists in the project root.

**DDG returning no results** — DuckDuckGo is rate-limiting you. Set `PROXY_URL` in `.env` to a rotating residential proxy, or wait and retry.

**Resuming gives wrong counts** — the checkpoint file (`leads.jsonl`) may be stale. Run with `--fresh` to reset.
