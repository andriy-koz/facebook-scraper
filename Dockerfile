FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY csv2jsonl.py searxng_search.py fb_scrape.py jsonl2csv.py progress.py _log.py run.sh ./
RUN chmod +x run.sh

ENTRYPOINT ["./run.sh"]
