#!/bin/bash
set -e
cd /opt/project

PIPELINES='{"src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100, "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 200, "src.scraping.ecommerce_scraper.pipelines.PostgresPipeline": 400}'

for SPIDER in books amazon americanas kabum; do
  echo "=== Running $SPIDER ==="
  scrapy crawl "$SPIDER" \
    -a query=notebook \
    -a limit=50 \
    -s POSTGRES_TABLE="raw.$SPIDER" \
    -s POSTGRES_URL="postgresql://postgres:postgres@postgres:5432/ecommerce" \
    -s "ITEM_PIPELINES=$PIPELINES" \
    -s LOG_LEVEL=WARNING \
    2>&1 || echo "WARN: $SPIDER returned non-zero (may have warnings)"
  echo "=== $SPIDER done ==="
done

# Override books query
echo "=== Re-running books with query=all ==="
scrapy crawl books \
  -a query=all \
  -a limit=50 \
  -s POSTGRES_TABLE=raw.books \
  -s POSTGRES_URL="postgresql://postgres:postgres@postgres:5432/ecommerce" \
  -s "ITEM_PIPELINES=$PIPELINES" \
  -s LOG_LEVEL=WARNING \
  2>&1 || echo "WARN: books returned non-zero (may have warnings)"
echo "=== books done ==="
