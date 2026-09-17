#!/bin/bash
set -uo pipefail
cd /opt/project

PIPELINES='{"src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100, "src.scraping.ecommerce_scraper.pipelines.ValidationPipeline": 200, "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 300, "src.scraping.ecommerce_scraper.pipelines.PostgresPipeline": 400}'

FAILED_SPIDERS=""

for SPIDER in books amazon americanas kabum; do
  echo "=== Running $SPIDER ==="
  if scrapy crawl "$SPIDER" \
    -a query=notebook \
    -a limit=50 \
    -s POSTGRES_TABLE="raw.$SPIDER" \
    -s POSTGRES_URL="postgresql://postgres:postgres@postgres:5432/ecommerce" \
    -s "ITEM_PIPELINES=$PIPELINES" \
    -s LOG_LEVEL=WARNING; then
    echo "=== $SPIDER SUCCESS ==="
  else
    echo "=== $SPIDER FAILED ==="
    FAILED_SPIDERS="$FAILED_SPIDERS $SPIDER"
  fi
done

if [ -n "$FAILED_SPIDERS" ]; then
  echo ""
  echo "FINAL RESULT:"
  echo "FAILED SPIDERS:$FAILED_SPIDERS"
  echo ""
  exit 1
fi

echo ""
echo "FINAL RESULT: ALL SPIDERS SUCCESS"
exit 0
