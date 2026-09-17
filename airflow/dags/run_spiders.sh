#!/bin/bash
set -uo pipefail
cd /opt/project

PIPELINES='{"src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100, "src.scraping.ecommerce_scraper.pipelines.ValidationPipeline": 200, "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 300, "src.scraping.ecommerce_scraper.pipelines.FileExportPipeline": 400}'

MAX_RETRIES=3
RETRY_DELAY=5
FAILED_SPIDERS=""

for SPIDER in books amazon americanas kabum; do
  ATTEMPT=0
  SUCCESS=false

  while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "=== Running $SPIDER (attempt $ATTEMPT/$MAX_RETRIES) ==="

    if scrapy crawl "$SPIDER" \
      -a query=notebook \
      -a limit=50 \
      -s POSTGRES_TABLE="raw.$SPIDER" \
      -s POSTGRES_URL="postgresql://postgres:postgres@postgres:5432/ecommerce" \
      -s "ITEM_PIPELINES=$PIPELINES" \
      -s LOG_LEVEL=WARNING; then
      echo "=== $SPIDER SUCCESS ==="
      SUCCESS=true
      break
    else
      echo "=== $SPIDER FAILED (attempt $ATTEMPT) ==="
      if [ $ATTEMPT -lt $MAX_RETRIES ]; then
        BACKOFF=$((RETRY_DELAY * (2 ** (ATTEMPT - 1))))
        echo "Retrying $SPIDER in ${BACKOFF}s (exponential backoff)..."
        sleep $BACKOFF
      fi
    fi
  done

  if [ "$SUCCESS" = false ]; then
    echo "=== $SPIDER FAILED AFTER $MAX_RETRIES ATTEMPTS ==="
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
