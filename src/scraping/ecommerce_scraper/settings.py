"""
Configurações do Scrapy para o projeto de data warehouse ecommerce.

Configure aqui os parâmetros de scraping, middlewares, pipelines e exportação.
"""

BOT_NAME = "ecommerce_scraper"

SPIDER_MODULES = ["src.scraping.ecommerce_scraper.spiders"]
NEWSPIDER_MODULE = "src.scraping.ecommerce_scraper.spiders"

# Crawl responsibly
ROBOTSTXT_OBEY = True

# Concorrencia e performance
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
DOWNLOAD_TIMEOUT = 30

# Retry
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# User-Agent (rotaciona entre opcoes realistas)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Middlewares
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

# Pipelines - ordem de prioridade
ITEM_PIPELINES = {
    "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
    "src.scraping.ecommerce_scraper.pipelines.ValidationPipeline": 200,
    "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 300,
    "src.scraping.ecommerce_scraper.pipelines.FileExportPipeline": 400,
}

# Exportação para arquivos (ETL: Extract escreve em staging local)
FEED_EXPORT_ENCODING = "utf-8"
JSON_OUTPUT_DIR = "data/raw"
PARQUET_OUTPUT_DIR = "data/raw"
FEED_FORMAT = "json"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# Desabilitar cookies (evita rastreamento)
COOKIES_ENABLED = False

# Headers realistas
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Cache (opcional - descomente para desenvolvimento)
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 3600
# HTTPCACHE_DIR = "httpcache"

# Desabilitar於ns em producao
TELNETCONSOLE_ENABLED = False

# Profiling
STATS_CLASS = "scrapy.statscollectors.MemoryStatsCollector"
