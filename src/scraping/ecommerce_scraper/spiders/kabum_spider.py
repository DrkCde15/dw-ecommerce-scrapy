"""
Spider para KaBuM via API interna.

Coleta dados de produtos via API JSON publica:
- https://servicespub.prod.api.aws.grupokabum.com.br/catalog/v2/products

A API retorna JSON com wrapper 'meta' + 'data[]'.
Paginacao via page_number/page_size ou links.next com cursor.
"""

import json
from datetime import datetime, timezone

import scrapy


class KabumItem(scrapy.Item):
    product_id = scrapy.Field()
    name = scrapy.Field()
    brand = scrapy.Field()
    price = scrapy.Field()
    old_price = scrapy.Field()
    discount_percentage = scrapy.Field()
    stock = scrapy.Field()
    category = scrapy.Field()
    image_url = scrapy.Field()
    url = scrapy.Field()
    seller = scrapy.Field()
    rating = scrapy.Field()
    reviews_count = scrapy.Field()
    source = scrapy.Field()
    scraped_at = scrapy.Field()


class KabumSpider(scrapy.Spider):
    """
    Spider para KaBuM (API interna).

    Uso:
        scrapy crawl kabum -a query="notebook" -a limit=100
        scrapy crawl kabum -a query="placa de video" -a limit=50
    """

    name = "kabum"
    allowed_domains = [
        "www.kabum.com.br",
        "kabum.com.br",
        "servicespub.prod.api.aws.grupokabum.com.br",
    ]

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 200,
        },
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS": 4,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, query="notebook", limit=100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.limit = int(limit)
        self.items_count = 0
        self.page_number = 1
        self.page_size = 100

        self._build_url()

    def _build_url(self):
        self.start_urls = [
            f"https://servicespub.prod.api.aws.grupokabum.com.br/catalog/v2/products"
            f"?query={self.query}&page_number={self.page_number}&page_size={self.page_size}"
        ]

    def parse(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Erro ao decodificar JSON: {response.url}")
            return

        meta = data.get("meta", {})
        products = data.get("data", [])

        if not products:
            self.logger.info("Nenhum produto encontrado")
            return

        total = meta.get("total_items_count", 0)
        if self.items_count == 0:
            self.logger.info(f"Total de itens disponiveis: {total}")

        self.logger.info(f"Processando {len(products)} produtos da pagina {self.page_number}")

        for product in products:
            if self.items_count >= self.limit:
                return

            attrs = product.get("attributes", {})
            prod_id = product.get("id", "")

            if not attrs.get("title"):
                continue

            item = KabumItem()

            item["product_id"] = str(prod_id)

            item["name"] = attrs.get("title", "")

            manufacturer = attrs.get("manufacturer", {})
            item["brand"] = manufacturer.get("name", "") if isinstance(manufacturer, dict) else ""

            item["price"] = attrs.get("price_with_discount", 0) or attrs.get("price", 0)

            item["old_price"] = attrs.get("old_price", 0)

            item["discount_percentage"] = attrs.get("discount_percentage", 0)

            item["stock"] = attrs.get("stock", 0)

            menu = attrs.get("menu", "")
            item["category"] = menu.split("/")[-1] if menu else ""

            images = attrs.get("images", [])
            item["image_url"] = images[0] if images else ""

            product_link = attrs.get("product_link", "")
            item["url"] = f"https://www.kabum.com.br/produto/{prod_id}" if product_link else ""

            marketplace = attrs.get("marketplace")
            if isinstance(marketplace, dict) and marketplace.get("seller_name"):
                item["seller"] = marketplace["seller_name"]
            else:
                item["seller"] = "KaBuM"

            item["rating"] = attrs.get("average_of_ratings", 0)

            item["reviews_count"] = attrs.get("number_of_ratings", 0)

            item["source"] = "kabum"
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            self.items_count += 1
            yield item

        if self.items_count < self.limit and len(products) >= self.page_size:
            self.page_number += 1
            self._build_url()
            yield scrapy.Request(
                self.start_urls[0],
                callback=self.parse,
            )
