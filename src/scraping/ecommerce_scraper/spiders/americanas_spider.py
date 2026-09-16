"""
Spider para Americanas.com.br via VTEX API.

Coleta dados de produtos via API JSON publica da VTEX:
- https://www.americanas.com.br/api/catalog_system/pub/products/search/{query}

A API retorna JSON direto, sem necessidade de parsing HTML.
Paginacao via header 'resources: 0-9/18703' e parametros _from/_to.
"""

import json
from datetime import datetime, timezone

import scrapy


class AmericanasItem(scrapy.Item):
    product_id = scrapy.Field()
    name = scrapy.Field()
    brand = scrapy.Field()
    price = scrapy.Field()
    list_price = scrapy.Field()
    available_quantity = scrapy.Field()
    category = scrapy.Field()
    image_url = scrapy.Field()
    url = scrapy.Field()
    seller = scrapy.Field()
    source = scrapy.Field()
    scraped_at = scrapy.Field()


class AmericanasSpider(scrapy.Spider):
    """
    Spider para Americanas.com.br (VTEX API).

    Uso:
        scrapy crawl americanas -a query="notebook" -a limit=100
        scrapy crawl americanas -a query="celular" -a limit=50
    """

    name = "americanas"
    allowed_domains = ["www.americanas.com.br", "americanas.com.br"]

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
        self.page_size = 10
        self.current_from = 0
        self.total_items = None

        self._build_url()

    def _build_url(self):
        current_to = self.current_from + self.page_size - 1
        self.start_urls = [
            f"https://www.americanas.com.br/api/catalog_system/pub/products/search/{self.query}"
            f"?_from={self.current_from}&_to={current_to}"
        ]

    def parse(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Erro ao decodificar JSON: {response.url}")
            return

        if not isinstance(data, list) or len(data) == 0:
            self.logger.info("Nenhum produto encontrado")
            return

        if self.total_items is None:
            resources = response.headers.get("resources", b"").decode()
            if "/" in resources:
                self.total_items = int(resources.split("/")[1])
                self.logger.info(f"Total de itens disponiveis: {self.total_items}")

        self.logger.info(f"Processando {len(data)} produtos da pagina")

        for product in data:
            if self.items_count >= self.limit:
                return

            item = AmericanasItem()

            item["product_id"] = str(product.get("productId", ""))

            item["name"] = product.get("productName", "")

            item["brand"] = product.get("brand", "")

            categories = product.get("categories", [])
            item["category"] = categories[0].strip("/") if categories else ""

            link_text = product.get("linkText", "")
            item["url"] = f"https://www.americanas.com.br/{link_text}/p" if link_text else ""

            items_list = product.get("items", [])
            if items_list:
                first_item = items_list[0]

                images = first_item.get("images", [])
                if images:
                    item["image_url"] = images[0].get("imageUrl", "")
                else:
                    item["image_url"] = ""

                sellers = first_item.get("sellers", [])
                if sellers:
                    default_seller = next(
                        (s for s in sellers if s.get("sellerDefault")), sellers[0]
                    )
                    offer = default_seller.get("commertialOffer", {})

                    item["price"] = offer.get("Price", 0)
                    item["list_price"] = offer.get("ListPrice", 0)
                    item["available_quantity"] = offer.get("AvailableQuantity", 0)
                    item["seller"] = default_seller.get("sellerName", "")
                else:
                    item["price"] = 0
                    item["list_price"] = 0
                    item["available_quantity"] = 0
                    item["seller"] = ""
            else:
                item["image_url"] = ""
                item["price"] = 0
                item["list_price"] = 0
                item["available_quantity"] = 0
                item["seller"] = ""

            item["source"] = "americanas"
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            if item.get("name") and item.get("product_id"):
                self.items_count += 1
                yield item

        if self.items_count < self.limit and len(data) >= self.page_size:
            self.current_from += self.page_size
            self._build_url()
            yield scrapy.Request(
                self.start_urls[0],
                callback=self.parse,
            )
