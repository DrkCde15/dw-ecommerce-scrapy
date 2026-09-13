"""
Spider para Mercado Livre (raspagem HTML).

Coleta dados de produtos via busca no site:
- https://lista.mercadolivre.com.br/{query}

LIMITACAO: ML requer autenticacao (login) para buscas.
O spider detecta o redirect para verificacao e para com warning.
Para producao, considere: autenticacao via API OAuth ou proxy com sessao autenticada.
"""

import re
from datetime import datetime, timezone

import scrapy


def clean_price(value):
    """Converte preco para float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("R$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def clean_integer(value):
    """Extrai inteiro de strings."""
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        digits = re.sub(r"[^\d]", "", value)
        try:
            return int(digits)
        except ValueError:
            return None
    return None


class MercadoLivreItem(scrapy.Item):
    product_id = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    original_price = scrapy.Field()
    discount_percentage = scrapy.Field()
    condition = scrapy.Field()
    sold_quantity = scrapy.Field()
    category = scrapy.Field()
    brand = scrapy.Field()
    permalink = scrapy.Field()
    thumbnail = scrapy.Field()
    shipping_free = scrapy.Field()
    source = scrapy.Field()
    scraped_at = scrapy.Field()


class MercadoLivreSpider(scrapy.Spider):
    """
    Spider para Mercado Livre (raspagem HTML).

    Uso:
        scrapy crawl mercadolivre -a query="iphone 15" -a limit=50

    NOTA IMPORTANTE:
    ML requer autenticacao (login) para buscas publicas.
    Sem autenticacao, redireciona para account-verification.
    Em producao, use OAuth2 API ou proxy com sessao autenticada.
    """

    name = "mercadolivre"
    allowed_domains = ["lista.mercadolivre.com.br", "mercadolivre.com.br", "www.mercadolivre.com.br"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 200,
        },
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 4,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, query="celular", limit=50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.limit = int(limit)
        self.items_count = 0
        self.start_urls = [f"https://lista.mercadolivre.com.br/{self.query}"]

    def parse(self, response):
        if "account-verification" in response.url:
            self.logger.warning(
                f"ML redirecionou para verificacao: {response.url}. "
                "Buscas requerem autenticacao (login)."
            )
            return

        results = response.css("li.ui-search-layout__item")
        if not results:
            results = response.css("div.ui-search-result")

        self.logger.info(f"Encontrados {len(results)} itens na pagina")

        for element in results:
            if self.items_count >= self.limit:
                return

            item = {}

            item["permalink"] = (
                element.css("a.ui-search-link::attr(href)").get()
                or element.css("a.ui-search-item__group__element::attr(href)").get()
                or ""
            )

            item["name"] = (
                element.css("h2.ui-search-item__title::text").get()
                or element.css("h3.ui-search-item__title::text").get()
                or ""
            )

            price_text = element.css("span.andes-money-amount__fraction::text").get() or ""
            item["price"] = clean_price(price_text) if price_text else None

            orig_text = (
                element.css("del span.andes-money-amount__fraction::text").get()
                or element.css("span.ui-search-price__original-value span.andes-money-amount__fraction::text").get()
                or ""
            )
            item["original_price"] = clean_price(orig_text) if orig_text else None

            attrs = element.css("span.ui-search-item__group__attributes::text").getall()
            item["condition"] = attrs[0] if attrs else ""

            sold_text = ""
            for a in attrs:
                if "vendido" in a.lower() or "venda" in a.lower():
                    sold_text = a
                    break
            item["sold_quantity"] = clean_integer(sold_text) if sold_text else None

            shipping_text = element.css("span.ui-search-item__shipping--free::text").get() or ""
            item["shipping_free"] = "Frete" in shipping_text and "grátis" in shipping_text.lower()

            link = item.get("permalink", "")
            id_match = re.search(r"(MLB-?\d+)", link)
            item["product_id"] = id_match.group(1).replace("-", "") if id_match else ""

            item["source"] = "mercadolivre"
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            if item["original_price"] and item["price"] and item["original_price"] > 0:
                item["discount_percentage"] = round((1 - item["price"] / item["original_price"]) * 100)

            if item.get("name"):
                self.items_count += 1
                yield item

        if self.items_count < self.limit:
            next_page = (
                response.css("a.andes-pagination__link[title='Seguinte']::attr(href)").get()
                or response.css("a[title='Seguinte']::attr(href)").get()
            )
            if next_page:
                yield response.follow(next_page, callback=self.parse)
