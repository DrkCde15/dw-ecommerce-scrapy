"""
Spider para Amazon.com.br

Coleta dados de produtos via HTML scraping.
Nota: Amazon tem protecao anti-bot, use com moderação e respeite robots.txt.
"""

import re
from datetime import datetime, timezone

import scrapy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst


def clean_price(value: str) -> float | None:
    """Extrai preco de strings como 'R$ 1.299,00'."""
    if not value:
        return None
    cleaned = re.sub(r"[^\d.,]", "", value.strip())
    if not cleaned:
        return None
    # Formato BR: 1.299,00 -> remove pontos, troca virgula por ponto
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_rating(value: str) -> float | None:
    """Extrai rating de strings como '4,5 de 5 estrelas'."""
    if not value:
        return None
    match = re.search(r"(\d+[.,]?\d*)", value)
    if match:
        try:
            return float(match.group(1).replace(",", "."))
        except ValueError:
            return None
    return None


def clean_reviews_count(value: str) -> int | None:
    """Extrai numero de reviews de strings como '1.234 evaluations'."""
    if not value:
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    try:
        return int(cleaned)
    except ValueError:
        return None


class AmazonProductItem(scrapy.Item):
    product_id = scrapy.Field(output_processor=TakeFirst())
    name = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(
        input_processor=MapCompose(clean_price),
        output_processor=TakeFirst(),
    )
    original_price = scrapy.Field(
        input_processor=MapCompose(clean_price),
        output_processor=TakeFirst(),
    )
    rating = scrapy.Field(
        input_processor=MapCompose(clean_rating),
        output_processor=TakeFirst(),
    )
    reviews_count = scrapy.Field(
        input_processor=MapCompose(clean_reviews_count),
        output_processor=TakeFirst(),
    )
    category = scrapy.Field(output_processor=TakeFirst())
    brand = scrapy.Field(output_processor=TakeFirst())
    image_url = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())
    source = scrapy.Field(output_processor=TakeFirst())
    scraped_at = scrapy.Field(output_processor=TakeFirst())


class AmazonSpider(scrapy.Spider):
    """
    Spider para Amazon.com.br.

    Uso:
        scrapy crawl amazon -a query="notebook" -a pages=3
        scrapy crawl amazon -a url="https://www.amazon.com.br/s?k=celular"
    """

    name = "amazon"
    allowed_domains = ["www.amazon.com.br", "amazon.com.br"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 200,
        },
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 4,
    }

    def __init__(self, query=None, url=None, pages=3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query or "celular"
        self.custom_url = url
        self.pages = int(pages)

        if not self.custom_url:
            self.start_urls = [f"https://www.amazon.com.br/s?k={self.query}"]

    def parse(self, response):
        """Parse da pagina de resultados de busca."""
        products = response.css("div[data-component-type='s-search-result']")

        for product in products:
            loader = ItemLoader(item=AmazonProductItem(), selector=product)

            # ID do produto
            loader.add_css(
                "product_id",
                "div[data-asin]::attr(data-asin)",
            )

            # Nome do produto
            loader.add_css(
                "name",
                "h2 a span::text, h2 span::text",
            )

            # Preco atual
            loader.add_css(
                "price",
                "span.a-price span.a-offscreen::text",
            )

            # Preco original (riscado)
            loader.add_css(
                "original_price",
                "span.a-price.a-text-price span.a-offscreen::text",
            )

            # Rating
            loader.add_css(
                "rating",
                "span.a-icon-alt::text",
            )

            # Numero de reviews
            loader.add_css(
                "reviews_count",
                "span.a-size-base.s-underline-text::text",
            )

            # URL do produto
            loader.add_css(
                "url",
                "h2 a::attr(href)",
            )

            # Imagem
            loader.add_css(
                "image_url",
                "img.s-image::attr(src)",
            )

            item = loader.load_item()
            item["source"] = "amazon.com.br"
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            # So retorna itens com product_id valido
            if item.get("product_id"):
                yield item

        # Paginacao
        if self.pages > 1:
            next_page = response.css("a.s-pagination-next::attr(href)").get()
            if next_page:
                self.pages -= 1
                yield response.follow(next_page, callback=self.parse)
