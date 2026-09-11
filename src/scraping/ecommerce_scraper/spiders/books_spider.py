"""
Spider especifico para books.toscrape.com.

Raspa catalogo completo de livros com:
- Titulo, preco, rating, disponibilidade
- Imagem, URL do produto
- Paginacao automatica
"""

from datetime import datetime, timezone

import scrapy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst


def clean_price(value: str) -> float | None:
    """Remove '£' e converte para float."""
    if not value:
        return None
    cleaned = value.replace("£", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def rating_to_int(value: list) -> int:
    """Converte classe CSS de rating para inteiro."""
    rating_map = {
        "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5,
    }
    if isinstance(value, list) and value:
        return rating_map.get(value[0], 0)
    return 0


class BookItem(scrapy.Item):
    product_id = scrapy.Field(output_processor=TakeFirst())
    name = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(
        input_processor=MapCompose(clean_price),
        output_processor=TakeFirst(),
    )
    rating = scrapy.Field(output_processor=TakeFirst())
    availability = scrapy.Field(output_processor=TakeFirst())
    image_url = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())
    category = scrapy.Field(output_processor=TakeFirst())
    source = scrapy.Field(output_processor=TakeFirst())
    scraped_at = scrapy.Field(output_processor=TakeFirst())


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/catalogue/category/books_1/index.html"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.ValidationPipeline": 200,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 300,
            "src.scraping.ecommerce_scraper.pipelines.PostgresPipeline": 400,
        },
        "POSTGRES_TABLE": "raw.books",
    }

    def parse(self, response):
        """Raspa livros da pagina atual e segue paginacao."""
        books = response.css("article.product_pod")

        for book in books:
            loader = ItemLoader(item=BookItem(), selector=book)

            # ID: extrai do href (ex: catalogue/a-light-in-the-attic_1000/index.html)
            loader.add_css(
                "product_id",
                "h3 a::attr(href)",
                MapCompose(lambda x: x.split("_")[-1].replace("/index.html", "")),
            )

            # Titulo completo via atributo title
            loader.add_css("name", "h3 a::attr(title)")

            # Preco
            loader.add_css("price", "p.price_color::text")

            # Rating (extrai a classe CSS)
            loader.add_css(
                "rating",
                "p.star-rating::attr(class)",
                MapCompose(lambda x: x.replace("star-rating", "").strip()),
            )

            # Disponibilidade
            loader.add_css("availability", "p.instock.availability::text")

            # Imagem
            loader.add_css("image_url", "img.thumbnail::attr(src)")

            # URL do produto
            loader.add_css("url", "h3 a::attr(href)")

            item = loader.load_item()
            item["source"] = response.url
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()
            item["category"] = "Livros"

            yield item

        # Paginacao: segue para proxima pagina
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
