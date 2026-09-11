"""
Spider para raspagem de produtos de e-commerce.

Coleta dados de sites de e-commerce publicos e extrai:
- ID do produto
- Nome/titulo
- Preco
- Categoria
- URL da imagem
- Descricao
- URL de origem
"""

import re
from datetime import datetime, timezone

import scrapy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from scrapy import Request


def clean_price(value: str) -> float | None:
    """Remove caracteres nao numericos do preco e converte para float.

    Suporta formatos brasileiro (1.299,99) e americano (1,299.99).
    """
    if not value:
        return None
    cleaned = re.sub(r"[^\d.,]", "", value.strip())
    if not cleaned:
        return None

    # Detecta formato: se tem virgula E ponto, determina qual e separador decimal
    if "," in cleaned and "." in cleaned:
        # Formato BR: 1.299,99 -> remove pontos, troca virgula por ponto
        # Formato US: 1,299.99 -> remove virgulas
        last_comma = cleaned.rfind(",")
        last_dot = cleaned.rfind(".")
        if last_comma > last_dot:
            # Formato BR: ultimo separador e virgula (decimal)
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # Formato US: ultimo separador e ponto (decimal)
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        # So tem virgula: assume formato BR (virgula = decimal)
        cleaned = cleaned.replace(",", ".")
    # Se so tem ponto, mantem como esta (ja e formato US)

    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_text(value: str) -> str:
    """Remove espacos extras e quebras de linha."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip())


def parse_date(value: str) -> str | None:
    """Tenta parsear datas em formatos comuns."""
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"]
    for fmt in formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return None


class ProductItem(scrapy.Item):
    """Item que representa um produto de e-commerce."""

    product_id = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst(),
    )
    name = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst(),
    )
    price = scrapy.Field(
        input_processor=MapCompose(clean_price),
        output_processor=TakeFirst(),
    )
    category = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst(),
    )
    description = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst(),
    )
    image_url = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())
    source = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst(),
    )
    scraped_at = scrapy.Field(output_processor=TakeFirst())


class ProductSpider(scrapy.Spider):
    """
    Spider generico para raspagem de produtos de e-commerce.

    Suporta multiplos sites atraves de configuracao.
    Use: scrapy crawl products -a url=https://example.com/products
    """

    name = "products"
    allowed_domains: list[str] = []

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.ValidationPipeline": 200,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 300,
        },
    }

    def __init__(self, url=None, category=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_url = url or "https://httpbin.org"
        self.category = category or "all"
        self.logger.info(f"Iniciando spider com URL: {self.start_url}")

    def start_requests(self):
        """Gera requests iniciais para o site alvo."""
        yield Request(
            url=self.start_url,
            callback=self.parse,
            errback=self.handle_error,
            meta={"category": self.category},
        )

    def parse(self, response):
        """
        Parse generico de pagina de produtos.

        Este metodo serve como template. Cada site real precisaria
        de seletores CSS/XPath especificos.
        """
        # Exemplo basico de extracao (adaptar para cada site)
        products = response.css("div.product, div.item, article.product-card")

        if not products:
            # Fallback: tenta extrair links de produtos
            self.logger.info("Nenhum produto encontrado, tentando extrair links...")
            links = response.css("a[href*='product']::attr(href)").getall()
            for link in links[:10]:  # Limita a 10 links
                yield response.follow(
                    link,
                    callback=self.parse_product_detail,
                    meta={"category": response.meta.get("category")},
                )
            return

        for product in products:
            loader = ItemLoader(item=ProductItem(), selector=product)

            loader.add_css("product_id", "::attr(data-id), ::attr(id)")
            loader.add_css("name", "h2::text, h3::text, .product-name::text")
            loader.add_css("price", ".price::text, .product-price::text")
            loader.add_css("category", ".category::text, .tag::text")
            loader.add_css("description", ".description::text, p::text")
            loader.add_css("image_url", "img::attr(src)")
            loader.add_css("url", "a::attr(href)")

            item = loader.load_item()
            item["source"] = response.url
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()
            item["category"] = response.meta.get("category", "all")

            yield item

        # Paginacao
        next_page = response.css("a.next::attr(href), a[rel='next']::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={"category": response.meta.get("category")},
            )

    def parse_product_detail(self, response):
        """Parse detalhado de pagina de produto individual."""
        loader = ItemLoader(item=ProductItem(), response=response)

        loader.add_css("product_id", "::attr(data-id), ::attr(id)")
        loader.add_css("name", "h1::text, .product-title::text")
        loader.add_css("price", ".price::text, .product-price::text, [data-price]::attr(data-price)")
        loader.add_css("category", ".breadcrumb a:last-child::text")
        loader.add_css("description", ".product-description::text, .description::text")
        loader.add_css("image_url", "img.product-image::attr(src), .gallery img::attr(src)")
        loader.add_css("url", "link[rel='canonical']::attr(href)")

        item = loader.load_item()
        item["source"] = response.url
        item["scraped_at"] = datetime.now(timezone.utc).isoformat()

        yield item

    def handle_error(self, failure):
        """Trata erros de requisicao."""
        self.logger.error(f"Erro na requisicao: {failure.request.url} - {failure.value}")
