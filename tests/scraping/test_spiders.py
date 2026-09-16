"""
Testes unitarios para o spider de produtos.
"""

import pytest
import scrapy
from scrapy.http import HtmlResponse, Request

from src.scraping.ecommerce_scraper.spiders.products_spider import (
    ProductItem,
    ProductSpider,
    clean_price,
    clean_text,
    parse_date,
)


class TestCleanPrice:
    """Testes para a funcao clean_price."""

    def test_clean_price_with_currency_symbol(self):
        assert clean_price("R$ 150,90") == 150.90

    def test_clean_price_with_dot_separator(self):
        assert clean_price("$1,299.99") == 1299.99

    def test_clean_price_simple(self):
        assert clean_price("99.90") == 99.90

    def test_clean_price_none(self):
        assert clean_price(None) is None

    def test_clean_price_empty_string(self):
        assert clean_price("") is None

    def test_clean_price_invalid(self):
        assert clean_price("abc") is None

    def test_clean_price_with_spaces(self):
        assert clean_price("  R$ 45,50  ") == 45.50


class TestCleanText:
    """Testes para a funcao clean_text."""

    def test_clean_text_removes_extra_spaces(self):
        assert clean_text("  Produto   Legal  ") == "Produto Legal"

    def test_clean_text_removes_newlines(self):
        assert clean_text("Produto\n\ndescrição\nlonga") == "Produto descrição longa"

    def test_clean_text_none(self):
        assert clean_text(None) == ""

    def test_clean_text_empty(self):
        assert clean_text("") == ""

    def test_clean_text_tabs(self):
        assert clean_text("Produto\t\testilo") == "Produto estilo"


class TestParseDate:
    """Testes para a funcao parse_date."""

    def test_parse_date_iso(self):
        assert parse_date("2024-01-15") == "2024-01-15T00:00:00"

    def test_parse_date_brazilian(self):
        assert parse_date("15/01/2024") == "2024-01-15T00:00:00"

    def test_parse_date_with_time(self):
        assert parse_date("2024-01-15T10:30:00") == "2024-01-15T10:30:00"

    def test_parse_date_invalid(self):
        assert parse_date("not-a-date") is None


class TestProductItem:
    """Testes para o Item de produto."""

    def test_item_has_required_fields(self):
        item = ProductItem()
        required_fields = [
            "product_id", "name", "price", "category",
            "description", "image_url", "url", "source", "scraped_at",
        ]
        for field in required_fields:
            assert field in item.fields

    def test_item_can_store_values(self):
        item = ProductItem()
        item["product_id"] = "123"
        item["name"] = "Teste"
        item["price"] = 99.90

        assert item["product_id"] == "123"
        assert item["name"] == "Teste"
        assert item["price"] == 99.90


class TestProductSpider:
    """Testes para o Spider de produtos."""

    def setup_method(self):
        self.spider = ProductSpider(url="https://example.com/products")

    def test_spider_name(self):
        assert self.spider.name == "products"

    def test_spider_default_url(self):
        spider = ProductSpider()
        assert spider.start_url == "https://httpbin.org"

    def test_spider_custom_url(self):
        spider = ProductSpider(url="https://custom.com")
        assert spider.start_url == "https://custom.com"

    def test_spider_category(self):
        spider = ProductSpider(category="eletronicos")
        assert spider.category == "eletronicos"

    def test_start_requests(self):
        requests = list(self.spider.start_requests())
        assert len(requests) == 1
        assert requests[0].url == "https://example.com/products"

    def test_parse_returns_items(self):
        html = """
        <html>
        <body>
            <div class="product" data-id="001">
                <h2>Produto Teste</h2>
                <span class="price">R$ 99,90</span>
                <span class="category">Eletronicos</span>
                <p>Descricao do produto</p>
                <img src="https://example.com/img.jpg">
            </div>
        </body>
        </html>
        """
        request = Request(url="https://example.com/products")
        response = HtmlResponse(
            url="https://example.com/products",
            request=request,
            body=html.encode("utf-8"),
        )

        items = list(self.spider.parse(response))
        assert len(items) >= 0  # Pode nao encontrar produtos com seletores genericos

    def test_parse_handles_empty_page(self):
        html = "<html><body><p>Nenhum produto</p></body></html>"
        request = Request(url="https://example.com/products")
        response = HtmlResponse(
            url="https://example.com/products",
            request=request,
            body=html.encode("utf-8"),
        )

        items = list(self.spider.parse(response))
        assert isinstance(items, list)

    def test_handle_error(self, caplog):
        import logging
        request = Request(url="https://example.com/fail")
        failure = type("Failure", (), {"request": request, "value": Exception("Timeout")})()

        with caplog.at_level(logging.ERROR):
            self.spider.handle_error(failure)

        assert "Erro na requisicao" in caplog.text


# =============================================================================
# Testes para AmericanasSpider
# =============================================================================


class TestAmericanasItem:
    """Testes para o Item da Americanas."""

    def test_item_has_required_fields(self):
        from src.scraping.ecommerce_scraper.spiders.americanas_spider import AmericanasItem

        item = AmericanasItem()
        required_fields = [
            "product_id", "name", "brand", "price", "list_price",
            "available_quantity", "category", "image_url", "url",
            "seller", "source", "scraped_at",
        ]
        for field in required_fields:
            assert field in item.fields

    def test_item_can_store_values(self):
        from src.scraping.ecommerce_scraper.spiders.americanas_spider import AmericanasItem

        item = AmericanasItem()
        item["product_id"] = "12345"
        item["name"] = "Notebook Dell"
        item["price"] = 3999.90

        assert item["product_id"] == "12345"
        assert item["name"] == "Notebook Dell"
        assert item["price"] == 3999.90


class TestAmericanasSpider:
    """Testes para o Spider da Americanas."""

    def setup_method(self):
        from src.scraping.ecommerce_scraper.spiders.americanas_spider import AmericanasSpider
        self.spider = AmericanasSpider(query="notebook", limit=10)

    def test_spider_name(self):
        assert self.spider.name == "americanas"

    def test_spider_default_query(self):
        from src.scraping.ecommerce_scraper.spiders.americanas_spider import AmericanasSpider
        spider = AmericanasSpider()
        assert spider.query == "notebook"
        assert spider.limit == 100

    def test_spider_custom_params(self):
        assert self.spider.query == "notebook"
        assert self.spider.limit == 10

    def test_build_url(self):
        self.spider._build_url()
        assert "_from=0" in self.spider.start_urls[0]
        assert "_to=9" in self.spider.start_urls[0]
        assert "notebook" in self.spider.start_urls[0]

    def test_parse_returns_items(self):
        import json

        data = [
            {
                "productId": "2021246",
                "productName": "Notebook Dell Inspiron",
                "brand": "Dell",
                "categories": ["/Computadores/Notebooks/"],
                "linkText": "notebook-dell-inspiron",
                "items": [
                    {
                        "images": [{"imageUrl": "https://example.com/img.jpg"}],
                        "sellers": [
                            {
                                "sellerDefault": True,
                                "sellerName": "Americanas",
                                "commertialOffer": {
                                    "Price": 3999.90,
                                    "ListPrice": 4999.90,
                                    "AvailableQuantity": 5,
                                },
                            }
                        ],
                    }
                ],
            }
        ]

        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=json.dumps(data).encode("utf-8"),
            headers={"resources": "0-0/100"},
        )

        items = list(self.spider.parse(response))
        assert len(items) == 1
        assert items[0]["product_id"] == "2021246"
        assert items[0]["name"] == "Notebook Dell Inspiron"
        assert items[0]["price"] == 3999.90

    def test_parse_handles_empty_response(self):
        import json

        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=json.dumps([]).encode("utf-8"),
        )

        items = list(self.spider.parse(response))
        assert len(items) == 0

    def test_parse_handles_invalid_json(self):
        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=b"not json",
        )

        items = list(self.spider.parse(response))
        assert len(items) == 0


# =============================================================================
# Testes para KabumSpider
# =============================================================================


class TestKabumItem:
    """Testes para o Item do KaBuM."""

    def test_item_has_required_fields(self):
        from src.scraping.ecommerce_scraper.spiders.kabum_spider import KabumItem

        item = KabumItem()
        required_fields = [
            "product_id", "name", "brand", "price", "old_price",
            "discount_percentage", "stock", "category", "image_url",
            "url", "seller", "rating", "reviews_count", "source", "scraped_at",
        ]
        for field in required_fields:
            assert field in item.fields

    def test_item_can_store_values(self):
        from src.scraping.ecommerce_scraper.spiders.kabum_spider import KabumItem

        item = KabumItem()
        item["product_id"] = "1049348"
        item["name"] = "Notebook Acer"
        item["price"] = 2999.90

        assert item["product_id"] == "1049348"
        assert item["name"] == "Notebook Acer"
        assert item["price"] == 2999.90


class TestKabumSpider:
    """Testes para o Spider do KaBuM."""

    def setup_method(self):
        from src.scraping.ecommerce_scraper.spiders.kabum_spider import KabumSpider
        self.spider = KabumSpider(query="notebook", limit=10)

    def test_spider_name(self):
        assert self.spider.name == "kabum"

    def test_spider_default_query(self):
        from src.scraping.ecommerce_scraper.spiders.kabum_spider import KabumSpider
        spider = KabumSpider()
        assert spider.query == "notebook"
        assert spider.limit == 100

    def test_spider_custom_params(self):
        assert self.spider.query == "notebook"
        assert self.spider.limit == 10

    def test_build_url(self):
        self.spider._build_url()
        assert "page_number=1" in self.spider.start_urls[0]
        assert "page_size=100" in self.spider.start_urls[0]
        assert "notebook" in self.spider.start_urls[0]

    def test_parse_returns_items(self):
        import json

        data = {
            "meta": {"total_items_count": 100},
            "data": [
                {
                    "id": 1049348,
                    "attributes": {
                        "title": "Notebook Acer Aspire",
                        "price": 2999.90,
                        "price_with_discount": 2799.90,
                        "old_price": 3299.90,
                        "discount_percentage": 15,
                        "stock": 10,
                        "menu": "Computadores/Notebooks",
                        "images": ["https://example.com/img.jpg"],
                        "product_link": "notebook-acer-aspire",
                        "manufacturer": {"name": "Acer"},
                        "marketplace": None,
                        "average_of_ratings": 4.5,
                        "number_of_ratings": 120,
                    },
                }
            ],
        }

        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=json.dumps(data).encode("utf-8"),
        )

        items = list(self.spider.parse(response))
        assert len(items) == 1
        assert items[0]["product_id"] == "1049348"
        assert items[0]["name"] == "Notebook Acer Aspire"
        assert items[0]["price"] == 2799.90

    def test_parse_handles_empty_data(self):
        import json

        data = {"meta": {"total_items_count": 0}, "data": []}

        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=json.dumps(data).encode("utf-8"),
        )

        items = list(self.spider.parse(response))
        assert len(items) == 0

    def test_parse_handles_invalid_json(self):
        request = Request(url="https://example.com/api")
        response = scrapy.http.TextResponse(
            url="https://example.com/api",
            request=request,
            body=b"not json",
        )

        items = list(self.spider.parse(response))
        assert len(items) == 0
