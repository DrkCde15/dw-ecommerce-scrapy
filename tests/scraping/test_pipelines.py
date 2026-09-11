"""
Testes unitarios para os pipelines de processamento.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.scraping.ecommerce_scraper.pipelines import (
    CleaningPipeline,
    DuplicatesFilterPipeline,
    JsonExportPipeline,
    PandasExportPipeline,
    PostgresPipeline,
    ValidationPipeline,
)


@pytest.fixture
def sample_item():
    """Item de exemplo para testes."""
    return {
        "product_id": "123",
        "name": "produto teste",
        "price": "R$ 99,90",
        "category": "eletronicos",
        "description": "Um produto muito legal",
        "image_url": "https://example.com/img.jpg",
        "url": "https://example.com/product/123",
        "source": "https://example.com/products?page=1",
        "scraped_at": "2024-01-15T10:00:00",
    }


@pytest.fixture
def invalid_item():
    """Item invalido para testes."""
    return {
        "product_id": None,
        "name": "",
        "price": None,
        "category": "test",
    }


@pytest.fixture
def spider():
    """Spider mock para testes."""
    return MagicMock()


class TestValidationPipeline:
    """Testes para o pipeline de validacao."""

    def test_valid_item_passes(self, spider):
        item = {"product_id": "123", "name": "produto teste", "price": 99.90}
        pipeline = ValidationPipeline()
        result = pipeline.process_item(item, spider)
        assert result is not None
        assert result["product_id"] == "123"

    def test_missing_product_id_rejected(self, invalid_item, spider):
        pipeline = ValidationPipeline()
        result = pipeline.process_item(invalid_item, spider)
        assert result is None

    def test_missing_name_rejected(self, spider):
        item = {"product_id": "123", "name": None, "price": 99.90}
        pipeline = ValidationPipeline()
        result = pipeline.process_item(item, spider)
        assert result is None

    def test_zero_price_rejected(self, spider):
        item = {"product_id": "123", "name": "Teste", "price": 0}
        pipeline = ValidationPipeline()
        result = pipeline.process_item(item, spider)
        assert result is None

    def test_negative_price_rejected(self, spider):
        item = {"product_id": "123", "name": "Teste", "price": -10}
        pipeline = ValidationPipeline()
        result = pipeline.process_item(item, spider)
        assert result is None

    def test_string_price_rejected(self, spider):
        item = {"product_id": "123", "name": "Teste", "price": "invalido"}
        pipeline = ValidationPipeline()
        result = pipeline.process_item(item, spider)
        assert result is None


class TestCleaningPipeline:
    """Testes para o pipeline de limpeza."""

    def test_cleans_name(self, sample_item, spider):
        pipeline = CleaningPipeline()
        result = pipeline.process_item(sample_item, spider)
        assert result["name"] == "Produto Teste"

    def test_normalizes_category(self, sample_item, spider):
        pipeline = CleaningPipeline()
        result = pipeline.process_item(sample_item, spider)
        assert result["category"] == "Eletronicos"

    def test_converts_price_to_float(self, sample_item, spider):
        pipeline = CleaningPipeline()
        result = pipeline.process_item(sample_item, spider)
        assert isinstance(result["price"], float)
        assert result["price"] == 99.90

    def test_cleans_source_url(self, sample_item, spider):
        pipeline = CleaningPipeline()
        result = pipeline.process_item(sample_item, spider)
        assert "?" not in result["source"]
        assert not result["source"].endswith("/")

    def test_adds_scraped_at_if_missing(self, spider):
        item = {
            "product_id": "123",
            "name": "teste",
            "price": 10.0,
            "source": "https://example.com",
        }
        pipeline = CleaningPipeline()
        result = pipeline.process_item(item, spider)
        assert "scraped_at" in result

    def test_keeps_existing_scraped_at(self, sample_item, spider):
        pipeline = CleaningPipeline()
        result = pipeline.process_item(sample_item, spider)
        assert result["scraped_at"] == "2024-01-15T10:00:00"


class TestDuplicatesFilterPipeline:
    """Testes para o pipeline de deduplicacao."""

    def test_first_item_passes(self, sample_item, spider):
        pipeline = DuplicatesFilterPipeline()
        pipeline.open_spider(spider)
        result = pipeline.process_item(sample_item, spider)
        assert result is not None

    def test_duplicate_item_rejected(self, sample_item, spider):
        pipeline = DuplicatesFilterPipeline()
        pipeline.open_spider(spider)
        pipeline.process_item(sample_item, spider)
        result = pipeline.process_item(sample_item, spider)
        assert result is None

    def test_different_items_pass(self, spider):
        pipeline = DuplicatesFilterPipeline()
        pipeline.open_spider(spider)

        item1 = {"product_id": "001", "name": "A", "source": "https://a.com"}
        item2 = {"product_id": "002", "name": "B", "source": "https://a.com"}

        result1 = pipeline.process_item(item1, spider)
        result2 = pipeline.process_item(item2, spider)

        assert result1 is not None
        assert result2 is not None

    def test_same_id_different_source_pass(self, spider):
        pipeline = DuplicatesFilterPipeline()
        pipeline.open_spider(spider)

        item1 = {"product_id": "001", "name": "A", "source": "https://a.com"}
        item2 = {"product_id": "001", "name": "A", "source": "https://b.com"}

        result1 = pipeline.process_item(item1, spider)
        result2 = pipeline.process_item(item2, spider)

        assert result1 is not None
        assert result2 is not None

    def test_open_spider_clears_seen(self, spider):
        pipeline = DuplicatesFilterPipeline()
        pipeline.open_spider(spider)
        pipeline.seen_ids.add("test")
        pipeline.open_spider(spider)
        assert len(pipeline.seen_ids) == 0


class TestJsonExportPipeline:
    """Testes para o pipeline de exportacao JSON."""

    def test_export_creates_file(self, sample_item, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = JsonExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)
            pipeline.process_item(sample_item, spider)
            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.json"))
            assert len(files) == 1

            with open(files[0], encoding="utf-8") as f:
                data = json.load(f)
                assert len(data) == 1
                assert data[0]["product_id"] == "123"

    def test_export_empty_when_no_items(self, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = JsonExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)
            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.json"))
            assert len(files) == 0

    def test_export_multiple_items(self, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = JsonExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)

            for i in range(5):
                item = {"product_id": str(i), "name": f"Item {i}"}
                pipeline.process_item(item, spider)

            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.json"))
            with open(files[0], encoding="utf-8") as f:
                data = json.load(f)
                assert len(data) == 5


class TestPandasExportPipeline:
    """Testes para o pipeline de exportacao Parquet."""

    def test_export_creates_parquet(self, sample_item, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PandasExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)
            pipeline.process_item(sample_item, spider)
            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

            df = pd.read_parquet(files[0])
            assert len(df) == 1
            assert "product_id" in df.columns

    def test_export_empty_when_no_items(self, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PandasExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)
            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 0

    def test_export_multiple_items(self, spider):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PandasExportPipeline(output_dir=tmpdir)
            pipeline.open_spider(spider)

            for i in range(10):
                item = {"product_id": str(i), "name": f"Item {i}", "price": i * 10.0}
                pipeline.process_item(item, spider)

            pipeline.close_spider(spider)

            files = list(Path(tmpdir).glob("*.parquet"))
            df = pd.read_parquet(files[0])
            assert len(df) == 10


class TestPostgresPipeline:
    """Testes para o pipeline de exportacao PostgreSQL."""

    def test_init_default_values(self):
        pipeline = PostgresPipeline(
            postgres_url="postgresql://user:pass@localhost:5432/db",
            table_name="raw.books",
        )
        assert pipeline.postgres_url == "postgresql://user:pass@localhost:5432/db"
        assert pipeline.table_name == "raw.books"
        assert pipeline.items == []

    def test_init_custom_table(self):
        pipeline = PostgresPipeline(
            postgres_url="postgresql://user:pass@localhost:5432/db",
            table_name="staging.products",
        )
        assert pipeline.table_name == "staging.products"

    def test_process_item_accumulates(self, spider):
        pipeline = PostgresPipeline(
            postgres_url="postgresql://user:pass@localhost:5432/db",
        )
        pipeline.engine = None  # Simula falha de conexao

        item = {"product_id": "1", "name": "Teste", "price": 99.90}
        result = pipeline.process_item(item, spider)

        assert result is not None
        assert len(pipeline.items) == 1

    def test_flush_without_engine(self, spider):
        pipeline = PostgresPipeline(
            postgres_url="postgresql://user:pass@localhost:5432/db",
        )
        pipeline.engine = None

        pipeline.items = [{"product_id": "1", "name": "Teste"}]
        pipeline._flush()

        # Nao deve alterar os itens quando nao ha engine
        assert len(pipeline.items) == 1

    def test_from_crawler_settings(self):
        class MockSettings(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        class MockCrawler:
            settings = MockSettings({
                "POSTGRES_URL": "postgresql://test:test@localhost:5432/testdb",
                "POSTGRES_TABLE": "raw.test_data",
            })

        pipeline = PostgresPipeline.from_crawler(MockCrawler())
        assert pipeline.postgres_url == "postgresql://test:test@localhost:5432/testdb"
        assert pipeline.table_name == "raw.test_data"

    def test_from_crawler_defaults(self):
        class MockSettings(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        class MockCrawler:
            settings = MockSettings({})

        pipeline = PostgresPipeline.from_crawler(MockCrawler())
        assert pipeline.postgres_url == "postgresql://ecommerce:ecommerce123@localhost:5432/ecommerce"
        assert pipeline.table_name == "raw.books"
