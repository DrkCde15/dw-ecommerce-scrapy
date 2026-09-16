"""
Pipelines de processamento para dados raspados.

Responsaveis por:
- Validar campos obrigatorios
- Limpar e normalizar dados
- Filtrar duplicatas
- Exportar para JSON/DataFrame/PostgreSQL
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from scrapy import signals
from sqlalchemy import create_engine, text

from .spiders.products_spider import clean_price

logger = logging.getLogger(__name__)


class ValidationPipeline:
    """Valida campos obrigatorios nos itens raspados."""

    REQUIRED_FIELDS = ["product_id", "name", "price"]

    def process_item(self, item, spider):
        missing = [f for f in self.REQUIRED_FIELDS if not item.get(f)]

        if missing:
            logger.warning(
                f"Item ignorado (campos faltando: {missing}): "
                f"{item.get('name', 'UNKNOWN')}"
            )
            return None

        if not isinstance(item.get("price"), (int, float)):
            logger.warning(f"Preco invalido para '{item.get('name')}': {item.get('price')}")
            return None

        if item["price"] <= 0:
            logger.warning(f"Preco zero/negativo para '{item.get('name')}': {item['price']}")
            return None

        return item


class CleaningPipeline:
    """Limpa e normaliza os dados dos itens."""

    CATEGORY_MAP = {
        "eletronicos": "Eletronicos",
        "eletrônicos": "Eletronicos",
        "electronics": "Eletronicos",
        "casa": "Casa e Decoração",
        "home": "Casa e Decoração",
        "moda": "Moda",
        "fashion": "Moda",
        "esportes": "Esportes",
        "sports": "Esportes",
        "livros": "Livros",
        "books": "Livros",
        "alimentos": "Alimentos",
        "food": "Alimentos",
    }

    def process_item(self, item, spider):
        # Normaliza nome
        if item.get("name"):
            item["name"] = item["name"].strip().title()

        # Normaliza categoria
        if item.get("category"):
            cat_lower = item["category"].lower().strip()
            item["category"] = self.CATEGORY_MAP.get(cat_lower, item["category"].title())

        # Converte rating de texto para numero (One->1, Two->2, etc.)
        RATING_MAP = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
        if item.get("rating"):
            if isinstance(item["rating"], str):
                item["rating"] = RATING_MAP.get(item["rating"].lower().strip(), item["rating"])
            try:
                item["rating"] = int(item["rating"])
            except (ValueError, TypeError):
                item["rating"] = None

        # Garante que price e float (usa clean_price para formatos BR/US)
        if item.get("price"):
            if isinstance(item["price"], str):
                item["price"] = clean_price(item["price"])
            else:
                try:
                    item["price"] = round(float(item["price"]), 2)
                except (ValueError, TypeError):
                    item["price"] = None

        # Adiciona timestamp se nao existir
        if not item.get("scraped_at"):
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Normaliza source (remove query params)
        if item.get("source"):
            item["source"] = item["source"].split("?")[0].rstrip("/")

        return item


class DuplicatesFilterPipeline:
    """Remove duplicatas baseado no product_id."""

    def __init__(self):
        self.seen_ids: set[str] = set()

    def process_item(self, item, spider):
        product_id = item.get("product_id")
        if not product_id:
            return item

        # Gera hash unico
        item_hash = hashlib.md5(
            f"{product_id}:{item.get('source', '')}".encode()
        ).hexdigest()

        if item_hash in self.seen_ids:
            logger.debug(f"Duplicata ignorada: {product_id}")
            return None

        self.seen_ids.add(item_hash)
        return item

    def open_spider(self, spider):
        self.seen_ids.clear()

    def close_spider(self, spider):
        logger.info(f"Total de itens unicos: {len(self.seen_ids)}")


class JsonExportPipeline:
    """Exporta itens para arquivo JSON."""

    def __init__(self, output_dir="data/scraped"):
        self.output_dir = Path(output_dir)
        self.items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("JSON_OUTPUT_DIR", "data/scraped")
        return cls(output_dir=output_dir)

    def open_spider(self, spider):
        self.items = []
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        if not self.items:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{spider.name}_{timestamp}.json"
        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

        logger.info(f"Exportados {len(self.items)} itens para {filepath}")


class PandasExportPipeline:
    """Exporta itens para DataFrame e salva como Parquet."""

    def __init__(self, output_dir="data/scraped"):
        self.output_dir = Path(output_dir)
        self.items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("PARQUET_OUTPUT_DIR", "data/scraped")
        return cls(output_dir=output_dir)

    def open_spider(self, spider):
        self.items = []
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        if not self.items:
            return

        df = pd.DataFrame(self.items)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = self.output_dir / f"{spider.name}_{timestamp}.parquet"

        df.to_parquet(filepath, index=False, engine="pyarrow")
        logger.info(f"Exportados {len(df)} itens para {filepath}")


class PostgresPipeline:
    """
    Pipeline que salva itens raspados direto no PostgreSQL.

    Configuracao no settings.py ou via -a:
        POSTGRES_URL=postgresql://user:pass@host:5432/dbname
    """

    def __init__(self, postgres_url: str, table_name: str = "raw.books"):
        self.postgres_url = postgres_url
        self.table_name = table_name
        self.engine = None
        self.items: list[dict] = []
        self.batch_size = 100

    @classmethod
    def from_crawler(cls, crawler):
        postgres_url = crawler.settings.get(
            "POSTGRES_URL",
            "postgresql://ecommerce:ecommerce123@localhost:5432/ecommerce",
        )
        table_name = crawler.settings.get("POSTGRES_TABLE", "raw.books")
        return cls(postgres_url=postgres_url, table_name=table_name)

    def open_spider(self, spider):
        try:
            self.engine = create_engine(self.postgres_url)
            # Testa conexao
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Conectado ao PostgreSQL: {self.table_name}")
        except Exception as e:
            logger.error(f"Erro ao conectar no PostgreSQL: {e}")
            self.engine = None

    def process_item(self, item, spider):
        self.items.append(dict(item))

        # Batch insert a cada N itens
        if len(self.items) >= self.batch_size:
            self._flush()

        return item

    def _flush(self):
        """Insere itens acumulados no banco."""
        if not self.items or not self.engine:
            return

        try:
            df = pd.DataFrame(self.items)
            df.to_sql(
                self.table_name.split(".")[-1],
                self.engine,
                schema=self.table_name.split(".")[0] if "." in self.table_name else None,
                if_exists="append",
                index=False,
                method="multi",
            )
            logger.info(f"Inseridos {len(self.items)} itens no {self.table_name}")
            self.items.clear()
        except Exception as e:
            logger.error(f"Erro ao inserir no PostgreSQL: {e}")

    def close_spider(self, spider):
        """Insere itens restantes ao fechar."""
        self._flush()

        if self.engine:
            total_query = text(f"SELECT COUNT(*) FROM {self.table_name}")
            with self.engine.connect() as conn:
                result = conn.execute(total_query)
                total = result.scalar()
                logger.info(f"Total de registros em {self.table_name}: {total}")

            self.engine.dispose()
