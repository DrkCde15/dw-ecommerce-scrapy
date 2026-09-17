"""
Pipelines de processamento para dados raspados.

Responsaveis por:
- Validar campos obrigatorios
- Limpar e normalizar dados
- Filtrar duplicatas
- Exportar para JSON/Parquet (Extract → arquivo)
- Carga no PostgreSQL via StoragePipeline (Load separado)
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
        if item.get("name"):
            item["name"] = item["name"].strip().title()

        if item.get("category"):
            cat_lower = item["category"].lower().strip()
            item["category"] = self.CATEGORY_MAP.get(cat_lower, item["category"].title())

        RATING_MAP = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
        if item.get("rating"):
            if isinstance(item["rating"], str):
                item["rating"] = RATING_MAP.get(item["rating"].lower().strip(), item["rating"])
            try:
                item["rating"] = int(item["rating"])
            except (ValueError, TypeError):
                item["rating"] = None

        if item.get("price"):
            if isinstance(item["price"], str):
                item["price"] = clean_price(item["price"])
            else:
                try:
                    item["price"] = round(float(item["price"]), 2)
                except (ValueError, TypeError):
                    item["price"] = None

        if not item.get("scraped_at"):
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

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

    def __init__(self, output_dir="data/raw"):
        self.output_dir = Path(output_dir)
        self.items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("JSON_OUTPUT_DIR", "data/raw")
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

    def __init__(self, output_dir="data/raw"):
        self.output_dir = Path(output_dir)
        self.items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("PARQUET_OUTPUT_DIR", "data/raw")
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


class FileExportPipeline:
    """
    Pipeline de exportação para arquivos JSON e Parquet.
    Substitui PostgresPipeline no Extract.
    Os arquivos são depois carregados pelo StoragePipeline (Load).
    """

    def __init__(self, output_dir="data/raw"):
        self.output_dir = Path(output_dir)
        self._items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("JSON_OUTPUT_DIR", "data/raw")
        return cls(output_dir=output_dir)

    def open_spider(self, spider):
        self._items = []
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"FileExportPipeline iniciado para spider {spider.name}")

    def process_item(self, item, spider):
        self._items.append(dict(item))
        return item

    def close_spider(self, spider):
        if not self._items:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        spider_name = spider.name
        output_dir = self.output_dir / spider_name
        output_dir.mkdir(parents=True, exist_ok=True)

        json_path = output_dir / f"{spider_name}_{timestamp}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self._items, f, ensure_ascii=False, indent=2)
        logger.info(f"FileExportPipeline: {len(self._items)} itens → {json_path}")

        try:
            df = pd.DataFrame(self._items)
            parquet_path = output_dir / f"{spider_name}_{timestamp}.parquet"
            df.to_parquet(parquet_path, index=False, engine="pyarrow")
            logger.info(f"FileExportPipeline: {len(self._items)} itens → {parquet_path}")
        except Exception as e:
            logger.error(f"Erro ao exportar Parquet para {spider_name}: {e}")

        logger.info(f"FileExportPipeline concluído para {spider_name}: {len(self._items)} registros")


class PostgresPipeline:
    """
    Pipeline para UPSERT no PostgreSQL (usado pelo StoragePipeline).
    NÃO é usado pelos spiders diretamente.
    """

    def __init__(self, postgres_url: str = None, table_name: str = "raw.books"):
        self.postgres_url = postgres_url or "postgresql://postgres:postgres@localhost:5432/ecommerce"
        self.table_name = table_name
        self.engine = None

    @classmethod
    def from_crawler(cls, crawler):
        postgres_url = crawler.settings.get(
            "POSTGRES_URL",
            "postgresql://ecommerce:ecommerce123@localhost:5432/ecommerce",
        )
        table_name = crawler.settings.get("POSTGRES_TABLE", "raw.books")
        return cls(postgres_url=postgres_url, table_name=table_name)

    def _get_engine(self):
        if not self.engine:
            self.engine = create_engine(self.postgres_url)
        return self.engine

    def upsert_records(self, records: list[dict], conflict_columns: list[str] = None):
        """UPSERT registros no PostgreSQL usando ON CONFLICT."""
        if not records:
            return 0

        engine = self._get_engine()
        df = pd.DataFrame(records)
        table = self.table_name.split(".")[-1]
        schema = self.table_name.split(".")[0] if "." in self.table_name else "raw"

        columns = df.columns.tolist()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        if conflict_columns:
            conflict_str = ", ".join(conflict_columns)
            update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_columns])
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols}"
        else:
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        with engine.begin() as conn:
            conn.execute(text(sql), df.to_dict(orient="records"))

        return len(records)

    def close(self):
        if self.engine:
            self.engine.dispose()


class StoragePipeline:
    """
    Pipeline de Load: arquivos JSON/Parquet → PostgreSQL com UPSERT e dedup.
    Substitui o PostgresPipeline direto dos spiders.
    Implementa ETL correto com CDC e deduplicação.
    """

    def __init__(self, postgres_url: str = None, data_dir: str = "data/raw"):
        self.postgres_url = postgres_url or "postgresql://postgres:postgres@localhost:5432/ecommerce"
        self.engine = None
        self.data_dir = Path(data_dir)
        self._lineage_entries = []

    def _get_engine(self):
        if not self.engine:
            self.engine = create_engine(self.postgres_url)
        return self.engine

    def load_json_to_table(self, json_path: str, table_name: str, schema: str = "raw",
                           conflict_columns: list[str] = None):
        """Carrega um arquivo JSON para PostgreSQL com UPSERT e dedup."""
        print(f"Carregando {json_path} → {schema}.{table_name} (UPSERT)")

        with open(json_path, "r") as f:
            records = json.load(f)

        if not records:
            print(f"  → 0 registros no arquivo")
            return 0

        df = pd.DataFrame(records)
        print(f"  → {len(df)} registros no arquivo")

        now = datetime.now(timezone.utc).isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now
        if "_extract_ts" not in df.columns:
            df["_extract_ts"] = now

        dedup_cols = conflict_columns or ["product_id"]
        if "scraped_at" in df.columns:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            df = df.sort_values("scraped_at", ascending=False)
            df = df.drop_duplicates(subset=dedup_cols, keep="first")
        else:
            df = df.drop_duplicates(subset=dedup_cols, keep="first")

        engine = self._get_engine()
        table = table_name.split(".")[-1] if "." in table_name else table_name
        schema = table_name.split(".")[0] if "." in table_name else schema

        columns = df.columns.tolist()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        if conflict_columns:
            conflict_str = ", ".join(conflict_columns)
            update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_columns])
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols}"
        else:
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        with engine.begin() as conn:
            conn.execute(text(sql), df.to_dict(orient="records"))

        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            total = result.scalar()

        print(f"  → {len(df)} registros carregados (UPSERT), total na tabela: {total}")
        self._record_lineage(table_name, schema, len(df), "load")
        return len(df)

    def load_parquet_to_table(self, parquet_path: str, table_name: str, schema: str = "raw",
                              conflict_columns: list[str] = None):
        """Carrega um arquivo Parquet para PostgreSQL com UPSERT."""
        df = pd.read_parquet(parquet_path)

        now = datetime.now(timezone.utc).isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now
        if "_extract_ts" not in df.columns:
            df["_extract_ts"] = now

        dedup_cols = conflict_columns or ["product_id"]
        if "scraped_at" in df.columns:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            df = df.sort_values("scraped_at", ascending=False)
            df = df.drop_duplicates(subset=dedup_cols, keep="first")
        else:
            df = df.drop_duplicates(subset=dedup_cols, keep="first")

        engine = self._get_engine()
        table = table_name.split(".")[-1]
        schema = table_name.split(".")[0] if "." in table_name else schema

        columns = df.columns.tolist()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        if conflict_columns:
            conflict_str = ", ".join(conflict_columns)
            update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_columns])
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols}"
        else:
            sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        with engine.begin() as conn:
            conn.execute(text(sql), df.to_dict(orient="records"))

        print(f"  → {len(df)} registros carregados (UPSERT) de {parquet_path}")
        self._record_lineage(table_name, schema, len(df), "load_parquet")
        return len(df)

    def load_all_from_directory(self, data_dir: str = None):
        """Carrega todos os arquivos JSON e Parquet de um diretório para PostgreSQL."""
        data_path = Path(data_dir) if data_dir else self.data_dir
        results = {}

        file_to_table = {
            "books": ("raw.books", ["product_id", "source"]),
            "amazon": ("raw.amazon", ["product_id", "source"]),
            "americanas": ("raw.americanas", ["product_id", "source"]),
            "kabum": ("raw.kabum", ["product_id", "source"]),
        }

        for spider_name, (table_name, conflict_cols) in file_to_table.items():
            json_files = list(data_path.glob(f"{spider_name}/*.json"))
            parquet_files = list(data_path.glob(f"{spider_name}/*.parquet"))

            for json_file in json_files:
                try:
                    count = self.load_json_to_table(str(json_file), table_name, "raw", conflict_cols)
                    results[f"{spider_name}_{json_file.stem}"] = {"status": "success", "count": count}
                except Exception as e:
                    print(f"  → ERRO carregando {json_file}: {e}")
                    results[f"{spider_name}_{json_file.stem}"] = {"status": "error", "error": str(e)}

            for parquet_file in parquet_files:
                try:
                    count = self.load_parquet_to_table(str(parquet_file), table_name, "raw", conflict_cols)
                    results[f"{spider_name}_{parquet_file.stem}"] = {"status": "success", "count": count}
                except Exception as e:
                    print(f"  → ERRO carregando {parquet_file}: {e}")
                    results[f"{spider_name}_{parquet_file.stem}"] = {"status": "error", "error": str(e)}

        return results

    def _record_lineage(self, table_name: str, schema: str, record_count: int, operation: str):
        """Registra linhagem de dados."""
        self._lineage_entries.append({
            "table_name": table_name,
            "schema": schema,
            "record_count": record_count,
            "operation": operation,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def log_lineage(self):
        """Imprime e persiste a linhagem dos dados."""
        print("\n=== DATA LINEAGE ===")
        for entry in self._lineage_entries:
            print(f"  {entry['operation']}: {entry['schema']}.{entry['table_name']} → {entry['record_count']} registros")
        return self._lineage_entries

    def truncate_table(self, table_name: str, schema: str = "raw"):
        """Limpa uma tabela antes de recarregar."""
        with self._get_engine().begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name} CASCADE"))
        print(f"  → Tabela {schema}.{table_name} limpa")

    def table_exists(self, table_name: str, schema: str = "raw") -> bool:
        """Verifica se uma tabela existe."""
        with self._get_engine().connect() as conn:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
            ))
            return result.scalar()

    def close(self):
        if self.engine:
            self.engine.dispose()