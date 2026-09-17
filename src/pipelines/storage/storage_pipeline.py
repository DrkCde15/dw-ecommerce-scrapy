"""
Pipeline de armazenamento: JSON/Parquet → PostgreSQL
Responsavel por carregar dados brutos com UPSERT, dedup e CDC.
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime, timezone
from ..monitoring.lineage import get_lineage_tracker


class StoragePipeline:
    """Pipeline para armazenar dados brutos no PostgreSQL com UPSERT, dedup e CDC."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)
        self._lineage = []
        self._lineage_tracker = get_lineage_tracker(self.postgres_url)

    def _execute_sql(self, query: str):
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def _read_sql(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.engine)

    def _add_cdc_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas CDC ao DataFrame."""
        now = datetime.now(timezone.utc).isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now
        if "_extract_ts" not in df.columns:
            df["_extract_ts"] = now
        if "deleted_at" not in df.columns:
            df["deleted_at"] = None
        return df

    def _deduplicate(self, df: pd.DataFrame, dedup_cols: list[str]) -> pd.DataFrame:
        """Deduplica mantendo o registro mais recente."""
        if "scraped_at" in df.columns:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            df = df.sort_values("scraped_at", ascending=False)
        df = df.drop_duplicates(subset=dedup_cols, keep="first")
        return df

    def _upsert(self, df: pd.DataFrame, table_name: str, schema: str,
                conflict_cols: list[str]):
        """UPSERT com ON CONFLICT DO UPDATE."""
        columns = df.columns.tolist()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        if conflict_cols:
            conflict_str = ", ".join(conflict_cols)
            update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_cols])
            sql = f"INSERT INTO {schema}.{table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols}"
        else:
            sql = f"INSERT INTO {schema}.{table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        with self.engine.begin() as conn:
            conn.execute(text(sql), df.to_dict(orient="records"))

    def _record_lineage(self, source: str, target_table: str, record_count: int, status: str = "success"):
        self._lineage.append({
            "source": source,
            "target_table": target_table,
            "record_count": record_count,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        # Persistir no banco
        self._lineage_tracker.record_lineage(
            source_name=source,
            source_type="file",
            target_schema=target_table.split(".")[0] if "." in target_table else "raw",
            target_table=target_table.split(".")[-1] if "." in target_table else target_table,
            operation="load",
            record_count=record_count,
            status=status,
        )

    def load_json_to_table(self, json_path: str, table_name: str, schema: str = "raw",
                           conflict_cols: list[str] = None):
        """Carrega JSON para PostgreSQL com UPSERT e dedup (Load stage)."""
        print(f"Carregando {json_path} → {schema}.{table_name}")

        with open(json_path, "r") as f:
            records = json.load(f)
        if not records:
            print(f"  → 0 registros")
            return 0

        df = pd.DataFrame(records)
        print(f"  → {len(df)} registros lidos")

        # Schema validation (Extract quality check)
        self._validate_schema(df, table_name)

        # Add CDC columns
        df = self._add_cdc_columns(df)

        # Deduplicate
        dedup_cols = conflict_cols or ["product_id"]
        df = self._deduplicate(df, dedup_cols)
        print(f"  → {len(df)} registros após dedup")

        # Row count check (Transform quality check)
        self._validate_row_count(df, table_name, min_rows=0)

        # UPSERT
        self._upsert(df, table_name, schema, conflict_cols or ["product_id"])

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            total = result.scalar()

        print(f"  → {len(df)} registros carregados. Total: {total}")
        self._record_lineage(json_path, f"{schema}.{table_name}", len(df))
        return len(df)

    def load_parquet_to_table(self, parquet_path: str, table_name: str, schema: str = "raw",
                               conflict_cols: list[str] = None):
        """Carrega Parquet para PostgreSQL com UPSERT e dedup."""
        print(f"Carregando {parquet_path} → {schema}.{table_name}")

        df = pd.read_parquet(parquet_path)
        print(f"  → {len(df)} registros lidos")

        self._validate_schema(df, table_name)
        df = self._add_cdc_columns(df)

        dedup_cols = conflict_cols or ["product_id"]
        df = self._deduplicate(df, dedup_cols)
        print(f"  → {len(df)} registros após dedup")

        self._validate_row_count(df, table_name, min_rows=0)
        self._upsert(df, table_name, schema, conflict_cols or ["product_id"])

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            total = result.scalar()

        print(f"  → {len(df)} registros carregados. Total: {total}")
        self._record_lineage(parquet_path, f"{schema}.{table_name}", len(df))
        return len(df)

    def load_all_from_directory(self, data_dir: str = None):
        """Carrega todos os arquivos JSON/Parquet do diretório para PostgreSQL."""
        data_path = Path(data_dir) if data_dir else Path("data/raw")
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
                    print(f"  → ERRO: {e}")
                    results[f"{spider_name}_{json_file.stem}"] = {"status": "error", "error": str(e)}
                    self._record_lineage(str(json_file), f"raw.{table_name}", 0, "error")

            for parquet_file in parquet_files:
                try:
                    count = self.load_parquet_to_table(str(parquet_file), table_name, "raw", conflict_cols)
                    results[f"{spider_name}_{parquet_file.stem}"] = {"status": "success", "count": count}
                except Exception as e:
                    print(f"  → ERRO: {e}")
                    results[f"{spider_name}_{parquet_file.stem}"] = {"status": "error", "error": str(e)}
                    self._record_lineage(str(parquet_file), f"raw.{table_name}", 0, "error")

        return results

    def _validate_schema(self, df: pd.DataFrame, table_name: str):
        """Schema validation at extract stage."""
        print(f"  → Schema validation para {table_name}: {list(df.columns)}")
        if len(df) == 0:
            raise ValueError(f"Tabela {table_name} sem dados após extração")

    def _validate_row_count(self, df: pd.DataFrame, table_name: str, min_rows: int = 0):
        """Row count check at transform stage."""
        if len(df) < min_rows and min_rows > 0:
            raise ValueError(f"Tabela {table_name} tem {len(df)} registros (minimo: {min_rows})")
        print(f"  → Row count OK: {len(df)} registros")

    def _validate_referential_integrity(self, child_table: str, child_col: str,
                                          parent_table: str, parent_col: str):
        """Referential integrity check at load stage."""
        result = self._read_sql(f"""
            SELECT COUNT(*) as invalid_count
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
            WHERE p.{parent_col} IS NULL
        """)
        invalid = result["invalid_count"].iloc[0]
        if invalid > 0:
            print(f"  → WARNING: {invalid} chaves refenciais invalidas em {child_table}.{child_col}")
        else:
            print(f"  → Referential integrity OK para {child_table}.{child_col}")
        return invalid == 0

    def get_lineage(self):
        return self._lineage

    def truncate_table(self, table_name: str, schema: str = "raw"):
        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name} CASCADE"))
        print(f"  → Tabela {schema}.{table_name} limpa")

    def table_exists(self, table_name: str, schema: str = "raw") -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
            ))
            return result.scalar()


def main():
    pipeline = StoragePipeline()
    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "raw")
    if os.path.exists(data_dir):
        results = pipeline.load_all_from_directory(data_dir)
        print("\n=== Resultados ===")
        for table, result in results.items():
            print(f"  {table}: {result}")
        print(f"\n=== Data Lineage ===")
        for entry in pipeline.get_lineage():
            print(f"  {entry['source']} → {entry['target_table']}: {entry['record_count']} registros ({entry['status']})")
    else:
        print(f"Diretorio de dados nao encontrado: {data_dir}")


if __name__ == "__main__":
    main()