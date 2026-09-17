"""
Pipeline de transformacao: raw → staging → marts
Responsavel por transformar dados brutos em tabelas de analise.
Usa incremental loading com UPSERT ao inves de replace.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone


class TransformPipeline:
    """Pipeline para transformar dados no PostgreSQL com incremental loading."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)
        self._validation_errors = []

    def _execute_sql(self, query: str):
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def _read_sql(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.engine)

    def _add_cdc_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Adiciona colunas CDC ao DataFrame."""
        now = datetime.now(timezone.utc).isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now
        if "_extract_ts" not in df.columns:
            df["_extract_ts"] = now
        if "deleted_at" not in df.columns:
            df["deleted_at"] = None
        return df

    def _incremental_upsert(self, df: pd.DataFrame, table_name: str, schema: str,
                            conflict_cols: list[str]):
        """Incremental UPSERT ao inves de replace."""
        if len(df) == 0:
            return

        df = self._add_cdc_columns(df, table_name)

        columns = df.columns.tolist()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        conflict_str = ", ".join(conflict_cols)
        update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_cols])
        sql = f"INSERT INTO {schema}.{table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols}"

        with self.engine.begin() as conn:
            conn.execute(text(sql), df.to_dict(orient="records"))

        print(f"  → {len(df)} registros UPSERT em {schema}.{table_name}")

    def _validate_row_count(self, df: pd.DataFrame, stage: str, table_name: str, min_rows: int = 0):
        """Row count validation at transform stage."""
        if len(df) < min_rows and min_rows > 0:
            error = f"{stage} {table_name}: apenas {len(df)} registros (minimo: {min_rows})"
            self._validation_errors.append(error)
            print(f"  → VALIDATION FAIL: {error}")
        else:
            print(f"  → Row count OK ({stage}): {len(df)} registros")

    def _validate_nulls(self, df: pd.DataFrame, stage: str, table_name: str, required_cols: list[str]):
        """Null check at transform stage."""
        for col in required_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    error = f"{stage} {table_name}: {null_count} nulos em {col}"
                    self._validation_errors.append(error)
                    print(f"  → VALIDATION WARNING: {error}")

    # ==========================================
    # STAGING MODELS
    # ==========================================

    def transform_stg_books(self):
        """Transform raw.books → staging.stg_books (incremental)"""
        print("Transformando stg_books...")

        df = self._read_sql("SELECT * FROM raw.books")

        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
        df["rating"] = df["rating"].map(rating_map).fillna(df["rating"])

        def classify_rating(r):
            if pd.isna(r):
                return "Sem avaliacao"
            if r >= 4:
                return "Excelente"
            if r >= 3:
                return "Bom"
            if r >= 2:
                return "Regular"
            return "Ruim"

        df["rating_label"] = df["rating"].apply(classify_rating)

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")

        # Validation at transform stage
        self._validate_row_count(df, "staging", "stg_books")
        self._validate_nulls(df, "staging", "stg_books", ["product_id", "name"])

        # Incremental upsert ao inves de replace
        self._incremental_upsert(df, "stg_books", "staging", ["product_id"])

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_amazon(self):
        """Transform raw.amazon → staging.stg_amazon (incremental)"""
        print("Transformando stg_amazon...")

        df = self._read_sql("SELECT * FROM raw.amazon")

        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["reviews_count"] = pd.to_numeric(df["reviews_count"], errors="coerce").fillna(0).astype(int)
        df["source"] = "amazon"

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")

        self._validate_row_count(df, "staging", "stg_amazon")
        self._validate_nulls(df, "staging", "stg_amazon", ["product_id", "name"])

        self._incremental_upsert(df, "stg_amazon", "staging", ["product_id"])

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_americanas(self):
        """Transform raw.americanas → staging.stg_americanas (incremental)"""
        print("Transformando stg_americanas...")

        df = self._read_sql("SELECT * FROM raw.americanas")

        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
        df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0).astype(int)
        df["source"] = "americanas"

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")

        self._validate_row_count(df, "staging", "stg_americanas")
        self._validate_nulls(df, "staging", "stg_americanas", ["product_id", "name"])

        self._incremental_upsert(df, "stg_americanas", "staging", ["product_id"])

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_kabum(self):
        """Transform raw.kabum → staging.stg_kabum (incremental)"""
        print("Transformando stg_kabum...")

        df = self._read_sql("SELECT * FROM raw.kabum")

        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["old_price"] = pd.to_numeric(df["old_price"], errors="coerce")
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["reviews_count"] = pd.to_numeric(df["reviews_count"], errors="coerce").fillna(0).astype(int)
        df["source"] = "kabum"

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")

        self._validate_row_count(df, "staging", "stg_kabum")
        self._validate_nulls(df, "staging", "stg_kabum", ["product_id", "name"])

        self._incremental_upsert(df, "stg_kabum", "staging", ["product_id"])

        print(f"  → {len(df)} registros transformados")
        return len(df)

    # ==========================================
    # MARTS MODELS
    # ==========================================

    def transform_dim_books(self):
        """Transform staging.stg_books → marts.dim_books (incremental UPSERT)"""
        print("Transformando dim_books...")

        df = self._read_sql("SELECT * FROM staging.stg_books")

        category_avg = df.groupby("category")["price"].mean().reset_index()
        category_avg.columns = ["category", "avg_category_price"]
        df = df.merge(category_avg, on="category", how="left")

        df["price_vs_category"] = df.apply(
            lambda row: round(((row["price"] - row["avg_category_price"]) / row["avg_category_price"] * 100), 2)
            if pd.notna(row["avg_category_price"]) and row["avg_category_price"] > 0
            else 0,
            axis=1
        )

        df["book_id"] = df["product_id"].apply(lambda x: f"book_{x}")

        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        df = df.sort_values("scraped_at", ascending=False)
        df = df.drop_duplicates(subset=["product_id"], keep="first")

        dim_df = df[[
            "book_id", "product_id", "name", "price", "rating",
            "rating_label", "availability", "image_url", "url",
            "category", "source", "scraped_at", "price_vs_category"
        ]].copy()

        dim_df.columns = [
            "book_id", "product_id", "book_name", "price", "rating",
            "rating_label", "availability", "image_url", "url",
            "category", "source", "scraped_at", "price_vs_category"
        ]

        # Validation at transform stage
        self._validate_row_count(dim_df, "marts", "dim_books")
        self._validate_nulls(dim_df, "marts", "dim_books", ["book_id", "book_name", "price"])
        self._validate_nulls(dim_df, "marts", "dim_books", ["book_id"])

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS marts")

        # Incremental upsert ao inves de replace
        self._incremental_upsert(dim_df, "dim_books", "marts", ["book_id"])

        print(f"  → {len(dim_df)} registros transformados")
        return len(dim_df)

    def transform_dim_products(self):
        """Transform staging.stg_* → marts.dim_products (incremental UPSERT)"""
        print("Transformando dim_products...")

        amazon = self._read_sql("SELECT * FROM staging.stg_amazon")
        americanas = self._read_sql("SELECT * FROM staging.stg_americanas")
        kabum = self._read_sql("SELECT * FROM staging.stg_kabum")

        def prepare_source(df, source_name, price_col="price", old_price_col=None):
            result = pd.DataFrame()
            result["product_id"] = df["product_id"].astype(str)
            result["product_name"] = df["name"] if "name" in df.columns else df.get("product_name")
            result["brand"] = df.get("brand")
            result["price"] = pd.to_numeric(df[price_col], errors="coerce")
            result["original_price"] = pd.to_numeric(
                df[old_price_col] if old_price_col else df.get("original_price", 0),
                errors="coerce"
            ).fillna(0)
            result["category"] = df.get("category")
            result["seller"] = df.get("seller")
            result["image_url"] = df.get("image_url")
            result["url"] = df.get("url")
            result["source"] = source_name
            result["scraped_at"] = df.get("scraped_at")
            return result

        amazon_prep = prepare_source(amazon, "amazon")
        americanas_prep = prepare_source(americanas, "americanas", old_price_col="list_price")
        kabum_prep = prepare_source(kabum, "kabum", old_price_col="old_price")

        all_products = pd.concat([amazon_prep, americanas_prep, kabum_prep], ignore_index=True)

        all_products = all_products[
            all_products["product_name"].notna() &
            (all_products["price"] > 0)
        ]

        all_products["discount_percentage"] = all_products.apply(
            lambda row: round(((row["original_price"] - row["price"]) / row["original_price"] * 100), 2)
            if row["original_price"] > 0 and row["original_price"] > row["price"]
            else 0,
            axis=1
        )

        def price_tier(price):
            if price < 100:
                return "Economico"
            if price < 500:
                return "Medio"
            if price < 2000:
                return "Premium"
            return "Luxo"

        all_products["price_tier"] = all_products["price"].apply(price_tier)

        all_products["id"] = all_products.apply(
            lambda row: f"{row['product_id']}_{row['source']}", axis=1
        )

        all_products["scraped_at"] = pd.to_datetime(all_products["scraped_at"])
        all_products = all_products.sort_values("scraped_at", ascending=False)
        all_products = all_products.drop_duplicates(subset=["product_id", "source"], keep="first")

        dim_df = all_products[[
            "id", "product_id", "product_name", "brand", "price",
            "original_price", "discount_percentage", "category",
            "seller", "image_url", "url", "source", "price_tier",
            "scraped_at"
        ]].copy()

        self._validate_row_count(dim_df, "marts", "dim_products", min_rows=0)
        self._validate_nulls(dim_df, "marts", "dim_products", ["id", "product_name", "price"])

        self._execute_sql("CREATE SCHEMA IF NOT EXISTS marts")

        # Incremental upsert ao inves de replace
        self._incremental_upsert(dim_df, "dim_products", "marts", ["id"])

        print(f"  → {len(dim_df)} registros transformados")
        return len(dim_df)

    # ==========================================
    # EXECUCAO COMPLETA
    # ==========================================

    def run_all_staging(self):
        print("=== STAGING ===")
        results = {}
        results["stg_books"] = self.transform_stg_books()
        results["stg_amazon"] = self.transform_stg_amazon()
        results["stg_americanas"] = self.transform_stg_americanas()
        results["stg_kabum"] = self.transform_stg_kabum()

        if self._validation_errors:
            print(f"\n⚠️ {len(self._validation_errors)} warning(s) de validacao:")
            for err in self._validation_errors:
                print(f"  - {err}")
        return results

    def run_all_marts(self):
        print("=== MARTS ===")
        results = {}
        results["dim_books"] = self.transform_dim_books()
        results["dim_products"] = self.transform_dim_products()
        return results

    def run_all(self):
        print("=== INICIANDO TRANSFORMACOES (INCREMENTAL) ===")
        start_time = datetime.now()

        staging_results = self.run_all_staging()
        marts_results = self.run_all_marts()

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n=== TRANSFORMACOES CONCLUIDAS EM {elapsed:.2f}s ===")

        return {
            "staging": staging_results,
            "marts": marts_results,
            "elapsed_seconds": elapsed,
            "validation_errors": self._validation_errors
        }


def main():
    pipeline = TransformPipeline()
    results = pipeline.run_all()
    print("\nResultados:", results)


if __name__ == "__main__":
    main()