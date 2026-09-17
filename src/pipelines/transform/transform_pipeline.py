"""
Pipeline de transformacao: raw → staging → marts
Responsavel por transformar dados brutos em tabelas de analise.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


class TransformPipeline:
    """Pipeline para transformar dados no PostgreSQL."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)

    def _execute_sql(self, query: str):
        """Executa uma query SQL."""
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def _read_sql(self, query: str) -> pd.DataFrame:
        """Le dados do PostgreSQL para um DataFrame."""
        return pd.read_sql(query, self.engine)

    # ==========================================
    # STAGING MODELS
    # ==========================================

    def transform_stg_books(self):
        """Transform raw.books → staging.stg_books"""
        print("Transformando stg_books...")

        df = self._read_sql("SELECT * FROM raw.books")

        # Limpeza e normalizacao
        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        # Converter rating textual para numerico
        rating_map = {
            "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5
        }
        df["rating"] = df["rating"].map(rating_map).fillna(df["rating"])

        # Adicionar coluna de avaliacao
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

        # Salvar no schema staging
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")
        df.to_sql(
            name="stg_books",
            con=self.engine,
            schema="staging",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_amazon(self):
        """Transform raw.amazon → staging.stg_amazon"""
        print("Transformando stg_amazon...")

        df = self._read_sql("SELECT * FROM raw.amazon")

        # Limpeza e normalizacao
        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["reviews_count"] = pd.to_numeric(df["reviews_count"], errors="coerce").fillna(0).astype(int)

        # Adicionar source fixo
        df["source"] = "amazon"

        # Salvar no schema staging
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")
        df.to_sql(
            name="stg_amazon",
            con=self.engine,
            schema="staging",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_americanas(self):
        """Transform raw.americanas → staging.stg_americanas"""
        print("Transformando stg_americanas...")

        df = self._read_sql("SELECT * FROM raw.americanas")

        # Limpeza e normalizacao
        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
        df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0).astype(int)

        # Adicionar source fixo
        df["source"] = "americanas"

        # Salvar no schema staging
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")
        df.to_sql(
            name="stg_americanas",
            con=self.engine,
            schema="staging",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(df)} registros transformados")
        return len(df)

    def transform_stg_kabum(self):
        """Transform raw.kabum → staging.stg_kabum"""
        print("Transformando stg_kabum...")

        df = self._read_sql("SELECT * FROM raw.kabum")

        # Limpeza e normalizacao
        df["product_id"] = df["product_id"].astype(str)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["old_price"] = pd.to_numeric(df["old_price"], errors="coerce")
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["reviews_count"] = pd.to_numeric(df["reviews_count"], errors="coerce").fillna(0).astype(int)

        # Adicionar source fixo
        df["source"] = "kabum"

        # Salvar no schema staging
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS staging")
        df.to_sql(
            name="stg_kabum",
            con=self.engine,
            schema="staging",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(df)} registros transformados")
        return len(df)

    # ==========================================
    # MARTS MODELS
    # ==========================================

    def transform_dim_books(self):
        """Transform staging.stg_books → marts.dim_books"""
        print("Transformando dim_books...")

        df = self._read_sql("SELECT * FROM staging.stg_books")

        # Calcular media de preco por categoria
        category_avg = df.groupby("category")["price"].mean().reset_index()
        category_avg.columns = ["category", "avg_category_price"]
        df = df.merge(category_avg, on="category", how="left")

        # Calcular comparacao com media da categoria
        df["price_vs_category"] = df.apply(
            lambda row: round(((row["price"] - row["avg_category_price"]) / row["avg_category_price"] * 100), 2)
            if pd.notna(row["avg_category_price"]) and row["avg_category_price"] > 0
            else 0,
            axis=1
        )

        # Criar surrogate key
        df["book_id"] = df["product_id"].apply(lambda x: f"book_{x}")

        # Deduplicar (manter registro mais recente)
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        df = df.sort_values("scraped_at", ascending=False)
        df = df.drop_duplicates(subset=["product_id"], keep="first")

        # Selecionar colunas
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

        # Salvar no schema marts
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS marts")
        dim_df.to_sql(
            name="dim_books",
            con=self.engine,
            schema="marts",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(dim_df)} registros transformados")
        return len(dim_df)

    def transform_dim_products(self):
        """Transform staging.stg_* → marts.dim_products"""
        print("Transformando dim_products...")

        # Ler dados de todas as fontes
        amazon = self._read_sql("SELECT * FROM staging.stg_amazon")
        americanas = self._read_sql("SELECT * FROM staging.stg_americanas")
        kabum = self._read_sql("SELECT * FROM staging.stg_kabum")

        # Padronizar colunas
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

        # Combinar todas as fontes
        all_products = pd.concat([amazon_prep, americanas_prep, kabum_prep], ignore_index=True)

        # Filtrar dados invalidos
        all_products = all_products[
            all_products["product_name"].notna() &
            (all_products["price"] > 0)
        ]

        # Calcular desconto
        all_products["discount_percentage"] = all_products.apply(
            lambda row: round(((row["original_price"] - row["price"]) / row["original_price"] * 100), 2)
            if row["original_price"] > 0 and row["original_price"] > row["price"]
            else 0,
            axis=1
        )

        # Classificar faixa de preco
        def price_tier(price):
            if price < 100:
                return "Economico"
            if price < 500:
                return "Medio"
            if price < 2000:
                return "Premium"
            return "Luxo"

        all_products["price_tier"] = all_products["price"].apply(price_tier)

        # Criar surrogate key
        all_products["id"] = all_products.apply(
            lambda row: f"{row['product_id']}_{row['source']}", axis=1
        )

        # Deduplicar (manter registro mais recente)
        all_products["scraped_at"] = pd.to_datetime(all_products["scraped_at"])
        all_products = all_products.sort_values("scraped_at", ascending=False)
        all_products = all_products.drop_duplicates(subset=["product_id", "source"], keep="first")

        # Selecionar colunas finais
        dim_df = all_products[[
            "id", "product_id", "product_name", "brand", "price",
            "original_price", "discount_percentage", "category",
            "seller", "image_url", "url", "source", "price_tier",
            "scraped_at"
        ]].copy()

        # Salvar no schema marts
        self._execute_sql("CREATE SCHEMA IF NOT EXISTS marts")
        dim_df.to_sql(
            name="dim_products",
            con=self.engine,
            schema="marts",
            if_exists="replace",
            index=False
        )

        print(f"  → {len(dim_df)} registros transformados")
        return len(dim_df)

    # ==========================================
    # EXECUCAO COMPLETA
    # ==========================================

    def run_all_staging(self):
        """Executa todas as transformacoes de staging."""
        print("=== STAGING ===")
        results = {}
        results["stg_books"] = self.transform_stg_books()
        results["stg_amazon"] = self.transform_stg_amazon()
        results["stg_americanas"] = self.transform_stg_americanas()
        results["stg_kabum"] = self.transform_stg_kabum()
        return results

    def run_all_marts(self):
        """Executa todas as transformacoes de marts."""
        print("=== MARTS ===")
        results = {}
        results["dim_books"] = self.transform_dim_books()
        results["dim_products"] = self.transform_dim_products()
        return results

    def run_all(self):
        """Executa todas as transformacoes."""
        print("=== INICIANDO TRANSFORMACOES ===")
        start_time = datetime.now()

        staging_results = self.run_all_staging()
        marts_results = self.run_all_marts()

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n=== TRANSFORMACOES CONCLUIDAS EM {elapsed:.2f}s ===")

        return {
            "staging": staging_results,
            "marts": marts_results,
            "elapsed_seconds": elapsed
        }


def main():
    """Funcao principal para executar o pipeline de transformacao."""
    pipeline = TransformPipeline()
    results = pipeline.run_all()
    print("\nResultados:", results)


if __name__ == "__main__":
    main()
