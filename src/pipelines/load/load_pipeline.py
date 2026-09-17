"""
Pipeline de load: staging/marts → destino final
Responsavel por carregar dados transformados para o destino final (BI, API, etc).
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


class LoadPipeline:
    """Pipeline para carregar dados transformados."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)

    def _read_sql(self, query: str) -> pd.DataFrame:
        """Le dados do PostgreSQL."""
        return pd.read_sql(query, self.engine)

    def _execute_sql(self, query: str):
        """Executa uma query SQL."""
        with self.engine.begin() as conn:
            conn.execute(text(query))

    # ==========================================
    # LOAD PARA CSV (exportacao)
    # ==========================================

    def load_to_csv(self, table_name: str, output_dir: str, schema: str = "marts"):
        """Exporta uma tabela para CSV."""
        print(f"Exportando {schema}.{table_name} → {output_dir}/{table_name}.csv")

        df = self._read_sql(f"SELECT * FROM {schema}.{table_name}")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(output_path, index=False)

        print(f"  → {len(df)} registros exportados")
        return len(df)

    # ==========================================
    # LOAD PARA JSON (exportacao)
    # ==========================================

    def load_to_json(self, table_name: str, output_dir: str, schema: str = "marts"):
        """Exporta uma tabela para JSON."""
        print(f"Exportando {schema}.{table_name} → {output_dir}/{table_name}.json")

        df = self._read_sql(f"SELECT * FROM {schema}.{table_name}")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{table_name}.json")
        df.to_json(output_path, orient="records", date_format="iso", indent=2)

        print(f"  → {len(df)} registros exportados")
        return len(df)

    # ==========================================
    # LOAD PARA SCHEMA DE REPORT
    # ==========================================

    def load_to_report_schema(self, table_name: str, source_schema: str = "marts",
                              target_schema: str = "report"):
        """Copia uma tabela para schema de report (para dashboards)."""
        print(f"Carregando {source_schema}.{table_name} → {target_schema}.{table_name}")

        self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        self._execute_sql(f"""
            DROP TABLE IF EXISTS {target_schema}.{table_name};
            CREATE TABLE {target_schema}.{table_name} AS
            SELECT * FROM {source_schema}.{table_name}
        """)

        count = self._read_sql(f"SELECT COUNT(*) as cnt FROM {target_schema}.{table_name}")
        print(f"  → {count['cnt'].iloc[0]} registros carregados")
        return count["cnt"].iloc[0]

    # ==========================================
    # LOAD COMPLETO
    # ==========================================

    def load_all_to_report(self):
        """Carrega todas as tabelas marts para o schema report."""
        print("=== CARREGANDO PARA SCHEMA REPORT ===")

        results = {}
        results["dim_books"] = self.load_to_report_schema("dim_books")
        results["dim_products"] = self.load_to_report_schema("dim_products")

        print(f"\n=== LOAD CONCLUIDO ===")
        return results

    def load_all_to_csv(self, output_dir: str):
        """Exporta todas as tabelas marts para CSV."""
        print("=== EXPORTANDO PARA CSV ===")

        results = {}
        results["dim_books"] = self.load_to_csv("dim_books", output_dir)
        results["dim_products"] = self.load_to_csv("dim_products", output_dir)

        print(f"\n=== EXPORTACAO CONCLUIDA ===")
        return results

    def load_all_to_json(self, output_dir: str):
        """Exporta todas as tabelas marts para JSON."""
        print("=== EXPORTANDO PARA JSON ===")

        results = {}
        results["dim_books"] = self.load_to_json("dim_books", output_dir)
        results["dim_products"] = self.load_to_json("dim_products", output_dir)

        print(f"\n=== EXPORTACAO CONCLUIDA ===")
        return results


def main():
    """Funcao principal para executar o pipeline de load."""
    pipeline = LoadPipeline(
        postgres_url="postgresql://postgres:postgres@localhost:5432/ecommerce"
    )

    # Carregar para schema report
    pipeline.load_all_to_report()

    # Exportar para CSV
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "output")
    pipeline.load_all_to_csv(output_dir)

    # Exportar para JSON
    pipeline.load_all_to_json(output_dir)


if __name__ == "__main__":
    main()
