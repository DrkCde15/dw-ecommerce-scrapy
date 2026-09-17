"""
Pipeline de armazenamento: CSV/JSON → PostgreSQL
Responsavel por carregar dados brutos do Scrapy para o banco de dados.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path


class StoragePipeline:
    """Pipeline para armazenar dados brutos no PostgreSQL."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)

    def load_csv_to_table(self, csv_path: str, table_name: str, schema: str = "raw"):
        """Carrega um arquivo CSV para uma tabela no PostgreSQL."""
        print(f"Carregando {csv_path} → {schema}.{table_name}")

        df = pd.read_csv(csv_path)
        print(f"  → {len(df)} registros encontrados")

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.scalar()
        print(f"  → {count} registros totais na tabela")
        return len(df)

    def load_json_to_table(self, json_path: str, table_name: str, schema: str = "raw"):
        """Carrega um arquivo JSON para uma tabela no PostgreSQL."""
        print(f"Carregando {json_path} → {schema}.{table_name}")

        df = pd.read_json(json_path)
        print(f"  → {len(df)} registros encontrados")

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.scalar()
        print(f"  → {count} registros totais na tabela")
        return len(df)

    def load_all_from_directory(self, data_dir: str):
        """Carrega todos os CSVs e JSONs de um diretorio."""
        data_path = Path(data_dir)
        results = {}

        for csv_file in data_path.glob("*.csv"):
            table_name = csv_file.stem
            try:
                count = self.load_csv_to_table(str(csv_file), table_name)
                results[table_name] = {"status": "success", "count": count}
            except Exception as e:
                print(f"  → ERRO: {e}")
                results[table_name] = {"status": "error", "error": str(e)}

        for json_file in data_path.glob("*.json"):
            table_name = json_file.stem
            try:
                count = self.load_json_to_table(str(json_file), table_name)
                results[table_name] = {"status": "success", "count": count}
            except Exception as e:
                print(f"  → ERRO: {e}")
                results[table_name] = {"status": "error", "error": str(e)}

        return results

    def truncate_table(self, table_name: str, schema: str = "raw"):
        """Limpa uma tabela antes de recarregar."""
        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name} CASCADE"))
        print(f"  → Tabela {schema}.{table_name} limpa")

    def table_exists(self, table_name: str, schema: str = "raw") -> bool:
        """Verifica se uma tabela existe."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
            ))
            return result.scalar()


def main():
    """Funcao principal para executar o pipeline de armazenamento."""
    pipeline = StoragePipeline()

    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
    if os.path.exists(data_dir):
        results = pipeline.load_all_from_directory(data_dir)
        print("\n=== Resultados ===")
        for table, result in results.items():
            print(f"  {table}: {result}")
    else:
        print(f"Diretorio de dados nao encontrado: {data_dir}")


if __name__ == "__main__":
    main()
