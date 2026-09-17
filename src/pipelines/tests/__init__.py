"""
Testes de qualidade de dados
Responsavel por validar a integridade dos dados transformados.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text


class DataQualityTests:
    """Classe para executar testes de qualidade de dados."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)
        self.results = []

    def _read_sql(self, query: str) -> pd.DataFrame:
        """Le dados do PostgreSQL."""
        return pd.read_sql(query, self.engine)

    def _add_result(self, test_name: str, passed: bool, message: str = ""):
        """Adiciona resultado do teste."""
        status = "PASS" if passed else "FAIL"
        self.results.append({
            "test": test_name,
            "status": status,
            "message": message
        })
        print(f"  [{status}] {test_name}: {message}")

    # ==========================================
    # TESTES DE UNICIDADE
    # ==========================================

    def test_unique(self, table: str, column: str, schema: str = "marts"):
        """Testa se uma coluna tem valores unicos."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        duplicates = df[df.duplicated(subset=[column], keep=False)]

        if len(duplicates) == 0:
            self._add_result(
                f"unique_{table}_{column}",
                True,
                f"Coluna {column} tem valores unicos"
            )
        else:
            self._add_result(
                f"unique_{table}_{column}",
                False,
                f"Encontradas {len(duplicates)} duplicatas em {column}"
            )

    # ==========================================
    # TESTES DE NULIDADE
    # ==========================================

    def test_not_null(self, table: str, column: str, schema: str = "marts"):
        """Testa se uma coluna nao tem valores nulos."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        null_count = df[column].isna().sum()

        if null_count == 0:
            self._add_result(
                f"not_null_{table}_{column}",
                True,
                f"Coluna {column} nao tem valores nulos"
            )
        else:
            self._add_result(
                f"not_null_{table}_{column}",
                False,
                f"Encontrados {null_count} valores nulos em {column}"
            )

    # ==========================================
    # TESTES DE INTEGRIDADE
    # ==========================================

    def test_foreign_key(self, child_table: str, child_column: str,
                         parent_table: str, parent_column: str,
                         child_schema: str = "marts", parent_schema: str = "raw"):
        """Testa se uma chave estrangeira e valida."""
        query = f"""
            SELECT COUNT(*) as invalid_count
            FROM {child_schema}.{child_table} c
            LEFT JOIN {parent_schema}.{parent_table} p
                ON c.{child_column} = p.{parent_column}
            WHERE p.{parent_column} IS NULL
        """
        df = self._read_sql(query)
        invalid_count = df["invalid_count"].iloc[0]

        if invalid_count == 0:
            self._add_result(
                f"fk_{child_table}_{child_column}",
                True,
                f"Chave estrangeira {child_column} e valida"
            )
        else:
            self._add_result(
                f"fk_{child_table}_{child_column}",
                False,
                f"Encontradas {invalid_count} chaves estrangeiras invalidas"
            )

    # ==========================================
    # TESTES DE VALIDACAO
    # ==========================================

    def test_positive_value(self, table: str, column: str, schema: str = "marts"):
        """Testa se uma coluna tem valores positivos."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        negative_count = (df[column] < 0).sum()

        if negative_count == 0:
            self._add_result(
                f"positive_{table}_{column}",
                True,
                f"Coluna {column} tem apenas valores positivos"
            )
        else:
            self._add_result(
                f"positive_{table}_{column}",
                False,
                f"Encontrados {negative_count} valores negativos em {column}"
            )

    def test_value_in_set(self, table: str, column: str, valid_values: list,
                          schema: str = "marts"):
        """Testa se uma coluna tem apenas valores validos."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        invalid_count = (~df[column].isin(valid_values)).sum()

        if invalid_count == 0:
            self._add_result(
                f"valueset_{table}_{column}",
                True,
                f"Coluna {column} tem apenas valores validos"
            )
        else:
            self._add_result(
                f"valueset_{table}_{column}",
                False,
                f"Encontrados {invalid_count} valores invalidos em {column}"
            )

    def test_row_count(self, table: str, min_rows: int = 1, schema: str = "marts"):
        """Testa se uma tabela tem um numero minimo de linhas."""
        df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
        row_count = df["cnt"].iloc[0]

        if row_count >= min_rows:
            self._add_result(
                f"rowcount_{table}",
                True,
                f"Tabela {table} tem {row_count} linhas (minimo: {min_rows})"
            )
        else:
            self._add_result(
                f"rowcount_{table}",
                False,
                f"Tabela {table} tem apenas {row_count} linhas (minimo: {min_rows})"
            )

    # ==========================================
    # EXECUCAO DE TODOS OS TESTES
    # ==========================================

    def run_all_tests(self):
        """Executa todos os testes de qualidade."""
        print("=== EXECUTANDO TESTES DE QUALIDADE ===")

        # Testes de staging
        print("\n--- Staging ---")
        self.test_not_null("stg_books", "name", "staging")
        self.test_not_null("stg_amazon", "name", "staging")
        self.test_not_null("stg_americanas", "name", "staging")
        self.test_not_null("stg_kabum", "name", "staging")

        # Testes de marts
        print("\n--- Marts ---")
        self.test_unique("dim_books", "book_id")
        self.test_not_null("dim_books", "book_name")
        self.test_positive_value("dim_books", "price")
        self.test_row_count("dim_books", 100)

        self.test_unique("dim_products", "id")
        self.test_not_null("dim_products", "product_name")
        self.test_positive_value("dim_products", "price")
        self.test_row_count("dim_products", 100)
        self.test_value_in_set(
            "dim_products", "source",
            ["amazon", "americanas", "kabum"]
        )
        self.test_value_in_set(
            "dim_products", "price_tier",
            ["Economico", "Medio", "Premium", "Luxo"]
        )

        # Resumo
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        total = len(self.results)

        print(f"\n=== RESUMO: {passed}/{total} testes passaram ===")

        if failed > 0:
            print(f"❌ {failed} testes falharam")
            return False
        else:
            print("✅ Todos os testes passaram")
            return True

    def get_results_df(self) -> pd.DataFrame:
        """Retorna os resultados como DataFrame."""
        return pd.DataFrame(self.results)


def main():
    """Funcao principal para executar os testes."""
    tests = DataQualityTests()
    success = tests.run_all_tests()

    if not success:
        exit(1)


if __name__ == "__main__":
    main()
