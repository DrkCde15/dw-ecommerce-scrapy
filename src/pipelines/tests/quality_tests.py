"""
Testes de qualidade de dados - validacao em pipeline.
Responsavel por validar a integridade dos dados em cada etapa do ETL.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


class DataQualityTests:
    """Classe para executar testes de qualidade de dados em pipeline."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)
        self.results = []
        self.stage = "unknown"

    def _read_sql(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.engine)

    def _add_result(self, test_name: str, passed: bool, message: str = ""):
        status = "PASS" if passed else "FAIL"
        self.results.append({
            "stage": self.stage,
            "test": test_name,
            "status": status,
            "message": message
        })
        print(f"  [{status}] {test_name}: {message}")

    def set_stage(self, stage: str):
        """Define o stage atual para rastreamento."""
        self.stage = stage

    # ==========================================
    # EXTRACT STAGE VALIDATION
    # ==========================================

    def validate_extract_schema(self, table: str, schema: str = "raw",
                                   required_columns: list[str] = None):
        """Schema validation at extract stage."""
        try:
            df = self._read_sql(f"SELECT * FROM {schema}.{table} LIMIT 1")
            cols = list(df.columns)
            if required_columns:
                missing = [c for c in required_columns if c not in cols]
                if missing:
                    self._add_result(
                        f"extract_schema_{table}",
                        False,
                        f"Colunas faltando em {schema}.{table}: {missing}"
                    )
                else:
                    self._add_result(
                        f"extract_schema_{table}",
                        True,
                        f"Schema {schema}.{table} validado"
                    )
            else:
                self._add_result(
                    f"extract_schema_{table}",
                    True,
                    f"Schema {schema}.{table} possui {len(cols)} colunas"
                )
        except Exception as e:
            self._add_result(f"extract_schema_{table}", False, str(e))

    def validate_extract_row_count(self, table: str, schema: str = "raw",
                                      min_rows: int = 1):
        """Row count check at extract stage."""
        df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
        count = df["cnt"].iloc[0]
        passed = count >= min_rows
        self._add_result(
            f"extract_rowcount_{table}",
            passed,
            f"{schema}.{table} tem {count} registros (minimo: {min_rows})"
        )

    def validate_extract_not_empty(self, table: str, schema: str = "raw"):
        """Check if table has data after extract."""
        df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
        count = df["cnt"].iloc[0]
        passed = count > 0
        self._add_result(
            f"extract_notempty_{table}",
            passed,
            f"{schema}.{table} {'tem' if passed else 'vazio'} ({count} registros)"
        )

    # ==========================================
    # TRANSFORM STAGE VALIDATION
    # ==========================================

    def validate_transform_nulls(self, table: str, columns: list[str],
                                    schema: str = "staging"):
        """Null check at transform stage."""
        for col in columns:
            df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table} WHERE {col} IS NULL")
            null_count = df["cnt"].iloc[0]
            passed = null_count == 0
            self._add_result(
                f"transform_null_{table}_{col}",
                passed,
                f"{schema}.{table}.{col}: {null_count} nulos"
            )

    def validate_transform_row_count(self, table: str, min_rows: int = 1,
                                        schema: str = "staging"):
        """Row count check at transform stage."""
        df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
        count = df["cnt"].iloc[0]
        passed = count >= min_rows
        self._add_result(
            f"transform_rowcount_{table}",
            passed,
            f"{schema}.{table} tem {count} registros (minimo: {min_rows})"
        )

    def validate_transform_duplicates(self, table: str, column: str,
                                         schema: str = "staging"):
        """Duplicate check at transform stage."""
        df = self._read_sql(f"""
            SELECT COUNT(*) as cnt FROM (
                SELECT {column}, COUNT(*) as c
                FROM {schema}.{table}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            ) t
        """)
        dup_count = df["cnt"].iloc[0]
        passed = dup_count == 0
        self._add_result(
            f"transform_dedup_{table}_{column}",
            passed,
            f"{schema}.{table}: {dup_count} duplicatas em {column}"
        )

    def validate_transform_data_types(self, table: str, expected_types: dict,
                                         schema: str = "staging"):
        """Validate data types at transform stage."""
        df = self._read_sql(f"SELECT * FROM {schema}.{table} LIMIT 100")
        for col, expected_type in expected_types.items():
            if col in df.columns:
                if expected_type == "numeric":
                    invalid = df[col].apply(lambda x: not isinstance(x, (int, float)) and pd.notna(x)).sum()
                    passed = invalid == 0
                    self._add_result(
                        f"transform_dtype_{table}_{col}",
                        passed,
                        f"{schema}.{table}.{col}: {invalid} valores nao numericos"
                    )

    # ==========================================
    # LOAD STAGE VALIDATION
    # ==========================================

    def validate_load_referential_integrity(self, child_table: str, child_column: str,
                                               parent_table: str, parent_column: str,
                                               child_schema: str = "marts", parent_schema: str = "marts"):
        """Referential integrity check at load stage."""
        df = self._read_sql(f"""
            SELECT COUNT(*) as invalid_count
            FROM {child_schema}.{child_table} c
            LEFT JOIN {parent_schema}.{parent_table} p
                ON c.{child_column} = p.{parent_column}
            WHERE p.{parent_column} IS NULL
        """)
        invalid_count = df["invalid_count"].iloc[0]
        passed = invalid_count == 0
        self._add_result(
            f"load_fk_{child_table}_{child_column}",
            passed,
            f"{child_schema}.{child_table}.{child_column}: {invalid_count} chaves invalidas"
        )

    def validate_load_unique(self, table: str, column: str, schema: str = "marts"):
        """Uniqueness check at load stage."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        duplicates = df[df.duplicated(subset=[column], keep=False)]
        passed = len(duplicates) == 0
        self._add_result(
            f"load_unique_{table}_{column}",
            passed,
            f"{schema}.{table}.{column}: {'OK' if passed else f'{len(duplicates)} duplicatas'} "
        )

    def validate_load_not_null(self, table: str, column: str, schema: str = "marts"):
        """Not-null check at load stage."""
        df = self._read_sql(f"SELECT {column} FROM {schema}.{table}")
        null_count = df[column].isna().sum()
        passed = null_count == 0
        self._add_result(
            f"load_notnull_{table}_{column}",
            passed,
            f"{schema}.{table}.{column}: {null_count} nulos"
        )

    def validate_load_row_count(self, table: str, min_rows: int = 1, schema: str = "marts"):
        """Row count check at load stage."""
        df = self._read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
        count = df["cnt"].iloc[0]
        passed = count >= min_rows
        self._add_result(
            f"load_rowcount_{table}",
            passed,
            f"{schema}.{table} tem {count} registros (minimo: {min_rows})"
        )

    # ==========================================
    # CDC VALIDATION
    # ==========================================

    def validate_cdc_columns(self, table: str, schema: str = "raw",
                                required_columns: list[str] = None):
        """Check that CDC columns exist."""
        if required_columns is None:
            required_columns = ["updated_at", "_extract_ts", "deleted_at"]
        df = self._read_sql(f"SELECT * FROM {schema}.{table} LIMIT 1")
        cols = list(df.columns)
        missing = [c for c in required_columns if c not in cols]
        passed = len(missing) == 0
        self._add_result(
            f"cdc_{table}",
            passed,
            f"{schema}.{table}: colunas CDC {'OK' if passed else f'faltando: {missing}'}"
        )

    # ==========================================
    # DATA LINEAGE VALIDATION
    # ==========================================

    def validate_lineage_exists(self):
        """Check that data lineage table has entries."""
        df = self._read_sql("SELECT COUNT(*) as cnt FROM lineage.data_lineage")
        count = df["cnt"].iloc[0]
        passed = count > 0
        self._add_result(
            f"lineage_exists",
            passed,
            f"lineage.data_lineage tem {count} registros"
        )
        return passed

    # ==========================================
    # EXECUCAO DE TODOS OS TESTES
    # ==========================================

    def run_all_tests(self):
        """Executa todos os testes de qualidade em todas as etapas."""
        print("=== EXECUTANDO TESTES DE QUALIDADE EM PIPELINE ===")
        print()

        # EXTRACT STAGE
        print("\n--- EXTRACT Stage ---")
        self.set_stage("extract")
        self.validate_extract_schema("books", "raw", ["product_id", "name", "price", "source", "_extract_ts"])
        self.validate_extract_schema("amazon", "raw", ["product_id", "name", "price", "_extract_ts"])
        self.validate_extract_schema("americanas", "raw", ["product_id", "name", "_extract_ts"])
        self.validate_extract_schema("kabum", "raw", ["product_id", "name", "_extract_ts"])
        self.validate_extract_not_empty("books", "raw")
        self.validate_extract_not_empty("amazon", "raw")
        self.validate_extract_not_empty("americanas", "raw")
        self.validate_extract_not_empty("kabum", "raw")

        # TRANSFORM STAGE
        print("\n--- TRANSFORM Stage ---")
        self.set_stage("transform")
        self.validate_transform_nulls("stg_books", ["product_id", "name"], "staging")
        self.validate_transform_nulls("stg_amazon", ["product_id", "name"], "staging")
        self.validate_transform_nulls("stg_americanas", ["product_id", "name"], "staging")
        self.validate_transform_nulls("stg_kabum", ["product_id", "name"], "staging")
        self.validate_transform_duplicates("stg_books", "product_id", "staging")
        self.validate_transform_row_count("stg_books", min_rows=0, schema="staging")

        # LOAD STAGE
        print("\n--- LOAD Stage ---")
        self.set_stage("load")
        self.validate_load_unique("dim_books", "book_id", "marts")
        self.validate_load_unique("dim_products", "id", "marts")
        self.validate_load_not_null("dim_books", "book_name", "marts")
        self.validate_load_not_null("dim_products", "product_name", "marts")
        self.validate_load_row_count("dim_books", min_rows=0, schema="marts")
        self.validate_load_row_count("dim_products", min_rows=0, schema="marts")

        # CDC
        print("\n--- CDC Validation ---")
        self.set_stage("cdc")
        self.validate_cdc_columns("books", "raw")
        self.validate_cdc_columns("amazon", "raw")
        self.validate_cdc_columns("americanas", "raw")
        self.validate_cdc_columns("kabum", "raw")

        # DATA LINEAGE
        print("\n--- Data Lineage ---")
        self.set_stage("lineage")
        self.validate_lineage_exists()

        # Resumo
        print()
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        total = len(self.results)

        print(f"\n=== RESUMO: {passed}/{total} testes passaram ===")

        if failed > 0:
            print(f"❌ {failed} testes falharam")
            print("\nDetalhes dos failures:")
            for r in self.results:
                if r["status"] == "FAIL":
                    print(f"  [{r['stage']}] {r['test']}: {r['message']}")
            return False
        else:
            print("✅ Todos os testes passaram")
            return True

    def get_results_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.results)

    def get_results_by_stage(self, stage: str) -> list:
        return [r for r in self.results if r["stage"] == stage]

    def get_failed_tests(self) -> list:
        return [r for r in self.results if r["status"] == "FAIL"]


def main():
    tests = DataQualityTests()
    success = tests.run_all_tests()

    if not success:
        exit(1)


if __name__ == "__main__":
    main()