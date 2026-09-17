"""
Data lineage tracking module.
Registra qual fonte gerou qual tabela, quando, quantos registros.
"""

import os
from datetime import datetime, timezone
from sqlalchemy import create_engine, text


class DataLineage:
    """Rastreia a linhagem dos dados no pipeline ETL."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = None

    def _get_engine(self):
        if not self.engine:
            self.engine = create_engine(self.postgres_url)
        return self.engine

    def _execute_sql(self, query: str, params: dict = None):
        with self._get_engine().begin() as conn:
            conn.execute(text(query), params or {})

    def record_lineage(self, source_name: str, source_type: str,
                         target_schema: str, target_table: str,
                         operation: str, record_count: int,
                         duration_seconds: float = 0,
                         status: str = "success",
                         error_message: str = None,
                         checksum: str = None,
                         parent_lineage_id: int = None):
        """Registra uma entrada na tabela de lineage."""
        sql = """
            INSERT INTO lineage.data_lineage (
                source_name, source_type, target_schema, target_table,
                operation, record_count, duration_seconds, status,
                error_message, checksum, parent_lineage_id
            ) VALUES (
                :source_name, :source_type, :target_schema, :target_table,
                :operation, :record_count, :duration_seconds, :status,
                :error_message, :checksum, :parent_lineage_id
            )
        """
        try:
            self._execute_sql(sql, {
                "source_name": source_name,
                "source_type": source_type,
                "target_schema": target_schema,
                "target_table": target_table,
                "operation": operation,
                "record_count": record_count,
                "duration_seconds": duration_seconds,
                "status": status,
                "error_message": error_message,
                "checksum": checksum,
                "parent_lineage_id": parent_lineage_id,
            })
        except Exception as e:
            print(f"Erro ao registrar lineage: {e}")

    def record_change(self, source_table: str, source_key: str,
                        operation: str):
        """Registra uma mudança na tabela de change tracking (CDC)."""
        sql = """
            INSERT INTO lineage.change_tracking (
                source_table, source_key, operation
            ) VALUES (:source_table, :source_key, :operation)
        """
        try:
            self._execute_sql(sql, {
                "source_table": source_table,
                "source_key": source_key,
                "operation": operation,
            })
        except Exception as e:
            print(f"Erro ao registrar change tracking: {e}")

    def get_lineage_report(self):
        """Retorna um relatório completo de linhagem."""
        engine = self._get_engine()
        import pandas as pd

        df = pd.read_sql("""
            SELECT
                source_name, source_type, target_schema, target_table,
                operation, record_count, execution_time, duration_seconds, status
            FROM lineage.data_lineage
            ORDER BY execution_time DESC
            LIMIT 100
        """, engine)
        return df

    def get_lineage_summary(self):
        """Retorna um resumo da linhagem."""
        engine = self._get_engine()
        import pandas as pd

        df = pd.read_sql("""
            SELECT
                source_name, target_table, operation,
                SUM(record_count) as total_records,
                COUNT(*) as executions,
                MIN(execution_time) as first_run,
                MAX(execution_time) as last_run,
                COUNT(DISTINCT status) as distinct_statuses
            FROM lineage.data_lineage
            GROUP BY source_name, target_table, operation
            ORDER BY source_name, target_table
        """, engine)
        return df

    def get_daily_stats(self, date: str = None):
        """Retorna estatísticas diárias."""
        engine = self._get_engine()
        import pandas as pd

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        df = pd.read_sql(f"""
            SELECT
                target_schema, target_table,
                SUM(record_count) as total_records,
                COUNT(*) as executions
            FROM lineage.data_lineage
            WHERE DATE(execution_time) = '{date}'
            GROUP BY target_schema, target_table
            ORDER BY target_schema, target_table
        """, engine)
        return df

    def print_lineage_report(self):
        """Imprime o relatório de linhagem."""
        print("\n=== DATA LINEAGE REPORT ===")
        summary = self.get_lineage_summary()
        if summary.empty:
            print("  Nenhum registro de linhagem encontrado.")
        else:
            for _, row in summary.iterrows():
                print(f"  {row['source_name']} → {row['target_schema']}.{row['target_table']}: "
                      f"{row['total_records']} registros em {row['executions']} execuções")


# Singleton para uso global
_lineage_tracker = None

def get_lineage_tracker(postgres_url: str = None) -> DataLineage:
    """Retorna o singleton do DataLineage."""
    global _lineage_tracker
    if _lineage_tracker is None:
        _lineage_tracker = DataLineage(postgres_url)
    return _lineage_tracker
