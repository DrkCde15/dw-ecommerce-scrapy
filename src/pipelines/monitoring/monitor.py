"""
Sistema de monitoramento e alertas
Verifica a saude do pipeline ETL e envia alertas.
"""

import os
import json
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from sqlalchemy import create_engine, text
import requests


class PipelineMonitor:
    """Monitora a saude do pipeline ETL."""

    def __init__(self, postgres_url: str = None):
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL",
            "postgresql://postgres:postgres@localhost:5432/ecommerce"
        )
        self.engine = create_engine(self.postgres_url)
        self.alerts = []

    def _execute_sql(self, query: str):
        """Executa uma query SQL."""
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return result

    def _read_sql(self, query: str):
        """Le dados do PostgreSQL."""
        import pandas as pd
        return pd.read_sql(query, self.engine)

    # ==========================================
    # VERIFICACOES DE SAUDE
    # ==========================================

    def check_database_connection(self) -> bool:
        """Verifica se a conexao com o banco esta funcionando."""
        try:
            self._execute_sql("SELECT 1")
            return True
        except Exception as e:
            self.alerts.append({
                "level": "CRITICAL",
                "message": f"Falha na conexao com PostgreSQL: {e}",
                "timestamp": datetime.now().isoformat()
            })
            return False

    def check_table_exists(self, table_name: str, schema: str = "raw") -> bool:
        """Verifica se uma tabela existe."""
        result = self._execute_sql(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            )
        """)
        exists = result.scalar()
        if not exists:
            self.alerts.append({
                "level": "WARNING",
                "message": f"Tabela {schema}.{table_name} nao existe",
                "timestamp": datetime.now().isoformat()
            })
        return exists

    def check_table_not_empty(self, table_name: str, schema: str = "raw") -> bool:
        """Verifica se uma tabela nao esta vazia."""
        result = self._execute_sql(f"SELECT COUNT(*) FROM {schema}.{table_name}")
        count = result.scalar()
        if count == 0:
            self.alerts.append({
                "level": "WARNING",
                "message": f"Tabela {schema}.{table_name} esta vazia",
                "timestamp": datetime.now().isoformat()
            })
            return False
        return True

    def check_data_freshness(self, table_name: str, schema: str = "raw",
                              max_hours: int = 24) -> bool:
        """Verifica se os dados sao recentes."""
        try:
            result = self._execute_sql(f"""
                SELECT MAX(scraped_at) FROM {schema}.{table_name}
            """)
            last_scraped = result.scalar()
            if last_scraped is None:
                self.alerts.append({
                    "level": "WARNING",
                    "message": f"Tabela {schema}.{table_name} sem dados de scraped_at",
                    "timestamp": datetime.now().isoformat()
                })
                return False

            hours_since = (datetime.now() - last_scraped).total_seconds() / 3600
            if hours_since > max_hours:
                self.alerts.append({
                    "level": "WARNING",
                    "message": f"Dados de {schema}.{table_name} estao {hours_since:.1f}h atrasados (max: {max_hours}h)",
                    "timestamp": datetime.now().isoformat()
                })
                return False
            return True
        except Exception as e:
            # Coluna scraped_at pode nao existir
            return True

    def check_row_count(self, table_name: str, schema: str = "raw",
                         min_rows: int = 1) -> bool:
        """Verifica se a tabela tem um numero minimo de linhas."""
        result = self._execute_sql(f"SELECT COUNT(*) FROM {schema}.{table_name}")
        count = result.scalar()
        if count < min_rows:
            self.alerts.append({
                "level": "WARNING",
                "message": f"Tabela {schema}.{table_name} tem apenas {count} linhas (minimo: {min_rows})",
                "timestamp": datetime.now().isoformat()
            })
            return False
        return True

    def check_duplicates(self, table_name: str, column: str,
                          schema: str = "marts") -> bool:
        """Verifica se ha duplicatas em uma coluna."""
        result = self._execute_sql(f"""
            SELECT COUNT(*) FROM (
                SELECT {column}, COUNT(*) as cnt
                FROM {schema}.{table_name}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            ) t
        """)
        dup_count = result.scalar()
        if dup_count > 0:
            self.alerts.append({
                "level": "WARNING",
                "message": f"Tabela {schema}.{table_name} tem {dup_count} duplicatas em {column}",
                "timestamp": datetime.now().isoformat()
            })
            return False
        return True

    # ==========================================
    # VERIFICACAO COMPLETA
    # ==========================================

    def run_full_check(self) -> dict:
        """Executa todas as verificacoes de saude."""
        print("=== EXECUTANDO VERIFICACOES DE SAUDE ===")
        start_time = datetime.now()

        results = {
            "timestamp": start_time.isoformat(),
            "checks": {},
            "alerts": [],
            "status": "healthy"
        }

        # 1. Conexao
        print("Verificando conexao com PostgreSQL...")
        results["checks"]["database_connection"] = self.check_database_connection()

        if not results["checks"]["database_connection"]:
            results["status"] = "unhealthy"
            results["alerts"] = self.alerts
            return results

        # 2. Tabelas raw
        print("Verificando tabelas raw...")
        raw_tables = ["books", "amazon", "americanas", "kabum"]
        for table in raw_tables:
            results["checks"][f"raw_{table}_exists"] = self.check_table_exists(table, "raw")
            results["checks"][f"raw_{table}_not_empty"] = self.check_table_not_empty(table, "raw")
            results["checks"][f"raw_{table}_freshness"] = self.check_data_freshness(table, "raw", 48)
            results["checks"][f"raw_{table}_min_rows"] = self.check_row_count(table, "raw", 10)

        # 3. Tabelas marts
        print("Verificando tabelas marts...")
        results["checks"]["dim_products_exists"] = self.check_table_exists("dim_products", "marts")
        results["checks"]["dim_products_not_empty"] = self.check_table_not_empty("dim_products", "marts")
        results["checks"]["dim_products_unique_id"] = self.check_duplicates("dim_products", "id", "marts")

        results["checks"]["dim_books_exists"] = self.check_table_exists("dim_books", "marts")
        results["checks"]["dim_books_not_empty"] = self.check_table_not_empty("dim_books", "marts")
        results["checks"]["dim_books_unique_id"] = self.check_duplicates("dim_books", "book_id", "marts")

        # 4. Resumo
        elapsed = (datetime.now() - start_time).total_seconds()
        passed = sum(1 for v in results["checks"].values() if v)
        total = len(results["checks"])

        if passed == total:
            results["status"] = "healthy"
        elif passed >= total * 0.8:
            results["status"] = "degraded"
        else:
            results["status"] = "unhealthy"

        results["alerts"] = self.alerts
        results["summary"] = {
            "passed": passed,
            "total": total,
            "elapsed_seconds": elapsed,
            "status": results["status"]
        }

        print(f"\n=== VERIFICACAO CONCLUIDA EM {elapsed:.2f}s ===")
        print(f"Status: {results['status']}")
        print(f"Checks: {passed}/{total} passaram")

        if self.alerts:
            print(f"\nAlertas ({len(self.alerts)}):")
            for alert in self.alerts:
                print(f"  [{alert['level']}] {alert['message']}")

        return results

    # ==========================================
    # ALERTAS
    # ==========================================

    def send_email_alert(self, to_email: str, subject: str, body: str):
        """Envia alerta por email."""
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")

        if not smtp_user or not smtp_pass:
            print("Configuracao de email nao encontrada")
            return False

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = to_email

        try:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
            print(f"Email enviado para {to_email}")
            return True
        except Exception as e:
            print(f"Erro ao enviar email: {e}")
            return False

    def send_slack_alert(self, webhook_url: str, message: str):
        """Envia alerta para Slack."""
        payload = {"text": message}
        try:
            response = requests.post(webhook_url, json=payload)
            return response.status_code == 200
        except Exception as e:
            print(f"Erro ao enviar para Slack: {e}")
            return False

    def send_telegram_alert(self, bot_token: str, chat_id: str, message: str):
        """Envia alerta para Telegram."""
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}
        try:
            response = requests.post(url, json=payload)
            return response.status_code == 200
        except Exception as e:
            print(f"Erro ao enviar para Telegram: {e}")
            return False

    # ==========================================
    # EXPORTACAO
    # ==========================================

    def export_report(self, output_path: str):
        """Exporta relatorio de saude para JSON."""
        results = self.run_full_check()
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Relatorio exportado para {output_path}")
        return results


def main():
    """Funcao principal para executar o monitoramento."""
    monitor = PipelineMonitor(
        postgres_url="postgresql://postgres:postgres@localhost:5432/ecommerce"
    )

    # Executar verificacao completa
    results = monitor.run_full_check()

    # Exportar relatorio
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "reports")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"health_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    monitor.export_report(output_path)


if __name__ == "__main__":
    main()
