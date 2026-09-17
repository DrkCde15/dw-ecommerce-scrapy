# Data Warehouse - E-commerce

## Visao Geral

Projeto de Data Warehouse para analise de dados de e-commerce utilizando **PostgreSQL**, **Python** e **Scrapy** para web scraping com pipeline ETL robusto.

## Arquitetura ETL Corrigida

```
Spiders (Scrapy) → JSON/Parquet (arquivos) → StoragePipeline (UPSERT + dedup) → PostgreSQL raw.*
                                                              ↓
                                                    TransformPipeline (incremental UPSERT)
                                                              ↓
                                                    Staging → Marts → Report
                                                              ↓
                                                    Quality Tests + Data Lineage
```

### Fluxo ETL por Estágio

| Estágio | Entrada → Saída | Validacao |
|---------|-----------------|-----------|
| **Extract** | Spiders → JSON/Parquet files | Schema validation, row count |
| **Load** | Files → `raw.*` (UPSERT + dedup) | CDC columns, referential integrity |
| **Transform** | `raw.*` → `staging` → `marts` | Null checks, duplicate checks |
| **Load Report** | `marts` → `report` | Uniqueness, not-null |
| **Validate** | All stages | Full quality suite |
| **Monitor** | Pipeline health | Alerts, status tracking |

### Arquitetura Desacoplada (ETL Correto)

- **Extract**: Spiders escrevem em `data/raw/{spider}/*.json|parquet` via `FileExportPipeline` — **NÃO** inserem direto no PostgreSQL
- **Load**: `StoragePipeline` carrega arquivos → `raw.*` com **UPSERT**, **deduplicação** e **colunas CDC** (`updated_at`, `deleted_at`, `_extract_ts`)
- **Transform**: `TransformPipeline` usa **incremental loading** com `ON CONFLICT DO UPDATE` — **NÃO** mais `if_exists="replace"`
- **Validate**: Quality tests executam **em cada estágio** (extract, transform, load) — **NÃO** apenas post-hoc
- **Orchestrate**: Airflow DAG com `trigger_rule="all_success"` e `retry_exponential_backoff=True`
- **Lineage**: `DataLineage` rastreia source → table, timestamp, record_count em `lineage.data_lineage`

## Stack

| Camada | Tecnologia | Descricao |
|--------|------------|-----------|
| Scraping | Scrapy | Coleta de dados da web |
| Extract | FileExportPipeline | Spiders → JSON/Parquet |
| Load | StoragePipeline | Files → PostgreSQL (UPSERT + dedup + CDC) |
| Transform | Python + pandas | Incremental UPSERT |
| Quality | DataQualityTests | Validation em cada estágio |
| Lineage | DataLineage | Rastreamento de origem |
| Orquestracao | Airflow 2.10 | DAGs com depends_on_downstream |
| Banco | PostgreSQL 16 | Data Warehouse |
| Dashboard | Streamlit | Visualizacao interativa |

## Pre-requisitos

- Python 3.14+
- Podman + podman-compose

## Instalacao

```bash
# 1. Subir PostgreSQL + Airflow
podman-compose up -d

# 2. Instalar dependencias
pip install -r requirements.txt
pip install -r airflow/requirements.txt

# 3. Acessar Airflow
# http://localhost:8080 (admin/admin)
```

## Web Scraping

### Spiders Disponiveis

| Spider | Status | Metodo | Descricao | Uso |
|--------|--------|--------|-----------|-----|
| `books` | Funcional | HTML | Livros de books.toscrape.com | `scrapy crawl books` |
| `amazon` | Funcional | HTML | Produtos da Amazon.com.br | `scrapy crawl amazon -a query="notebook"` |
| `americanas` | Funcional | VTEX API (JSON) | Produtos da Americanas.com.br | `scrapy crawl americanas -a query="notebook"` |
| `kabum` | Funcional | API interna (JSON) | Produtos do KaBuM.com.br | `scrapy crawl kabum -a query="notebook"` |
| `configurable` | Funcional | HTML/JSON | Generico via JSON/YAML | `scrapy crawl configurable -a config=configs/example.yml` |
| `mercadolivre` | Bloqueado | - | ML requer OAuth2 (login) | `scrapy crawl mercadolivre -a query="iphone"` |
| `products` | Generico | HTML | Para e-commerce | `scrapy crawl products -a url=URL` |

### Exemplos de Uso

```bash
# Raspar livros (gera JSON/Parquet em data/raw/)
scrapy crawl books

# Raspar notebooks na Amazon
scrapy crawl amazon -a query="notebook" -a pages=3

# Raspar notebooks na Americanas (VTEX API)
scrapy crawl americanas -a query="notebook" -a limit=100

# Raspar notebooks no KaBuM (API interna)
scrapy crawl kabum -a query="notebook" -a limit=100

# Raspar com configuracao personalizada
scrapy crawl configurable -a config=configs/books_toscrape.yml
```

### Pipelines Scrapy (Extract)

| Pipeline | Prioridade | Funcao |
|----------|------------|--------|
| `CleaningPipeline` | 100 | Normaliza nomes, precos, categorias |
| `ValidationPipeline` | 200 | Rejeita campos obrigatorios faltando |
| `DuplicatesFilterPipeline` | 300 | Remove duplicatas por product_id |
| `FileExportPipeline` | 400 | Exporta para JSON/Parquet (Extract → arquivo) |

> **Nota**: `PostgresPipeline` foi removido dos spiders. Os spiders agora escrevem em arquivos JSON/Parquet, e o `StoragePipeline` faz o Load separado.

## Pipelines Python

### Estrutura

```
src/pipelines/
├── storage/storage_pipeline.py    # Arquivos JSON/Parquet → PostgreSQL (UPSERT + dedup + CDC)
├── transform/transform_pipeline.py # raw → staging → marts (incremental UPSERT)
├── load/load_pipeline.py          # marts → report (CSV/JSON)
├── tests/quality_tests.py         # Testes de qualidade por estágio
├── monitoring/
│   ├── monitor.py                 # Health checks e alertas
│   └── lineage.py                 # Data lineage tracking
└── __init__.py
```

### Storage Pipeline (Load)

Carrega dados de arquivos JSON/Parquet para o PostgreSQL com UPSERT e deduplicação:

```python
from src.pipelines.storage.storage_pipeline import StoragePipeline

pipeline = StoragePipeline()
pipeline.load_json_to_table("data/raw/books/books_*.json", "books", "raw", conflict_cols=["product_id", "source"])
pipeline.load_all_from_directory()  # Carrega todos os arquivos de data/raw/
pipeline.log_lineage()  # Imprime e persiste data lineage
pipeline.close()
```

**Características:**
- UPSERT com `ON CONFLICT DO UPDATE`
- Deduplicação mantendo o registro mais recente
- Colunas CDC: `updated_at`, `deleted_at`, `_extract_ts`
- Data lineage tracking no banco (`lineage.data_lineage`)

### Transform Pipeline

Transforma dados brutos em tabelas de análise com incremental loading:

```python
from src.pipelines.transform.transform_pipeline import TransformPipeline

pipeline = TransformPipeline()
results = pipeline.run_all()
# Retorna: {'staging': {...}, 'marts': {...}, 'elapsed_seconds': 1.65, 'validation_errors': []}
```

**Funções de transformação:**
- `transform_stg_books()` - Livros (incremental UPSERT)
- `transform_stg_amazon()` - Amazon
- `transform_stg_americanas()` - Americanas
- `transform_stg_kabum()` - KaBuM
- `transform_dim_books()` - Dimensão de livros (incremental UPSERT)
- `transform_dim_products()` - Dimensão unificada (incremental UPSERT)

**Características:**
- Incremental loading com `ON CONFLICT DO UPDATE` (não usa mais `if_exists="replace"`)
- Validação em pipeline: row count, null checks
- Colunas CDC atualizadas automaticamente

### Load Pipeline

Exporta dados transformados:

```python
from src.pipelines.load.load_pipeline import LoadPipeline

pipeline = LoadPipeline()
pipeline.load_all_to_report()      # marts → report
pipeline.load_all_to_csv("output") # marts → CSV
pipeline.load_all_to_json("output") # marts → JSON
```

### Quality Tests (Validação em Pipeline)

Valida qualidade dos dados em **cada estágio** do ETL:

```python
from src.pipelines.tests.quality_tests import DataQualityTests

tests = DataQualityTests()
success = tests.run_all_tests()
# Testes por estágio: extract, transform, load, cdc, lineage
```

**Testes por estágio:**
- **Extract**: `validate_extract_schema()`, `validate_extract_not_empty()`, `validate_cdc_columns()`
- **Transform**: `validate_transform_nulls()`, `validate_transform_duplicates()`, `validate_transform_row_count()`
- **Load**: `validate_load_referential_integrity()`, `validate_load_unique()`, `validate_load_not_null()`
- **CDC**: `validate_cdc_columns()`
- **Lineage**: `validate_lineage_exists()`

### Data Lineage

Rastreia a origem dos dados no pipeline:

```python
from src.pipelines.monitoring.lineage import get_lineage_tracker

tracker = get_lineage_tracker()
tracker.record_lineage(source_name="data/raw/books", source_type="file", ...)
tracker.print_lineage_report()
tracker.get_lineage_summary()
tracker.get_daily_stats("2024-01-15")
```

## Airflow

### DAG `ecommerce_etl`

```
run_spiders >> extract >> validate_extract >> transform >> validate_transform >> load_report >> validate_load >> quality_tests >> monitoring
  (BashOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)  (PythonOperator)
```

**Orquestração:**
- **`trigger_rule="all_success"`** em cada stage (equivalent a `depends_on_downstream` — se um stage falhar, os subsequentes não executam)
- **`retry_exponential_backoff=True`** — retries com backoff exponencial (30s, 60s, 120s)
- **`max_active_runs=1`** — evita execuções concorrentes

**Stages:**
1. `run_spiders` — Executa spiders via `run_spiders.sh` com retry
2. `extract` — StoragePipeline carrega arquivos → `raw.*`
3. `validate_extract` — Schema validation, CDC columns
4. `transform` — Raw → staging → marts (incremental UPSERT)
5. `validate_transform` — Null checks, duplicate checks
6. `load_report` — Marts → report schema
7. `validate_load` — Referential integrity, uniqueness
8. `quality_tests` — Full quality suite
9. `monitoring` — Pipeline health check

### run_spiders.sh

- Retry com **backoff exponencial** (5s, 10s, 20s)
- Até 3 tentativas por spider
- Falha parcial: continua outros spiders
- Exit 1 se algum spider falhar após retries

### Comandos uteis

```bash
# Ver status dos containers
podman ps

# Ver logs do Airflow
podman logs ecommerce_airflow_webserver
podman logs ecommerce_airflow_scheduler

# Listar DAGs
podman exec ecommerce_airflow_webserver airflow dags list

# Ativar DAG
podman exec ecommerce_airflow_webserver airflow dags unpause ecommerce_etl

# Rodar DAG manualmente
podman exec ecommerce_airflow_webserver airflow dags trigger ecommerce_etl
```

## Dashboard

### Acesso

| Servico | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |

### Secoes do Dashboard

| Secao | Descricao |
|-------|-----------|
| KPIs Gerais | Total de produtos, preco medio, min/max, fontes ativas |
| Volume de Scraping | Produtos por fonte, timeline, grafico de pizza |
| Analise de Precos | Boxplot, histograma, violino, top 10 mais caros |
| Analise Avancada | Preco medio, dispersao, categorias, heatmap |
| Dados | Tabelas filtraveis (raw, dim products, dim books) |

### Rodar localmente

```bash
# Instalar dependencias
pip install -r requirements.txt

# Iniciar PostgreSQL
podman-compose up -d postgres

# Rodar Streamlit
streamlit run dashboard/app.py
```

### Rodar via Docker

```bash
# Subir tudo (PostgreSQL + Airflow + Dashboard)
podman-compose up -d

# Ou apenas o dashboard
podman-compose up -d dashboard
```

## Podman

```bash
# Subir PostgreSQL
podman-compose up -d

# Parar
podman-compose down

# Ver logs
podman-compose logs postgres

# Acessar psql
podman exec -it ecommerce_postgres psql -U postgres -d ecommerce
```

## Estrutura do Projeto

```
03-data-warehouse-ecommerce/
├── docker-compose.yml
├── scrapy.cfg
├── requirements.txt
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   │   ├── ecommerce_etl.py
│   │   └── run_spiders.sh
│   └── logs/
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── notebooks/
│   ├── 01_analise_vendas.ipynb
│   ├── 02_qualidade_dados.ipynb
│   └── 03_comparativo_fontes.ipynb
├── configs/
│   ├── books_toscrape.yml
│   └── mercadolivre_celulares.json
├── src/
│   ├── scraping/
│   │   └── ecommerce_scraper/
│   │       ├── settings.py
│   │       ├── pipelines.py
│   │       └── spiders/
│   │           ├── books_spider.py
│   │           ├── amazon_spider.py
│   │           ├── americanas_spider.py
│   │           ├── kabum_spider.py
│   │           ├── mercadolivre_spider.py
│   │           ├── configurable_spider.py
│   │           └── products_spider.py
│   └── pipelines/
│       ├── storage/storage_pipeline.py    # Arquivos → PostgreSQL (UPSERT + dedup + CDC)
│       ├── transform/transform_pipeline.py # Incremental UPSERT
│       ├── load/load_pipeline.py          # marts → report
│       ├── monitoring/
│       │   ├── monitor.py                 # Health checks
│       │   └── lineage.py                 # Data lineage tracking
│       ├── tests/
│       │   ├── __init__.py
│       │   └── quality_tests.py           # Validation por estágio
│       └── __init__.py
├── sql/
│   ├── init/
│   │   └── init_schema.sql              # CDC columns + lineage tables
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── data/
├── tests/
│   └── scraping/
│       ├── test_spiders.py
│       └── test_pipelines.py
```

## Modelo de Dados

### Fluxo de Dados (ETL Correto)

```
books.toscrape.com ──→ data/raw/books/*.json|parquet ──→ StoragePipeline ──→ raw.books
amazon.com.br      ──→ data/raw/amazon/*.json|parquet ──→ StoragePipeline ──→ raw.amazon
americanas.com.br  ──→ data/raw/americanas/*.json|parquet ──→ StoragePipeline ──→ raw.americanas
kabum.com.br       ──→ data/raw/kabum/*.json|parquet ──→ StoragePipeline ──→ raw.kabum

raw.* ──→ TransformPipeline (incremental UPSERT) ──→ staging.* ──→ marts.* ──→ report.*
                                                                    ↓
                                                          lineage.data_lineage
                                                          lineage.change_tracking
```

### Tabelas

| Schema | Tabela | Registros | Descricao |
|--------|--------|-----------|-----------|
| `raw` | `books` | 12.000 | Livros brutos (com CDC columns) |
| `raw` | `amazon` | 535 | Produtos Amazon brutos |
| `raw` | `americanas` | 248 | Produtos Americanas brutos |
| `raw` | `kabum` | 250 | Produtos KaBuM brutos |
| `staging` | `stg_books` | 12.000 | Livros normalizados |
| `staging` | `stg_amazon` | 535 | Amazon normalizado |
| `staging` | `stg_americanas` | 248 | Americanas normalizado |
| `staging` | `stg_kabum` | 250 | KaBuM normalizado |
| `marts` | `dim_books` | 1.000 | Dimensao de livros |
| `marts` | `dim_products` | 347 | Dimensao unificada |
| `lineage` | `data_lineage` | - | Rastreamento de origem |
| `lineage` | `change_tracking` | - | CDC change tracking |

### Colunas CDC em todas as tabelas raw

Todas as tabelas no schema `raw` possuem:
- `updated_at` — Timestamp da última atualização
- `deleted_at` — Timestamp de exclusão lógica (NULL se ativo)
- `_extract_ts` — Timestamp da extração original
- `UNIQUE(product_id, source)` — Constraint para UPSERT

---

## Roadmap

### 1. Novos Spiders (curto prazo)

- [x] Spider para Amazon.com.br
- [x] Spider para Americanas.com.br (VTEX API)
- [x] Spider para KaBuM.com.br (API interna)
- [x] Spider generico configuravel via JSON/YAML
- [ ] Spider para Mercado Livre (requer OAuth2)
- [ ] Spider para Magazine Luiza (requer proxy residencial)
- [ ] Spider para Casas Bahia (requer proxy residencial)

### 2. Integracao scraping-transform (curto prazo)

- [x] Criar stg_books
- [x] Criar dim_books com metricas
- [x] Criar stg_amazon
- [x] Criar stg_americanas
- [x] Criar stg_kabum
- [x] Criar dim_products (unificado)
- [ ] Criar stg_mercadolivre (pendente OAuth2)

### 3. Automacao (medio prazo)

- [x] DAG no Airflow para rodar scraping diario
- [x] Schedule de transform apos scraping
- [x] Load para schema report
- [x] **Extract/Load desacoplado** (FileExportPipeline + StoragePipeline)
- [x] **Incremental loading** (UPSERT ao inves de replace)
- [x] **Quality checks em pipeline** (validate por estágio)
- [x] **Data lineage tracking** (lineage.data_lineage)
- [x] **CDC columns** (updated_at, deleted_at, _extract_ts)
- [x] **retry_exponential_backoff** no Airflow DAG
- [ ] Alertas quando scraping falhar
- [ ] Notificacao via Telegram/Slack

### 4. Data Quality (medio prazo)

- [x] Testes de qualidade em Python (14/14)
- [x] **Validation em cada estágio** (extract, transform, load)
- [ ] Testes de consistencia entre scraping e banco
- [ ] Monitoramento de volume de dados
- [ ] Alertas de anomalias (precos, volume, etc)

### 5. Visualizacao (medio/longo prazo)

- [x] Dashboard com Streamlit conectado ao DW
- [x] KPIs, graficos, tabelas filtraveis
- [x] Notebooks com analises automatizadas
- [ ] Relatorios periodicos via email

### 6. Infraestrutura (longo prazo)

- [ ] CI/CD no GitHub Actions (testes automaticos)
- [x] Deploy do Airflow no Docker
- [x] Volume dbt removido (diretorio nao existe mais)
- [ ] Backup automatico do PostgreSQL
- [ ] Monitoramento com Prometheus + Grafana

---

## Licenca

MIT License
