# Data Warehouse - E-commerce

## Visao Geral

Projeto de Data Warehouse para analise de dados de e-commerce utilizando **PostgreSQL**, **Python** e **Scrapy** para web scraping com injecao direta no banco.

## Arquitetura

```
Scrapy (scraping) → PostgreSQL (raw) → Python (staging → marts) → Dashboard
```

- **Scraping**: Spiders Scrapy coletam dados e salvam direto no PostgreSQL
- **Raw**: Dados brutos armazenados no PostgreSQL
- **Transform**: Scripts Python com pandas transformam dados
- **Marts**: Tabelas de dimensao prontas para analise
- **Load**: Exportacao para CSV/JSON e schema report
- **Dashboard**: Visualizacao interativa com Streamlit

## Stack

| Camada | Tecnologia | Descricao |
|--------|------------|-----------|
| Scraping | Scrapy 2.19 | Coleta de dados da web |
| Pipeline | SQLAlchemy | Injecao direta no PostgreSQL |
| Banco | PostgreSQL 16 | Data Warehouse |
| Transformacao | Python + pandas | Transformacao de dados |
| Orquestracao | Airflow 2.10 | DAGs e agendamento |
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
# Raspar livros e salvar no PostgreSQL
scrapy crawl books

# Raspar notebooks na Amazon
scrapy crawl amazon -a query="notebook" -a pages=3

# Raspar notebooks na Americanas (VTEX API)
scrapy crawl americanas -a query="notebook" -a limit=100

# Raspar notebooks no KaBuM (API interna)
scrapy crawl kabum -a query="notebook" -a limit=100

# Raspar com configuracao personalizada
scrapy crawl configurable -a config=configs/books_toscrape.yml

# Salvar apenas em JSON
scrapy crawl books -o data/books.json
```

### Pipelines Scrapy

| Pipeline | Prioridade | Funcao |
|----------|------------|--------|
| `CleaningPipeline` | 100 | Normaliza nomes, precos, categorias |
| `ValidationPipeline` | 200 | Rejeita campos obrigatorios faltando |
| `DuplicatesFilterPipeline` | 300 | Remove duplicatas por product_id |
| `PostgresPipeline` | 400 | Insere dados no PostgreSQL (batch) |

## Pipelines Python

### Estrutura

```
src/pipelines/
├── storage/storage_pipeline.py    # CSV/JSON → PostgreSQL
├── transform/transform_pipeline.py # raw → staging → marts
├── load/load_pipeline.py          # marts → report (CSV/JSON)
└── tests/quality_tests.py         # Testes de qualidade
```

### Storage Pipeline

Carrega dados brutos do Scrapy para o PostgreSQL:

```python
from src.pipelines.storage.storage_pipeline import StoragePipeline

pipeline = StoragePipeline()
pipeline.load_csv_to_table("data/books.csv", "books", schema="raw")
pipeline.load_json_to_table("data/products.json", "products", schema="raw")
```

### Transform Pipeline

Transforma dados brutos em tabelas de analise:

```python
from src.pipelines.transform.transform_pipeline import TransformPipeline

pipeline = TransformPipeline()
results = pipeline.run_all()
# Retorna: {'staging': {...}, 'marts': {...}, 'elapsed_seconds': 1.65}
```

**Funcoes de transformacao:**
- `transform_stg_books()` - Livros
- `transform_stg_amazon()` - Amazon
- `transform_stg_americanas()` - Americanas
- `transform_stg_kabum()` - KaBuM
- `transform_dim_books()` - Dimensao de livros
- `transform_dim_products()` - Dimensao unificada

### Load Pipeline

Exporta dados transformados:

```python
from src.pipelines.load.load_pipeline import LoadPipeline

pipeline = LoadPipeline()
pipeline.load_all_to_report()      # marts → report
pipeline.load_all_to_csv("output") # marts → CSV
pipeline.load_all_to_json("output") # marts → JSON
```

### Quality Tests

Valida qualidade dos dados:

```python
from src.pipelines.tests.quality_tests import DataQualityTests

tests = DataQualityTests()
success = tests.run_all_tests()
# 14/14 testes passando
```

**Testes disponiveis:**
- `test_unique()` - Unicidade de colunas
- `test_not_null()` - Nulidade
- `test_positive_value()` - Valores positivos
- `test_value_in_set()` - Valores em conjunto
- `test_row_count()` - Contagem de linhas
- `test_foreign_key()` - Chaves estrangeiras

## Airflow

### Acessos

| Servico | URL | Credenciais |
|---------|-----|-------------|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | postgres / postgres |

### DAG `ecommerce_etl`

```
run_all_spiders >> transform >> load >> quality_tests
   (BashOperator)   (PythonOperator)  (PythonOperator)  (PythonOperator)
```

- **Agendamento**: Diario as 6h
- **Spiders**: books (query=all), amazon/americanas/kabum (query=notebook, limit=50)

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

### Filtros

- **Fontes**: Selecionar quais fontes exibir (Books, Amazon, Americanas, KaBuM)
- **Faixa de Preco**: Filtrar por intervalo de preco

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
│       ├── storage/storage_pipeline.py
│       ├── transform/transform_pipeline.py
│       ├── load/load_pipeline.py
│       └── tests/quality_tests.py
├── sql/
│   └── init/
│       └── init_schema.sql
├── data/
└── tests/
    └── scraping/
        ├── test_spiders.py
        └── test_pipelines.py
```

## Modelo de Dados

### Fluxo de Dados

```
books.toscrape.com ──→ raw.books     ──→ stg_books     ──→ dim_books
amazon.com.br      ──→ raw.amazon    ──→ stg_amazon    ──┐
americanas.com.br  ──→ raw.americanas──→ stg_americanas──┤→ dim_products
kabum.com.br       ──→ raw.kabum     ──→ stg_kabum     ──┘
                                                            │
                                                            ▼
                                                     report (CSV/JSON)
```

### Tabelas

| Schema | Tabela | Registros | Descricao |
|--------|--------|-----------|-----------|
| `raw` | `books` | 12.000 | Livros brutos |
| `raw` | `amazon` | 535 | Produtos Amazon brutos |
| `raw` | `americanas` | 248 | Produtos Americanas brutos |
| `raw` | `kabum` | 250 | Produtos KaBuM brutos |
| `staging` | `stg_books` | 12.000 | Livros normalizados |
| `staging` | `stg_amazon` | 535 | Amazon normalizado |
| `staging` | `stg_americanas` | 248 | Americanas normalizado |
| `staging` | `stg_kabum` | 250 | KaBuM normalizado |
| `marts` | `dim_books` | 1.000 | Dimensao de livros |
| `marts` | `dim_products` | 347 | Dimensao unificada |

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
- [ ] Alertas quando scraping falhar
- [ ] Notificacao via Telegram/Slack

### 4. Data Quality (medio prazo)

- [x] Testes de qualidade em Python (14/14)
- [ ] Testes de consistencia entre scraping e banco
- [ ] Monitoramento de volume de dados
- [ ] Alertas de anomalias (precos, volume, etc)

### 5. Visualizacao (medio/longo prazo)

- [x] Dashboard com Streamlit conectado ao DW
- [x] KPIs, graficos, tabelas filtraveis
- [ ] Notebooks com analises automatizadas
- [ ] Relatorios periodicos via email

### 6. Infraestrutura (longo prazo)

- [ ] CI/CD no GitHub Actions (testes automaticos)
- [x] Deploy do Airflow no Docker
- [ ] Backup automatico do PostgreSQL
- [ ] Monitoramento com Prometheus + Grafana

---

## Licenca

MIT License
