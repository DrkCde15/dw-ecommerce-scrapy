# Data Warehouse - E-commerce

## Visao Geral

Projeto de Data Warehouse para analise de dados de e-commerce utilizando **PostgreSQL**, **SQL**, **dbt** e **Scrapy** para web scraping com injecao direta no banco.

## Arquitetura

```
Scrapy (scraping) → PostgreSQL (raw) → dbt (staging → intermediate → marts)
```

- **Scraping**: Spiders Scrapy coletam dados e salvam direto no PostgreSQL
- **Raw**: Dados brutos armazenados no PostgreSQL
- **Staging**: Modelos de estagiação que limpam e normalizam os dados
- **Intermediate**: Modelos intermediários com joins e transformações complexas
- **Marts**: Tabelas de dimensão e fato prontas para analise

## Stack

| Camada | Tecnologia | Descricao |
|--------|------------|-----------|
| Scraping | Scrapy 2.19 | Coleta de dados da web |
| Browser | Playwright (opcional) | Sites com JS anti-bot |
| Pipeline | SQLAlchemy | Injecao direta no PostgreSQL |
| Banco | PostgreSQL 16 | Data Warehouse |
| Transformacao | dbt 1.9 | Modelagem de dados |
| Orquestracao | Airflow 2.10 | DAGs e agendamento |
| Analise | Jupyter + pandas | Exploracao de dados |

## Pre-requisitos

- Python 3.14+
- Podman + podman-compose
- dbt-postgres

## Instalacao

```bash
# 1. Subir PostgreSQL + Airflow
podman-compose up -d

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar dbt
cd dbt
dbt deps

# 4. Acessar Airflow
# http://localhost:8080 (admin/admin)
```

## Configuracao dbt

Configure seu `profiles.yml`:

```yaml
ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      dbname: ecommerce
      user: postgres
      password: postgres
      schema: public
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

### Configuracoes

Arquivos de configuracao na pasta `configs/`:

| Arquivo | Descricao |
|---------|-----------|
| `books_toscrape.yml` | Config para books.toscrape.com |
| `mercadolivre_celulares.json` | Config para Mercado Livre |

Exemplo de configuracao YAML:

```yaml
name: "Meu Spider"
allowed_domains: ["example.com"]
start_url: "https://example.com/produtos"
selectors:
  item: "div.product"
  fields:
    name:
      css: "h2.title"
    price:
      css: "span.price"
      processors:
        - clean_price
pagination:
  next: "a.next::attr(href)"
  max_pages: 5
```

### Pipelines

| Pipeline | Prioridade | Funcao |
|----------|------------|--------|
| `CleaningPipeline` | 100 | Normaliza nomes, precos, categorias |
| `ValidationPipeline` | 200 | Rejeita campos obrigatorios faltando |
| `DuplicatesFilterPipeline` | 300 | Remove duplicatas por product_id |
| `PostgresPipeline` | 400 | Insere dados no PostgreSQL (batch) |

### Testes

```bash
# Rodar todos os testes
pytest tests/scraping/ -v

# Com coverage
pytest tests/scraping/ --cov=src/scraping
```

## dbt

```bash
# Rodar todos os modelos
dbt run

# Rodar apenas stg_books e dim_books
dbt run --select stg_books dim_books

# Rodar testes
dbt test

# Gerar documentacao
dbt docs generate && dbt docs serve
```

### Modelos de Dados

| Modelo | Camada | Fonte | Descricao |
|--------|--------|-------|-----------|
| `stg_books` | Staging | books.toscrape.com | Livros raspados via HTML |
| `stg_amazon` | Staging | amazon.com.br | Produtos raspados via HTML |
| `stg_americanas` | Staging | americanas.com.br | Produtos via VTEX API |
| `stg_kabum` | Staging | kabum.com.br | Produtos via API interna |
| `dim_books` | Marts | books | Metricas por livro |
| `dim_products` | Marts | todas | Tabela unificada de produtos |

## Airflow

### Acessos

| Servico | URL | Credenciais |
|---------|-----|-------------|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | postgres / postgres |

### DAG `ecommerce_etl`

Executa todos os 4 spiders (books, amazon, americanas, kabum) e salva no PostgreSQL via `PostgresPipeline`.

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

# Ver status da execucao
podman exec ecommerce_airflow_webserver airflow tasks states-for-dag-run ecommerce_etl <run_id>

# Parar tudo
podman-compose down

# Parar e limpar volumes
podman-compose down -v
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
podman exec -it postgres psql -U postgres -d ecommerce
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
├── configs/
│   ├── books_toscrape.yml
│   └── mercadolivre_celulares.json
├── src/scraping/
│   └── ecommerce_scraper/
│       ├── settings.py
│       ├── pipelines.py
│       └── spiders/
│           ├── books_spider.py
│           ├── amazon_spider.py
│           ├── americanas_spider.py
│           ├── kabum_spider.py
│           ├── mercadolivre_spider.py
│           ├── configurable_spider.py
│           └── products_spider.py
├── sql/
│   └── init/
│       └── init_schema.sql
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   ├── stg_books.sql
│       │   ├── stg_amazon.sql
│       │   ├── stg_americanas.sql
│       │   └── stg_kabum.sql
│       └── marts/
│           ├── schema.yml
│           ├── dim_books.sql
│           └── dim_products.sql
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
```

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

### 2. Integracao scraping-dbt (curto prazo)

- [x] Criar stg_books no dbt
- [x] Criar dim_books com metricas
- [x] Criar stg_amazon
- [x] Criar stg_americanas
- [x] Criar stg_kabum
- [x] Criar dim_products (unificado)
- [ ] Criar stg_mercadolivre (pendente OAuth2)

### 3. Automacao (medio prazo)

- [x] DAG no Airflow para rodar scraping diario
- [x] Schedule de `dbt run` apos scraping
- [ ] Alertas quando scraping falhar
- [ ] Notificacao via Telegram/Slack

### 4. Data Quality (medio prazo)

- [ ] Great Expectations ou pandera para validar dados antes de ingerir
- [ ] Testes de consistencia entre scraping e banco
- [ ] Monitoreamento de volume de dados
- [ ] Alertas de anomalias (precos, volume, etc)

### 5. Visualizacao (medio/longo prazo)

- [ ] Dashboard com Metabase ou Superset conectado ao DW
- [ ] Notebooks com analises automatizadas
- [ ] Relatorios periodicos via email
- [ ] KPIs de e-commerce (ticket medio, conversao, etc)

### 6. Infraestrutura (longo prazo)

- [ ] CI/CD no GitHub Actions (testes automaticos)
- [x] Deploy do Airflow no Docker
- [ ] Backup automatico do PostgreSQL
- [ ] Monitoramento com Prometheus + Grafana
