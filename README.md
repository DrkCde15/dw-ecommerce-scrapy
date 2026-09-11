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
| Pipeline | SQLAlchemy | Injecao direta no PostgreSQL |
| Banco | PostgreSQL 16 | Data Warehouse |
| Transformacao | dbt 1.9 | Modelagem de dados |
| Analise | Jupyter + pandas | Exploracao de dados |

## Tabelas

| Tipo | Tabela | Fonte |
|------|--------|-------|
| Dimensao | `dim_customers` | raw.customers |
| Dimensao | `dim_products` | raw.products |
| Fato | `fact_orders` | raw.orders + raw.order_items |
| Scraping | `raw.books` | books.toscrape.com |

## Pre-requisitos

- Python 3.9+
- Docker + Docker Compose
- dbt-postgres

## Instalacao

```bash
# 1. Subir PostgreSQL
docker compose up -d

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar dbt
cd dbt
dbt deps
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
      user: ecommerce
      password: ecommerce123
      schema: public
```

## Web Scraping

### Scraping com injecao no PostgreSQL

```bash
# Subir banco
docker compose up -d

# Raspar livros e salvar direto no PostgreSQL
scrapy crawl books

# Raspar e salvar so em JSON (sem banco)
scrapy crawl books -o data/books.json
```

### Scraping generico

```bash
# Raspar produtos de um site
scrapy crawl products -a url="https://site.com/produtos" -a category=eletronicos
```

### Spiders Disponiveis

| Spider | Descricao | Salva em |
|--------|-----------|----------|
| `books` | Livros de books.toscrape.com | PostgreSQL (`raw.books`) |
| `products` | Generico para e-commerce | JSON/Parquet |

### Pipelines

| Pipeline | Prioridade | Funcao |
|----------|------------|--------|
| `CleaningPipeline` | 100 | Normaliza nomes, precos, categorias |
| `ValidationPipeline` | 200 | Rejeita campos obrigatorios faltando |
| `DuplicatesFilterPipeline` | 300 | Remove duplicatas por product_id |
| `PostgresPipeline` | 400 | Insere dados no PostgreSQL (batch) |

### Configuracao do PostgreSQL

Via `settings.py` ou variaveis de ambiente:

```python
POSTGRES_URL = "postgresql://ecommerce:ecommerce123@localhost:5432/ecommerce"
POSTGRES_TABLE = "raw.books"
```

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

# Rodar testes
dbt test

# Gerar documentacao
dbt docs generate && dbt docs serve
```

## Docker

```bash
# Subir PostgreSQL
docker compose up -d

# Parar
docker compose down

# Ver logs
docker compose logs postgres

# Acessar psql
docker compose exec postgres psql -U ecommerce -d ecommerce
```

## Estrutura do Projeto

```
03-data-warehouse-ecommerce/
├── docker-compose.yml
├── scrapy.cfg
├── requirements.txt
├── src/scraping/
│   └── ecommerce_scraper/
│       ├── settings.py
│       ├── pipelines.py
│       └── spiders/
│           ├── products_spider.py
│           └── books_spider.py
├── sql/
│   ├── init/
│   │   └── 01_init_schema.sql
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── dbt/
│   ├── dbt_project.yml
│   └── models/
├── data/
├── notebooks/
└── tests/
    └── scraping/
        ├── test_spiders.py
        └── test_pipelines.py
```

## Modelo de Dados

### Fluxo de Dados

1. `stg_customers` → `dim_customers`
2. `stg_products` → `dim_products`
3. `stg_orders` + `int_order_items` → `fact_orders`
4. `raw.books` → (futuro) `dim_books`
