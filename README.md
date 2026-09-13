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

## Pre-requisitos

- Python 3.9+
- Podman + podman-compose
- dbt-postgres

## Instalacao

```bash
# 1. Subir PostgreSQL
podman-compose up -d

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
      user: postgres
      password: postgres
      schema: public
```

## Web Scraping

### Spiders Disponiveis

| Spider | Descricao | Uso |
|--------|-----------|-----|
| `books` | Livros de books.toscrape.com | `scrapy crawl books` |
| `mercadolivre` | Produtos do Mercado Livre (API publica) | `scrapy crawl mercadolivre -a query="iphone"` |
| `amazon` | Produtos da Amazon.com.br | `scrapy crawl amazon -a query="notebook"` |
| `configurable` | Generico via JSON/YAML | `scrapy crawl configurable -a config=configs/example.yml` |
| `products` | Generico para e-commerce | `scrapy crawl products -a url=URL` |

### Exemplos de Uso

```bash
# Raspar livros e salvar no PostgreSQL
scrapy crawl books

# Raspar celulares no Mercado Livre
scrapy crawl mercadolivre -a query="celular" -a limit=100

# Raspar notebooks na Amazon
scrapy crawl amazon -a query="notebook" -a pages=3

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

### Modelos de Livros

| Modelo | Camada | Descricao |
|--------|--------|-----------|
| `stg_books` | Staging | Limpa e normaliza dados de `raw.books` |
| `dim_books` | Marts | Metricas por livro e comparacao com categoria |

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
├── configs/
│   ├── books_toscrape.yml
│   └── mercadolivre_celulares.json
├── src/scraping/
│   └── ecommerce_scraper/
│       ├── settings.py
│       ├── pipelines.py
│       └── spiders/
│           ├── books_spider.py
│           ├── mercadolivre_spider.py
│           ├── amazon_spider.py
│           ├── configurable_spider.py
│           └── products_spider.py
├── sql/
│   ├── init/
│   │   └── init_schema.sql
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── scripts/
│   └── load_books.py
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   └── stg_books.sql
│       └── marts/
│           ├── schema.yml
│           └── dim_books.sql
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
4. `raw.books` → `stg_books` → `dim_books`

---

## Roadmap

### 1. Novos Spiders (curto prazo)

- [x] Spider para Mercado Livre (API publica)
- [x] Spider para Amazon.com.br
- [x] Spider generico configuravel via JSON/YAML
- [ ] Integrar spiders ao dbt (stg_mercadolivre, dim_products_ml)
- [ ] Spider para Magazine Luiza
- [ ] Spider para Americanas

### 2. Integracao scraping-dbt (curto prazo)

- [x] Criar stg_books no dbt
- [x] Criar dim_books com metricas
- [ ] Criar stg_mercadolivre
- [ ] Criar dim_mercadolivre
- [ ] Criar stg_amazon
- [ ] Criar dim_amazon

### 3. Automacao (medio prazo)

- [ ] DAG no Airflow ou cron para rodar scraping diario
- [ ] Schedule de `dbt run` apos scraping
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
- [ ] Deploy do Airflow no Docker
- [ ] Backup automatico do PostgreSQL
- [ ] Monitoramento com Prometheus + Grafana
