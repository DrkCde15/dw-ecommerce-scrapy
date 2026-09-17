-- Schema raw para dados brutos
CREATE SCHEMA IF NOT EXISTS raw;

-- Schema para staging (transformação Python)
CREATE SCHEMA IF NOT EXISTS staging;

-- Schema para marts (transformação Python)
CREATE SCHEMA IF NOT EXISTS marts;

-- Schema para lineage (rastreamento de origem dos dados)
CREATE SCHEMA IF NOT EXISTS lineage;

-- Tabela de auditoria de lineage
CREATE TABLE IF NOT EXISTS lineage.data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    duration_seconds NUMERIC(10,2),
    status VARCHAR(20) DEFAULT 'success',
    error_message TEXT,
    checksum VARCHAR(64),
    parent_lineage_id BIGINT REFERENCES lineage.data_lineage(lineage_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON lineage.data_lineage(source_name);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON lineage.data_lineage(target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_execution ON lineage.data_lineage(execution_time);

-- Tabela de tracking de changes para CDC
CREATE TABLE IF NOT EXISTS lineage.change_tracking (
    tracking_id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    source_key VARCHAR(255) NOT NULL,
    operation VARCHAR(20) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP,
    UNIQUE(source_table, source_key, operation, changed_at)
);

-- Tabela de clientes raw
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de produtos raw
CREATE TABLE IF NOT EXISTS raw.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de pedidos raw
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES raw.customers(customer_id),
    order_date DATE,
    status VARCHAR(50),
    total_amount DECIMAL(10,2),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de itens do pedido raw
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES raw.orders(order_id),
    product_id INTEGER REFERENCES raw.products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de livros (raspados via Scrapy)
CREATE TABLE IF NOT EXISTS raw.books (
    book_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    name VARCHAR(500),
    price DECIMAL(10,2),
    rating INTEGER,
    availability VARCHAR(100),
    image_url TEXT,
    url TEXT,
    category VARCHAR(100),
    source TEXT,
    scraped_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, source)
);

-- Tabela de produtos Americanas (raspados via VTEX API)
CREATE TABLE IF NOT EXISTS raw.americanas (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    name VARCHAR(500),
    brand VARCHAR(200),
    price DECIMAL(10,2),
    list_price DECIMAL(10,2),
    available_quantity INTEGER,
    category VARCHAR(500),
    image_url TEXT,
    url TEXT,
    seller VARCHAR(200),
    source VARCHAR(50),
    scraped_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, source)
);

-- Tabela de produtos KaBuM (raspados via API interna)
CREATE TABLE IF NOT EXISTS raw.kabum (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    name VARCHAR(500),
    brand VARCHAR(200),
    price DECIMAL(10,2),
    old_price DECIMAL(10,2),
    discount_percentage INTEGER,
    stock INTEGER,
    category VARCHAR(500),
    image_url TEXT,
    url TEXT,
    seller VARCHAR(200),
    rating DECIMAL(3,1),
    reviews_count INTEGER,
    source VARCHAR(50),
    scraped_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, source)
);

-- Tabela de produtos Amazon (raspados via HTML)
CREATE TABLE IF NOT EXISTS raw.amazon (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    name VARCHAR(500),
    price DECIMAL(10,2),
    original_price DECIMAL(10,2),
    rating DECIMAL(3,1),
    reviews_count INTEGER,
    image_url TEXT,
    url TEXT,
    source VARCHAR(50),
    scraped_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    _extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, source)
);
