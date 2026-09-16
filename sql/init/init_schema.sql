-- Schema raw para dados brutos
CREATE SCHEMA IF NOT EXISTS raw;

-- Schema para staging (dbt)
CREATE SCHEMA IF NOT EXISTS staging;

-- Schema para marts (dbt)
CREATE SCHEMA IF NOT EXISTS marts;

-- Tabela de clientes raw
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de produtos raw
CREATE TABLE IF NOT EXISTS raw.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de itens do pedido raw
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES raw.orders(order_id),
    product_id INTEGER REFERENCES raw.products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10,2)
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
    scraped_at TIMESTAMP
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
    scraped_at TIMESTAMP
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
    scraped_at TIMESTAMP
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
    scraped_at TIMESTAMP
);
