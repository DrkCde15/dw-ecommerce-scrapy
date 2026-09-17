"""
Dashboard de E-commerce - Data Warehouse
Visualizacao interativa dos dados raspados por Scrapy.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# Configuracao da pagina
st.set_page_config(
    page_title="E-commerce DW Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Conexao com PostgreSQL
@st.cache_resource
def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ecommerce")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


def load_data(query):
    engine = get_engine()
    return pd.read_sql(query, engine)


# Sidebar
st.sidebar.title("📊 Filtros")

# Carregar dados
try:
    df_books = load_data("SELECT * FROM raw.books")
    df_amazon = load_data("SELECT * FROM raw.amazon")
    df_americanas = load_data("SELECT * FROM raw.americanas")
    df_kabum = load_data("SELECT * FROM raw.kabum")
except Exception as e:
    st.error(f"Erro ao conectar com PostgreSQL: {e}")
    st.info("Certifique-se de que o PostgreSQL esta rodando: `podman-compose up -d postgres`")
    st.stop()

# Adicionar coluna source
df_books["source"] = "Books"
df_amazon["source"] = "Amazon"
df_americanas["source"] = "Americanas"
df_kabum["source"] = "KaBuM"

# Unificar DataFrames para analises comparativas
df_all = pd.concat([
    df_books[["product_id", "name", "price", "source", "scraped_at"]].assign(category=df_books.get("category", "Livros")),
    df_amazon[["product_id", "name", "price", "source", "scraped_at"]].assign(category="Amazon"),
    df_americanas[["product_id", "name", "price", "source", "scraped_at"]].assign(category=df_americanas.get("category", "Geral")),
    df_kabum[["product_id", "name", "price", "source", "scraped_at"]].assign(category=df_kabum.get("category", "Geral")),
], ignore_index=True)

# Filtros na sidebar
sources = st.sidebar.multiselect(
    "Fontes",
    options=df_all["source"].unique(),
    default=df_all["source"].unique(),
)

price_range = st.sidebar.slider(
    "Faixa de Preco (R$)",
    min_value=float(df_all["price"].min() or 0),
    max_value=float(df_all["price"].max() or 1000),
    value=(0, float(df_all["price"].max() or 1000)),
)

# Aplicar filtros
df_filtered = df_all[
    (df_all["source"].isin(sources))
    & (df_all["price"] >= price_range[0])
    & (df_all["price"] <= price_range[1])
]

# ============================================================
# HEADER
# ============================================================
st.title("📊 Dashboard E-commerce DW")
st.markdown("Dados raspados por Scrapy e armazenados no PostgreSQL")

# ============================================================
# KPIs GERAIS
# ============================================================
st.header("📈 KPIs Gerais")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total de Produtos", f"{len(df_filtered):,}")

with col2:
    st.metric("Preco Medio", f"R$ {df_filtered['price'].mean():.2f}")

with col3:
    st.metric("Preco Min", f"R$ {df_filtered['price'].min():.2f}")

with col4:
    st.metric("Preco Max", f"R$ {df_filtered['price'].max():.2f}")

st.divider()

# ============================================================
# VOLUME DE SCRAPING
# ============================================================
st.header("📦 Volume de Scraping")

col1, col2 = st.columns([2, 1])

with col1:
    volume_data = df_all.groupby("source").size().reset_index(name="produtos")
    fig_volume = px.bar(
        volume_data,
        x="source",
        y="produtos",
        color="source",
        title="Produtos por Fonte",
        labels={"source": "Fonte", "produtos": "Qtd Produtos"},
    )
    fig_volume.update_layout(showlegend=False)
    st.plotly_chart(fig_volume, use_container_width=True)

with col2:
    st.subheader("Por Fonte")
    for source in df_all["source"].unique():
        count = len(df_all[df_all["source"] == source])
        st.metric(source, f"{count:,}")

st.divider()

# ============================================================
# ANALISE DE PRECOS
# ============================================================
st.header("💰 Analise de Precos")

col1, col2 = st.columns(2)

with col1:
    fig_box = px.box(
        df_filtered,
        x="source",
        y="price",
        color="source",
        title="Distribuicao de Precos por Fonte",
        labels={"source": "Fonte", "price": "Preco (R$)"},
    )
    fig_box.update_layout(showlegend=False)
    st.plotly_chart(fig_box, use_container_width=True)

with col2:
    fig_hist = px.histogram(
        df_filtered,
        x="price",
        nbins=30,
        title="Distribuicao Geral de Precos",
        labels={"price": "Preco (R$)", "count": "Quantidade"},
    )
    st.plotly_chart(fig_hist, use_container_width=True)

# Estatisticas por fonte
st.subheader("Estatisticas por Fonte")
stats = df_filtered.groupby("source")["price"].agg(["count", "mean", "median", "min", "max"]).round(2)
stats.columns = ["Qtd", "Media", "Mediana", "Min", "Max"]
st.dataframe(stats, use_container_width=True)

st.divider()

# ============================================================
# COMPARACAO ENTRE FONTES
# ============================================================
st.header("🔄 Comparacao entre Fontes")

col1, col2 = st.columns(2)

with col1:
    avg_price = df_filtered.groupby("source")["price"].mean().reset_index()
    fig_avg = px.bar(
        avg_price,
        x="source",
        y="price",
        color="source",
        title="Preco Medio por Fonte",
        labels={"source": "Fonte", "price": "Preco Medio (R$)"},
    )
    fig_avg.update_layout(showlegend=False)
    st.plotly_chart(fig_avg, use_container_width=True)

with col2:
    fig_scatter = px.scatter(
        df_filtered.sample(min(500, len(df_filtered))),
        x="price",
        y="source",
        color="source",
        title="Amostra de Precos por Fonte",
        opacity=0.6,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()

# ============================================================
# LISTA DE PRODUTOS
# ============================================================
st.header("🛒 Lista de Produtos")

# Tabela de produtos
st.dataframe(
    df_filtered[["product_id", "name", "price", "source", "category", "scraped_at"]].sort_values("price", ascending=False),
    use_container_width=True,
    height=400,
)

# ============================================================
# SIDEBAR - Info
# ============================================================
st.sidebar.divider()
st.sidebar.markdown("### ℹ️ Info")
st.sidebar.markdown(f"- **Total de registros**: {len(df_all):,}")
st.sidebar.markdown(f"- **Fontes ativas**: {df_all['source'].nunique()}")
st.sidebar.markdown(f"- **Ultima atualizacao**: {df_all['scraped_at'].max()}")
st.sidebar.markdown("---")
st.sidebar.markdown("Fonte de dados: PostgreSQL `ecommerce`")
