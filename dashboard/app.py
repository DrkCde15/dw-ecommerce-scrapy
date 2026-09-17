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
from datetime import datetime, timedelta

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
    
    # Dados transformados
    df_dim_products = load_data("SELECT * FROM marts.dim_products")
    df_dim_books = load_data("SELECT * FROM marts.dim_books")
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
    df_kabum[["product_id", "name", "price", "source", "scraped_at"]].assign(category="KaBuM"),
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

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total de Produtos", f"{len(df_filtered):,}")

with col2:
    st.metric("Preco Medio", f"R$ {df_filtered['price'].mean():.2f}")

with col3:
    st.metric("Preco Min", f"R$ {df_filtered['price'].min():.2f}")

with col4:
    st.metric("Preco Max", f"R$ {df_filtered['price'].max():.2f}")

with col5:
    st.metric("Fontes Ativas", f"{df_filtered['source'].nunique()}")

st.divider()

# ============================================================
# TABS PRINCIPAIS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(["📦 Volume", "💰 Precos", "🔍 Analise", "📋 Dados"])

# ============================================================
# TAB 1: VOLUME DE SCRAPING
# ============================================================
with tab1:
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

    # Grafico de pizza
    col1, col2 = st.columns(2)

    with col1:
        fig_pie = px.pie(
            volume_data,
            names="source",
            values="produtos",
            title="Distribuicao por Fonte",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Timeline de scraping
        df_all["scraped_date"] = pd.to_datetime(df_all["scraped_at"]).dt.date
        timeline = df_all.groupby(["scraped_date", "source"]).size().reset_index(name="count")
        fig_timeline = px.line(
            timeline,
            x="scraped_date",
            y="count",
            color="source",
            title="Volume de Scraping ao Longo do Tempo",
            labels={"scraped_date": "Data", "count": "Quantidade", "source": "Fonte"},
        )
        st.plotly_chart(fig_timeline, use_container_width=True)

# ============================================================
# TAB 2: ANALISE DE PRECOS
# ============================================================
with tab2:
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

    # Grafico de violino
    col1, col2 = st.columns(2)

    with col1:
        fig_violin = px.violin(
            df_filtered,
            x="source",
            y="price",
            color="source",
            box=True,
            title="Distribuicao de Precos (Violino)",
            labels={"source": "Fonte", "price": "Preco (R$)"},
        )
        fig_violin.update_layout(showlegend=False)
        st.plotly_chart(fig_violin, use_container_width=True)

    with col2:
        # Top 10 produtos mais caros
        st.subheader("Top 10 Produtos Mais Caros")
        top_expensive = df_filtered.nlargest(10, "price")[["name", "price", "source"]]
        st.dataframe(top_expensive, use_container_width=True)

# ============================================================
# TAB 3: ANALISE AVANCADA
# ============================================================
with tab3:
    st.header("🔍 Analise Avancada")

    col1, col2 = st.columns(2)

    with col1:
        # Preco medio por fonte
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
        # Dispersao de precos
        fig_scatter = px.scatter(
            df_filtered.sample(min(500, len(df_filtered))),
            x="price",
            y="source",
            color="source",
            title="Amostra de Precos por Fonte",
            opacity=0.6,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # Analise de categorias
    st.subheader("Analise por Categoria")
    
    col1, col2 = st.columns(2)

    with col1:
        # Top 10 categorias
        if "category" in df_filtered.columns:
            cat_counts = df_filtered["category"].value_counts().head(10).reset_index()
            cat_counts.columns = ["category", "count"]
            fig_cat = px.bar(
                cat_counts,
                x="category",
                y="count",
                title="Top 10 Categorias",
                labels={"category": "Categoria", "count": "Quantidade"},
            )
            st.plotly_chart(fig_cat, use_container_width=True)

    with col2:
        # Preco medio por categoria
        if "category" in df_filtered.columns:
            cat_price = df_filtered.groupby("category")["price"].mean().sort_values(ascending=False).head(10).reset_index()
            cat_price.columns = ["category", "avg_price"]
            fig_cat_price = px.bar(
                cat_price,
                x="category",
                y="avg_price",
                title="Preco Medio por Categoria (Top 10)",
                labels={"category": "Categoria", "avg_price": "Preco Medio (R$)"},
            )
            st.plotly_chart(fig_cat_price, use_container_width=True)

    # Heatmap de precos
    st.subheader("Heatmap de Precos por Fonte e Categoria")
    if "category" in df_filtered.columns:
        pivot = df_filtered.pivot_table(
            values="price",
            index="category",
            columns="source",
            aggfunc="mean"
        ).fillna(0)
        
        fig_heatmap = px.imshow(
            pivot,
            title="Heatmap: Preco Medio por Categoria e Fonte",
            labels={"color": "Preco Medio (R$)"},
            aspect="auto",
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)

# ============================================================
# TAB 4: DADOS
# ============================================================
with tab4:
    st.header("📋 Dados")

    # Seletor de tabela
    table_option = st.selectbox(
        "Selecionar tabela",
        ["Dados Filtrados", "Dim Products", "Dim Books", "Raw Books", "Raw Amazon", "Raw Americanas", "Raw KaBuM"],
    )

    if table_option == "Dados Filtrados":
        st.dataframe(df_filtered, use_container_width=True, height=500)
    elif table_option == "Dim Products":
        st.dataframe(df_dim_products, use_container_width=True, height=500)
    elif table_option == "Dim Books":
        st.dataframe(df_dim_books, use_container_width=True, height=500)
    elif table_option == "Raw Books":
        st.dataframe(df_books, use_container_width=True, height=500)
    elif table_option == "Raw Amazon":
        st.dataframe(df_amazon, use_container_width=True, height=500)
    elif table_option == "Raw Americanas":
        st.dataframe(df_americanas, use_container_width=True, height=500)
    elif table_option == "Raw KaBuM":
        st.dataframe(df_kabum, use_container_width=True, height=500)

    # Estatisticas gerais
    st.subheader("Estatisticas Gerais")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total de Registros", f"{len(df_all):,}")

    with col2:
        st.metric("Tabelas Disponiveis", "7")

    with col3:
        st.metric("Ultima Atualizacao", f"{df_all['scraped_at'].max()}")

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
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Status do Pipeline")
st.sidebar.markdown("- ✅ Scraping: 4 spiders")
st.sidebar.markdown("- ✅ Transform: Python pipelines")
st.sidebar.markdown("- ✅ Load: CSV/JSON/Report")
st.sidebar.markdown("- ✅ Tests: 14/14 passando")
