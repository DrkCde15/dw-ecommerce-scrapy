"""Script para carregar dados raspados no PostgreSQL."""
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Conexao
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ecommerce"
engine = create_engine(DATABASE_URL)

# Ler JSON
data_path = Path("data/books.json")
df = pd.read_json(data_path)

print(f"Total de registros: {len(df)}")
print(f"Colunas: {list(df.columns)}")

# Limpar rating (converter texto para numero)
rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
df["rating"] = df["rating"].map(rating_map)

# Remover coluna availability se tiver so espacos
if "availability" in df.columns:
    df["availability"] = df["availability"].str.strip()

# Inserir no banco
with engine.connect() as conn:
    # Limpar tabela se existir
    conn.execute(text("TRUNCATE TABLE raw.books"))
    conn.commit()

    # Inserir dados
    df.to_sql(
        "books",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
    )
    conn.commit()

    # Verificar
    result = conn.execute(text("SELECT COUNT(*) FROM raw.books"))
    total = result.scalar()
    print(f"Registros inseridos: {total}")

print("Concluido!")
