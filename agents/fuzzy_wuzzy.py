import pandas as pd
from sqlalchemy import create_engine,  text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String
from langchain_community.utilities import SQLDatabase
from rapidfuzz import process, fuzz

import urllib.parse 

# Load engine and knowledge base
password = urllib.parse.quote_plus("Iameighteeni@18")

def return_db_postgres():
    # ✅ Return LangChain-compatible PostgreSQL connection
    return SQLDatabase.from_uri(
        f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary"
    )

# 🔌 Connect to DB and print tables
db = return_db_postgres()
PG_URI = f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary"
# Real SQLAlchemy engine (for pandas)
engine = create_engine(PG_URI, pool_pre_ping=True)

from typing import Any
try:
    from pydantic import BaseModel
except Exception:
    BaseModel = object  # fallback if not installed here

def unwrap_pydantic_root(x: Any) -> Any:
    """
    If x is a Pydantic RootModel/__root__ model, return its underlying Python value.
    Works for both Pydantic v1 (__root__) and v2 (.root).
    Otherwise return x unchanged.
    """
    # v2 RootModel has `.root`
    root = getattr(x, "root", None)
    if root is not None:
        return root
    # v1 BaseModel with __root__
    root = getattr(x, "__root__", None)
    if root is not None:
        return root
    return x


def get_best_fuzzy_match(input_value, choices):

    match, score, _ = process.extractOne(input_value, choices, scorer=fuzz.token_set_ratio)
    return match, score


def get_values(table_name, column_name):

    # SQL query to get distinct values
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"

    # Execute query and load results into DataFrame
    df = pd.read_sql(query, con=engine)

    # Optionally, convert to list if you want raw values
    unique_values = df[column_name].dropna().tolist()

    return unique_values


def call_match(val):
    val = unwrap_pydantic_root(val)
    final = []
    for lst in val[1:]:
        lst = unwrap_pydantic_root(lst)
        table = lst[0]
        column = lst[1]
        str_lst = [i.strip() for i in lst[2].split(',')]

        unq_col_val = get_values(table, column)
        unq_col_val = [str(i) for i in unq_col_val]

        for subval in str_lst:
            best_match, score = get_best_fuzzy_match(subval, unq_col_val)
            # if score > 80:
            final.append(["table name:"+table, "column_name:"+column, "filter_value:"+best_match])

    return final

