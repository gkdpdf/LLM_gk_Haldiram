# db.py
import os
from sqlalchemy import create_engine
from langchain_community.utilities.sql_database import SQLDatabase
from dotenv import load_dotenv

load_dotenv(override=True)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
)

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Optional: LangChain SQLDatabase wrapper (if you need it elsewhere)
db = SQLDatabase.from_uri(DATABASE_URL)
