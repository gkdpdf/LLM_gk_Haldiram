from langchain.tools import Tool
import re

def clean_sql_query(query: str) -> str:
    """
    Cleans SQL input by removing markdown formatting and accidental wrappers.
    """
    # Remove triple backticks and optional sql tag
    query = re.sub(r"```sql\s*|```", "", query, flags=re.IGNORECASE).strip()
    
    # Optionally fix stray semicolons or whitespace
    query = query.strip().rstrip(";") + ";"
    
    return query
