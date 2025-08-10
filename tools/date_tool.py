from datetime import datetime
from langchain.tools import tool

@tool
def get_current_date(format: str = "%Y-%m-%d") -> str:
    """Returns the current date in the specified format."""
    return datetime.now().strftime(format)
