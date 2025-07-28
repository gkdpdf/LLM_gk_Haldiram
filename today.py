from datetime import datetime
from langchain.tools import Tool

def get_today_date(_: str) -> str:
    return datetime.today().strftime("%Y-%m-%d")
