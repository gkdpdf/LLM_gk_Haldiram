from langchain.tools import Tool

def calculator_tool(expression: str) -> str:
    try:
        result = eval(expression, {"__builtins__": None})
        return str(result)
    except Exception as e:
        return f"Error: {str(e)}"