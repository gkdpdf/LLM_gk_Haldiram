from datetime import date

def get_current_date_node(state: dict) -> dict:
    state["current_date"] = date.today().isoformat()
    return state
