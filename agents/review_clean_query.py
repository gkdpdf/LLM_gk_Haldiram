from langgraph.types import interrupt
# Build LLM prompt (using both original + cleaned + feedback)
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

def review_cleaned_query_node(state: dict) -> dict:
    """
    Human-in-the-loop step:
    Show cleaned query, take user feedback,
    and use LLM to rewrite the query accordingly.
    """
    # Ask user for feedback
    value = interrupt({
        "original_user_query": state["user_query"],
        "cleaned_query": state["cleaned_user_query"],
        "message": "Do you want to change this query? Reply with new text or 'no'."
    })

    # Case 1: User says 'no' -> keep existing cleaned query
    if isinstance(value, str) and value.lower().strip() == "no":
        return state

    # Case 2: Extract feedback from value
    if isinstance(value, str):
        feedback = value
    else:
        feedback = value.get("cleaned_query", "")

    llm = ChatOpenAI(model="gpt-4o-mini")
    parser = StrOutputParser()

    rewrite_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a query rewriter.
    - Always keep the original intent and entities from the original query.
    - Only apply modifications that the user feedback explicitly requests.
    - Do not drop important details like product names, time ranges, or metrics unless the user feedback says to remove them.
    """),
        ("human", """Original user query:
    {original_query}

    Current cleaned query:
    {cleaned_query}

    User feedback:
    {feedback}

    Rewrite the cleaned query so it:
    1. Preserves all entities (like product names, customer names, regions, dates) from the original query.
    2. Modifies only what the feedback requests.
    3. Produces a single clear natural language query.
    """)
    ])

    chain = rewrite_prompt | llm | parser

    # Generate improved cleaned query
    new_cleaned_query = chain.invoke({
        "original_query": state["user_query"],
        "cleaned_query": state["cleaned_user_query"],
        "feedback": feedback
    })

    # Update state
    state["cleaned_user_query"] = new_cleaned_query.strip()
    return state
