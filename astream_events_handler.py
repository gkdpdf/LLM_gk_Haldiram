import streamlit as st
from langgraph_backend import workflow
import uuid
import json

# Initialise a thread config
thread_config = {"configurable": {"thread_id": str(uuid.uuid4())}}
print("Thread config:", thread_config)

async def invoke_our_graph(st_message, st_placeholder, st_state, st_state_g):
    """
    Asynchronously process a stream of events from the graph runnable and updates the streamlit interface
    """
    print("\n================= INVOKE OUR GRAPH START =================")
    print("Incoming user message:", st_message)
    print("Initial st_state:", st_state)
    print("Initial st_state_g:", st_state_g)

    container = st_placeholder

    # --- Resume logic ---
    if st_state_g.get("graph_resume") == True:
        print("[DEBUG] Resuming graph with feedback:", st_message)
        workflow.update_state(thread_config, {"feedback": st_message})
        print("State after updating feedback")
        st_state = None
        st_state_g["graph_resume"] = False

    print("[DEBUG] Starting async event loop...")
    async for event in workflow.astream_events(st_state, thread_config, version="v2"):
            # --- End of event loop ---
        state = workflow.get_state(thread_config)
        print("[DEBUG] Workflow state before  starting of loop:", state.values)
        interesting_events = {"cleaned_query", "review_cleaned_query_node", "take_feedback",
                      "add_filter_sql_query", "rewrite_sql_query",
                      "sql_query_execution", "summarise_results",
                      "tabls_found", "fuzzy_match",
                      "wether_query_success"}
        if event["name"] in interesting_events:
            print(f"\n[EVENT RECEIVED] {event['name']} ")

        name = event["name"]

        if name == "cleaned_query":
            st_state["cleaned_user_query"] = event["data"]["cleaned_user_query"]
            print("[DEBUG] Cleaned query stored:", st_state["cleaned_user_query"])
            container.info("SQL cleaned user query executed :" + event["data"]["cleaned_user_query"])

        elif name == "take_feedback":
            print("[DEBUG] Graph is waiting for feedback")
            container.warning("Please provide a feedback on the cleaned query. If the query is good, please write no. The chatbot is waiting for your response")
            st_state_g["graph_resume"] = True

        elif name == "tabls_found":
            tables = event["data"]["find_tables"]
            st_state["tables"] = tables
            print("[DEBUG] Tables found:", tables)
            container.success("Tables found :" + ", ".join(tables))

        elif name == "fuzzy_match":
            fuzzy_match = event["data"]["fuzz_match"]
            st_state["fuzz_match"] = fuzzy_match
            print("[DEBUG] Fuzzy match:", fuzzy_match)
            if len(fuzzy_match) > 0:
                container.error("Fuzzy match found for columns :" + ", ".join(fuzzy_match))
            else:
                container.success("No fuzzy match found")

        elif name == "add_filter_sql_query":
            sql_query = event["data"]["sql_query"]
            st_state["sql_query"] = sql_query
            print("[DEBUG] SQL query with filters:", sql_query)
            container.info("SQL query with filter added :" + sql_query)

        elif name == "rewrite_sql_query":
            sql_query = event["data"]["sql_query"]
            st_state["sql_query"] = sql_query
            print("[DEBUG] SQL query rewritten:", sql_query)
            container.success("SQL query added")

        elif name == "sql_query_execution":
            sql_result = event["data"]["sql_query_result"]
            st_state["dataframe"] = sql_result
            st_state["query_results"] = "Query executed successfully"
            print("[DEBUG] SQL execution result:", sql_result)
            container.success("SQL query executed successfully")

        elif name == "wether_query_success":
            query_success = event["data"]["query_ran_successfully"]
            st_state["is_empty_result"] = not query_success
            print("[DEBUG] Query success flag:", query_success)
            if query_success:
                container.success("Query ran successfully")
            else:
                container.error("Query failed or empty result")

        elif name == "summarise_results":
            summary_results = event["data"]["summary_results"]
            st_state["summary_results"] = summary_results
            print("[DEBUG] Summary results:", summary_results)
            container.success("Summary of results :" + summary_results)
            st_state = {}
            print("================= INVOKE OUR GRAPH END =================\n")
            return {"op": "query_successful", "msg": summary_results}

    # --- End of event loop ---
    state = workflow.get_state(thread_config)
    print("[DEBUG] Workflow state after loop:", state.values)

    if len(state.tasks) != 0 and len(state.next) != 0:
        issue = state.tasks[0].interrupts[0].value
        print("[DEBUG] Graph interrupted, waiting for feedback:", issue)
        return {"op": "on_waiting_feedback", "msg": issue}

    print("================= INVOKE OUR GRAPH END (no result) =================\n")
