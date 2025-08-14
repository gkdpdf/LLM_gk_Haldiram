# streamlit_app.py
from __future__ import annotations
from typing import List, Set, Dict, Any
import json
import streamlit as st
from sqlalchemy import inspect

from service import get_engine, llm_reply

# -------------------- Page --------------------
st.set_page_config(
    page_title="Sales Assistant",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -------------------- Init --------------------
ENGINE = get_engine()

def get_all_tables() -> List[str]:
    insp = inspect(ENGINE)
    return sorted(insp.get_table_names())

def allowed_tables_for(domain: str, all_tables: List[str]) -> List[str]:
    """Primary → all except tbl_shipment; Shipment → all except tbl_primary."""
    block: Set[str] = {"tbl_shipment"} if domain == "Primary" else {"tbl_primary"}
    return [t for t in all_tables if t not in block]

def run_query(question: str, domain: str, allowed: List[str]) -> Dict[str, Any]:
    # Guardrail + question (backend reads text after 'USER QUESTION:')
    guardrail = (
        "Use ONLY these tables: "
        + ", ".join(f'"{t}"' for t in allowed)
        + ". Do NOT reference any other tables."
    )
    payload = f"{guardrail}\n\nUSER QUESTION: {question.strip()}"
    route_pref = "primary" if domain == "Primary" else "shipment"
    return llm_reply(payload, route_pref=route_pref)

# -------------------- Session State --------------------
if "domain" not in st.session_state:
    st.session_state.domain = "Shipment"  # default
if "messages" not in st.session_state:
    # each message: {"role":"user"/"assistant", "content": str, "meta": dict|None}
    st.session_state.messages = []
if "tables_cache" not in st.session_state:
    st.session_state.tables_cache = get_all_tables()

# -------------------- Minimal Styles --------------------
st.markdown("""
<style>
.app-header {
  padding: 12px 18px;
  margin: -1rem -1rem 1rem -1rem;
  background: linear-gradient(90deg, #0ea5e9 0%, #6366f1 100%);
  color: white; border-bottom-left-radius: 12px; border-bottom-right-radius: 12px;
}
.app-header h1 { font-size: 1.6rem; margin: 0; line-height: 1.2; }
.app-subtle { opacity: .9; }
.card {
  background: #ffffff;
  border: 1px solid #ebedf0;
  border-radius: 14px;
  padding: 14px 16px;
}
</style>
""", unsafe_allow_html=True)

# -------------------- Header --------------------
st.markdown("""
<div class="app-header">
  <h1>Sales Assistant</h1>
  <div class="app-subtle">Select domain → chat. That’s it.</div>
</div>
""", unsafe_allow_html=True)

# -------------------- Filter (Domain) --------------------
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("**Choose data domain**")
    # Use radio (segmented control isn't available in all Streamlit versions)
    st.session_state.domain = st.radio(
        "Domain",
        options=["Primary", "Shipment"],
        horizontal=True,
        index=1 if st.session_state.domain == "Shipment" else 0,
        label_visibility="collapsed",
    )
    st.markdown('</div>', unsafe_allow_html=True)

# Prepare allowed tables silently (not shown)
all_tables = st.session_state.tables_cache or get_all_tables()
allowed = allowed_tables_for(st.session_state.domain, all_tables)

# -------------------- Chat --------------------
st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("**Chat**")

# Render history
for msg in st.session_state.messages:
    with st.chat_message("user" if msg["role"] == "user" else "assistant"):
        st.write(msg["content"])
        if msg["role"] == "assistant" and isinstance(msg.get("meta"), dict):
            with st.expander("Details / Debug"):
                meta = msg["meta"].copy()
                # clean heavy payloads if any
                for k in ["rows", "columns"]:
                    meta.pop(k, None)
                st.json(meta)

# Chat input
user_q = st.chat_input(f"Ask about {st.session_state.domain.lower()} data…")
if user_q:
    # Show user message
    st.session_state.messages.append({"role": "user", "content": user_q, "meta": {"domain": st.session_state.domain}})

    # Query backend
    with st.spinner("Thinking..."):
        result = run_query(user_q, st.session_state.domain, allowed)

    # Show assistant reply
    st.session_state.messages.append({
        "role": "assistant",
        "content": result.get("query_result") or "No response.",
        "meta": result
    })
    st.rerun()

st.markdown('</div>', unsafe_allow_html=True)
