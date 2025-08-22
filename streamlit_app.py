# streamlit_app.py
from __future__ import annotations
from typing import List, Set, Dict, Any
import streamlit as st
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from service import get_engine, llm_reply

st.set_page_config(page_title="Sales Assistant", page_icon="🧭", layout="wide", initial_sidebar_state="collapsed")

ENGINE = get_engine()

def get_all_tables() -> List[str]:
    try:
        insp = inspect(ENGINE)
        return sorted(insp.get_table_names())
    except (SQLAlchemyError, Exception):
        return []

def allowed_tables_for(domain: str, all_tables: List[str]) -> List[str]:
    block: Set[str] = {"tbl_shipment"} if domain == "Primary" else {"tbl_primary"}
    return [t for t in all_tables if t not in block]

def run_query(question: str, domain: str, allowed: List[str]) -> Dict[str, Any]:
    guardrail = "Use ONLY these tables: " + ", ".join(f'"{t}"' for t in allowed) + ". Do NOT reference any other tables."
    payload = f"{guardrail}\n\nUSER QUESTION: {question.strip()}"
    route_pref = "primary" if domain == "Primary" else "shipment"
    return llm_reply(payload, route_pref=route_pref, allowed_tables=allowed)

# Session
if "domain" not in st.session_state:
    st.session_state.domain = "Shipment"
if "messages" not in st.session_state:
    st.session_state.messages = []
if "tables_cache" not in st.session_state:
    st.session_state.tables_cache = get_all_tables()

# Styles + header
st.markdown("""
<style>
.app-header { padding: 12px 18px; margin: -1rem -1rem 1rem -1rem;
  background: linear-gradient(90deg, #0ea5e9 0%, #6366f1 100%); color: white;
  border-bottom-left-radius: 12px; border-bottom-right-radius: 12px; }
.app-header h1 { font-size: 1.6rem; margin: 0; line-height: 1.2; }
.app-subtle { opacity: .9; }
.card { background: #fff; border: 1px solid #ebedf0; border-radius: 14px; padding: 14px 16px; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="app-header">
  <h1>Sales Assistant</h1>
  <div class="app-subtle">Select domain → chat. That’s it.</div>
</div>
""", unsafe_allow_html=True)

with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("**Choose data domain**")
    st.session_state.domain = st.radio(
        "Domain", options=["Primary", "Shipment"], horizontal=True,
        index=1 if st.session_state.domain == "Shipment" else 0,
        label_visibility="collapsed", key="domain_radio_unique"
    )
    st.markdown('</div>', unsafe_allow_html=True)

all_tables = st.session_state.tables_cache or get_all_tables()
allowed = allowed_tables_for(st.session_state.domain, all_tables)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("**Chat**")

for msg in st.session_state.messages:
    with st.chat_message("user" if msg["role"] == "user" else "assistant"):
        st.write(msg["content"])
        if msg["role"] == "assistant" and isinstance(msg.get("meta"), dict):
            with st.expander("Details / Debug"):
                meta = msg["meta"].copy()
                for k in ["rows", "columns"]:
                    meta.pop(k, None)
                st.json(meta)

user_q = st.chat_input(f"Ask about {st.session_state.domain.lower()} data…", key="chat_input_unique")
if user_q:
    st.session_state.messages.append({"role": "user", "content": user_q, "meta": {"domain": st.session_state.domain}})
    with st.spinner("Thinking..."):
        result = run_query(user_q, st.session_state.domain, allowed)
    st.session_state.messages.append({"role": "assistant", "content": result.get("query_result") or "No response.", "meta": result})
    st.rerun()

st.markdown('</div>', unsafe_allow_html=True)
