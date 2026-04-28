"""
dashboard.py
============
Dashboard Streamlit pour visualiser le pipeline de detection de fraude
en temps reel et en historique.

Deux sources :
  - Live : query Postgres pour les 100 dernieres transactions
  - Historique : agregations sur payments_gold

Usage :
  streamlit run dashboard.py

Variable d'environnement :
  DATABASE_URL : connexion Postgres
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraud@localhost:5432/fraud"
)

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🚨",
    layout="wide",
)


# ----------------------------------------------------------------------
# DATA ACCESS (avec cache pour limiter les appels)
# ----------------------------------------------------------------------

@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


@st.cache_data(ttl=10)
def load_recent(n: int = 100) -> pd.DataFrame:
    engine = get_engine()
    sql = text("""
        SELECT trans_num, trans_date_trans_time, merchant, category,
               amt, state, city, is_fraud, score, predicted_at
        FROM payments_gold
        ORDER BY ingested_at DESC
        LIMIT :n
    """)
    return pd.read_sql(sql, engine, params={"n": n})


@st.cache_data(ttl=60)
def load_kpis() -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM payments_gold")).scalar() or 0
        frauds = conn.execute(text("SELECT COUNT(*) FROM payments_gold WHERE is_fraud=1")).scalar() or 0
        amt_fraud = conn.execute(text("SELECT COALESCE(SUM(amt),0) FROM payments_gold WHERE is_fraud=1")).scalar() or 0
        last_hour = conn.execute(text(
            "SELECT COUNT(*) FROM payments_gold WHERE ingested_at >= NOW() - INTERVAL '1 hour'"
        )).scalar() or 0
    return {
        "total": total,
        "frauds": frauds,
        "fraud_rate": (frauds / total * 100) if total else 0,
        "amt_fraud_sum": float(amt_fraud),
        "last_hour": last_hour,
    }


@st.cache_data(ttl=120)
def fraud_by_hour() -> pd.DataFrame:
    engine = get_engine()
    sql = text("""
        SELECT EXTRACT(HOUR FROM trans_date_trans_time)::INT AS hour,
               COUNT(*) FILTER (WHERE is_fraud=1) AS frauds,
               COUNT(*) AS total
        FROM payments_gold
        GROUP BY 1
        ORDER BY 1
    """)
    return pd.read_sql(sql, engine)


@st.cache_data(ttl=120)
def fraud_by_category() -> pd.DataFrame:
    engine = get_engine()
    sql = text("""
        SELECT category,
               COUNT(*) FILTER (WHERE is_fraud=1) AS frauds,
               COUNT(*) AS total,
               ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud=1) / COUNT(*), 2) AS fraud_rate_pct
        FROM payments_gold
        GROUP BY category
        ORDER BY fraud_rate_pct DESC
    """)
    return pd.read_sql(sql, engine)


# ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------

st.title("🚨 Fraud Detection - Pipeline Bloc 3")
st.caption("Dashboard temps reel et historique sur PostgreSQL")

# Bouton refresh
col_refresh, _ = st.columns([1, 5])
if col_refresh.button("🔄 Refresh", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# KPIs
try:
    kpis = load_kpis()
except Exception as e:
    st.error(f"Impossible de joindre la base : {e}")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions traitees", f"{kpis['total']:,}")
c2.metric("Fraudes detectees", f"{kpis['frauds']:,}")
c3.metric("Taux de fraude", f"{kpis['fraud_rate']:.2f} %")
c4.metric("Montant fraude (cumul)", f"${kpis['amt_fraud_sum']:,.0f}")
c5.metric("Trafic 1h", f"{kpis['last_hour']:,}")

# Onglets
tab_live, tab_categ, tab_horaire = st.tabs(["📡 Live", "🏪 Par categorie", "🕐 Par heure"])

with tab_live:
    st.subheader("100 dernieres transactions")
    df_live = load_recent(100)

    def highlight_fraud(row):
        return ['background-color: #ffe5e5' if row.is_fraud == 1 else '' for _ in row]

    st.dataframe(
        df_live.style.apply(highlight_fraud, axis=1),
        use_container_width=True,
        height=500,
    )

with tab_categ:
    st.subheader("Taux de fraude par categorie de marchand")
    df_cat = fraud_by_category()
    st.bar_chart(df_cat.set_index("category")["fraud_rate_pct"])
    st.dataframe(df_cat, use_container_width=True)

with tab_horaire:
    st.subheader("Volumes par heure de la journee")
    df_h = fraud_by_hour()
    if not df_h.empty:
        df_h["fraud_rate_pct"] = (df_h["frauds"] / df_h["total"] * 100).round(2)
        st.line_chart(df_h.set_index("hour")[["total", "frauds"]])
        st.bar_chart(df_h.set_index("hour")["fraud_rate_pct"])
    else:
        st.info("Aucune donnee encore disponible.")

st.caption(f"Mis a jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
