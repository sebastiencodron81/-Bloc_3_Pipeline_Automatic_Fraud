"""Dashboard Streamlit pour le pipeline de detection de fraude."""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraud@localhost:5432/fraud",
)

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon=":rotating_light:",
    layout="wide",
)


@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


@st.cache_data(ttl=10)
def load_recent(n: int = 100) -> pd.DataFrame:
    engine = get_engine()
    sql = text(
        """
        SELECT trans_num, trans_date_trans_time, merchant, category,
               amt, state, city, is_fraud, ground_truth, score, predicted_at
        FROM payments_gold
        ORDER BY ingested_at DESC
        LIMIT :n
        """
    )
    return pd.read_sql(sql, engine, params={"n": n})


@st.cache_data(ttl=30)
def load_kpis() -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM payments_gold")).scalar() or 0
        frauds_pred = (
            conn.execute(
                text("SELECT COUNT(*) FROM payments_gold WHERE is_fraud=1")
            ).scalar()
            or 0
        )
        amt_fraud = (
            conn.execute(
                text("SELECT COALESCE(SUM(amt),0) FROM payments_gold WHERE is_fraud=1")
            ).scalar()
            or 0
        )
        last_hour = (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM payments_gold "
                    "WHERE ingested_at >= NOW() - INTERVAL '1 hour'"
                )
            ).scalar()
            or 0
        )
    return {
        "total": total,
        "frauds_pred": frauds_pred,
        "fraud_rate": (frauds_pred / total * 100) if total else 0,
        "amt_fraud_sum": float(amt_fraud),
        "last_hour": last_hour,
    }


@st.cache_data(ttl=30)
def load_model_metrics() -> dict:
    engine = get_engine()
    sql = text(
        """
        SELECT
          SUM(CASE WHEN is_fraud=1 AND ground_truth=1 THEN 1 ELSE 0 END) AS tp,
          SUM(CASE WHEN is_fraud=1 AND ground_truth=0 THEN 1 ELSE 0 END) AS fp,
          SUM(CASE WHEN is_fraud=0 AND ground_truth=1 THEN 1 ELSE 0 END) AS fn_,
          SUM(CASE WHEN is_fraud=0 AND ground_truth=0 THEN 1 ELSE 0 END) AS tn,
          COUNT(*) AS n_with_truth
        FROM payments_gold
        WHERE ground_truth IS NOT NULL
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).first()

    tp, fp, fn, tn, n = row.tp or 0, row.fp or 0, row.fn_ or 0, row.tn or 0, row.n_with_truth or 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision and recall and (precision + recall) > 0
        else None
    )
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "n_with_truth": n,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


@st.cache_data(ttl=120)
def fraud_by_hour() -> pd.DataFrame:
    engine = get_engine()
    sql = text(
        """
        SELECT EXTRACT(HOUR FROM trans_date_trans_time)::INT AS hour,
               COUNT(*) FILTER (WHERE is_fraud=1) AS frauds,
               COUNT(*) AS total
        FROM payments_gold
        GROUP BY 1
        ORDER BY 1
        """
    )
    return pd.read_sql(sql, engine)


@st.cache_data(ttl=120)
def fraud_by_category() -> pd.DataFrame:
    engine = get_engine()
    sql = text(
        """
        SELECT category,
               COUNT(*) FILTER (WHERE is_fraud=1) AS frauds,
               COUNT(*) AS total,
               ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud=1) / COUNT(*), 2) AS fraud_rate_pct
        FROM payments_gold
        GROUP BY category
        ORDER BY fraud_rate_pct DESC
        """
    )
    return pd.read_sql(sql, engine)


st.title(":rotating_light: Fraud Detection - Pipeline Bloc 3")
st.caption("Dashboard temps reel sur PostgreSQL")

if st.button("Refresh", use_container_width=False):
    st.cache_data.clear()
    st.rerun()

try:
    kpis = load_kpis()
except Exception as e:
    st.error(f"Impossible de joindre la base : {e}")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions traitees", f"{kpis['total']:,}")
c2.metric("Fraudes predites", f"{kpis['frauds_pred']:,}")
c3.metric("Taux de fraude", f"{kpis['fraud_rate']:.2f} %")
c4.metric("Montant fraude (cumul)", f"${kpis['amt_fraud_sum']:,.0f}")
c5.metric("Trafic 1h", f"{kpis['last_hour']:,}")

st.divider()
st.subheader("Performance live du modele (vs ground truth)")

metrics = load_model_metrics()
if metrics["n_with_truth"] == 0:
    st.info("Aucune transaction avec ground truth disponible pour l'instant.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Precision",
        f"{metrics['precision']:.2%}" if metrics["precision"] is not None else "N/A",
        help="TP / (TP + FP)",
    )
    m2.metric(
        "Recall",
        f"{metrics['recall']:.2%}" if metrics["recall"] is not None else "N/A",
        help="TP / (TP + FN)",
    )
    m3.metric(
        "F1-score",
        f"{metrics['f1']:.2%}" if metrics["f1"] is not None else "N/A",
    )
    m4.metric("Echantillon", f"{metrics['n_with_truth']:,}")

    cm_col1, cm_col2 = st.columns([1, 2])
    with cm_col1:
        st.markdown("**Matrice de confusion**")
        cm = pd.DataFrame(
            [
                ["TP", metrics["tp"]],
                ["FP", metrics["fp"]],
                ["FN", metrics["fn"]],
                ["TN", metrics["tn"]],
            ],
            columns=["Type", "Count"],
        )
        st.dataframe(cm, use_container_width=True, hide_index=True)

tab_live, tab_categ, tab_horaire = st.tabs(
    ["Live", "Par categorie", "Par heure"]
)

with tab_live:
    st.subheader("100 dernieres transactions")
    df_live = load_recent(100)

    def highlight_fraud(row):
        return [
            "background-color: #ffe5e5" if row.is_fraud == 1 else ""
            for _ in row
        ]

    st.dataframe(
        df_live.style.apply(highlight_fraud, axis=1),
        use_container_width=True,
        height=500,
    )

with tab_categ:
    st.subheader("Taux de fraude par categorie")
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
        st.info("Aucune donnee disponible.")

st.caption(f"Mis a jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
