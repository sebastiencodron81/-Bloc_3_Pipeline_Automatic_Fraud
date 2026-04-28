"""
daily_report.py
===============
Genere le rapport quotidien des paiements et fraudes de la veille.

Lance par un cron a 6h du matin (ou via docker-compose avec un service
dedie). Produit un fichier JSON et envoie un mail recapitulatif.

Usage :
  python daily_report.py [--date YYYY-MM-DD]

Sans argument, traite la veille (NOW - 1 day).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://fraud:fraud@localhost:5432/fraud"
)
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "./reports"))
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
ALERT_FROM = os.getenv("ALERT_FROM", SMTP_USER)
REPORT_TO = os.getenv("REPORT_TO", "")


logging.basicConfig(level=logging.INFO, format="[%(asctime)s][report] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ----------------------------------------------------------------------

def build_report(date: str) -> dict:
    """Aggrege les statistiques de la journee."""
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    sql = text("""
        SELECT *
        FROM payments_gold
        WHERE DATE(trans_date_trans_time) = :d
    """)
    df = pd.read_sql(sql, engine, params={"d": date})

    if df.empty:
        return {"date": date, "total": 0, "fraudes": 0, "details": "Aucune transaction"}

    fraudes = df[df["is_fraud"] == 1]

    return {
        "date": date,
        "total_transactions": int(len(df)),
        "total_fraudes": int(len(fraudes)),
        "taux_fraude_pct": round(100 * len(fraudes) / len(df), 3),
        "montant_total": float(df["amt"].sum()),
        "montant_fraude": float(fraudes["amt"].sum()),
        "top_categories_frauduleuses": (
            fraudes["category"].value_counts().head(5).to_dict()
        ),
        "top_etats": fraudes["state"].value_counts().head(5).to_dict(),
        "score_moyen_fraude": round(float(fraudes["score"].mean()), 3) if len(fraudes) else 0,
    }


def save_report(report: dict, date: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"daily_report_{date}.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return path


def format_email(report: dict) -> str:
    return (
        f"Rapport quotidien - {report['date']}\n\n"
        f"  Transactions traitees    : {report.get('total_transactions', 0):,}\n"
        f"  Fraudes detectees        : {report.get('total_fraudes', 0):,}\n"
        f"  Taux de fraude           : {report.get('taux_fraude_pct', 0):.2f} %\n"
        f"  Montant total            : ${report.get('montant_total', 0):,.2f}\n"
        f"  Montant fraude           : ${report.get('montant_fraude', 0):,.2f}\n"
        f"  Score moyen fraude       : {report.get('score_moyen_fraude', 0):.3f}\n\n"
        f"  Top categories frauduleuses : {report.get('top_categories_frauduleuses', {})}\n"
        f"  Top etats               : {report.get('top_etats', {})}\n"
    )


def send_email(report: dict) -> None:
    if not SMTP_USER or not REPORT_TO:
        log.info("SMTP non configure, skip envoi mail.")
        return
    body = format_email(report)
    msg = MIMEText(body)
    msg["Subject"] = f"[Rapport fraude] {report['date']}"
    msg["From"] = ALERT_FROM
    msg["To"] = REPORT_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
    log.info(f"Mail envoye a {REPORT_TO}")


# ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (defaut : hier)", default=None)
    args = parser.parse_args()

    if args.date:
        date = args.date
    else:
        date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    log.info(f"Generation rapport pour {date}...")
    report = build_report(date)
    path = save_report(report, date)
    log.info(f"Rapport ecrit : {path}")
    print(format_email(report))
    send_email(report)


if __name__ == "__main__":
    main()
