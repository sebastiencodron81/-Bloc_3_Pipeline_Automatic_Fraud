"""
consumer_alerter.py
===================
Consumer Kafka qui envoie une alerte des qu'une fraude est detectee.

Deux backends supportes :
  1. SMTP (defaut) - mail vers une adresse configuree
  2. Webhook       - POST JSON vers Discord/Slack/Teams

Selection via la variable d'environnement ALERT_BACKEND={smtp|webhook}.

Rate limiting : maximum N alertes par minute pour eviter le spam en
cas d'incident massif (les fraudes droppees sont visibles via les logs
et restent en base, donc rien n'est perdu).
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import time
from collections import deque
from email.mime.text import MIMEText

import requests
from kafka import KafkaConsumer


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PRED = os.getenv("TOPIC_PREDICTIONS", "fraud-predictions")
GROUP_ID = os.getenv("GROUP_ID", "alerter-consumer-group")
ALERT_BACKEND = os.getenv("ALERT_BACKEND", "smtp")           # smtp | webhook
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "0.5")) # alerte si score >=
MAX_ALERTS_PER_MIN = int(os.getenv("MAX_ALERTS_PER_MIN", "10"))

# SMTP
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
ALERT_FROM = os.getenv("ALERT_FROM", SMTP_USER)
ALERT_TO = os.getenv("ALERT_TO", "")

# Webhook
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][alerter] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# RATE LIMITER (sliding window)
# ----------------------------------------------------------------------

class RateLimiter:
    def __init__(self, max_per_min: int):
        self.max = max_per_min
        self.window: deque[float] = deque()

    def allow(self) -> bool:
        now = time.time()
        # Purge des entrees > 60s
        while self.window and self.window[0] < now - 60:
            self.window.popleft()
        if len(self.window) >= self.max:
            return False
        self.window.append(now)
        return True


# ----------------------------------------------------------------------
# BACKENDS
# ----------------------------------------------------------------------

def send_smtp(payload: dict) -> None:
    if not SMTP_USER or not ALERT_TO:
        log.warning("SMTP non configure (SMTP_USER ou ALERT_TO vide). Skip.")
        return

    body = (
        f"FRAUDE DETECTEE\n\n"
        f"Transaction : {payload['trans_num']}\n"
        f"Marchand    : {payload['merchant']} ({payload['category']})\n"
        f"Montant     : ${payload['amt']:.2f}\n"
        f"Lieu        : {payload['city']}, {payload['state']}\n"
        f"Score       : {payload['score']:.3f}\n"
        f"Date        : {payload['trans_date_trans_time']}\n"
    )
    msg = MIMEText(body)
    msg["Subject"] = f"[ALERTE FRAUDE] ${payload['amt']:.0f} - {payload['merchant']}"
    msg["From"] = ALERT_FROM
    msg["To"] = ALERT_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def send_webhook(payload: dict) -> None:
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL non configure. Skip.")
        return

    text = (
        f"**FRAUDE DETECTEE**\n"
        f"Transaction `{payload['trans_num']}` - "
        f"${payload['amt']:.2f} chez {payload['merchant']} "
        f"({payload['city']}, {payload['state']})\n"
        f"Score : {payload['score']:.3f}"
    )
    requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)


SEND = {"smtp": send_smtp, "webhook": send_webhook}


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    if ALERT_BACKEND not in SEND:
        raise SystemExit(f"ALERT_BACKEND inconnu : {ALERT_BACKEND}")

    log.info(f"Backend alerte : {ALERT_BACKEND} | seuil score : {SCORE_THRESHOLD}")
    log.info(f"Connexion Kafka {KAFKA_BOOTSTRAP} (group={GROUP_ID})")

    consumer = KafkaConsumer(
        TOPIC_PRED,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="latest",                                # alertes uniquement sur le neuf
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    limiter = RateLimiter(MAX_ALERTS_PER_MIN)
    n_alerts = 0
    n_skipped = 0

    try:
        for msg in consumer:
            payload = msg.value
            if payload.get("is_fraud") != 1 or payload.get("score", 0) < SCORE_THRESHOLD:
                continue

            if not limiter.allow():
                n_skipped += 1
                log.warning(f"Rate limit atteint ({MAX_ALERTS_PER_MIN}/min), alerte droppee.")
                continue

            try:
                SEND[ALERT_BACKEND](payload)
                n_alerts += 1
                log.info(f"Alerte envoyee | trans={payload['trans_num']} | total={n_alerts}")
            except Exception as e:
                log.error(f"Echec envoi alerte : {e}")

    except KeyboardInterrupt:
        log.info("Arret demande.")
    finally:
        consumer.close()
        log.info(f"Fermeture | total alertes={n_alerts} | skipped(rate-limit)={n_skipped}")


if __name__ == "__main__":
    main()
