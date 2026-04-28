"""
fake_api.py
===========
Simulateur local de l'API temps reel des paiements.

Remplace le service Heroku d'origine (qui est down) par un serveur
FastAPI local qui rejoue les lignes de fraudTest.csv. Le contrat de
sortie est strictement identique a celui qu'aurait du fournir l'API
de production : meme schema de colonnes, sans la verite terrain
(is_fraud) puisque c'est ce que le modele doit predire.

Endpoints exposes :
  GET  /                         -> page d'accueil texte
  GET  /health                   -> {"status": "ok", "rows_loaded": N}
  GET  /current-transactions     -> 1 transaction au format Pandas split
  GET  /current-transactions?n=N -> N transactions (1 <= N <= 1000)

Format de reponse (Pandas split, le plus generique) :
  {
    "columns": ["trans_date_trans_time", "cc_num", ...],
    "data":    [[...valeurs ligne 1...], [...ligne 2...]],
    "index":   [0, 1, ...]
  }

Usage :
  pip install -r requirements.txt
  python fake_api.py
  # ouvre http://127.0.0.1:8000/current-transactions

Astuce :
  Pour pointer ton inspecteur ou ton pipeline vers ce simulateur, utilise
  l'URL : http://127.0.0.1:8000/current-transactions
"""

from __future__ import annotations

import logging
import os
import random
import urllib.request
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse


# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

CSV_PATH = Path(__file__).parent / "fraudTest.csv"
CSV_URL = "https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv"
TARGET_COL = "is_fraud"     # Colonne label : retiree pour imiter la prod

# Lit HOST depuis l'environnement.
#   - "127.0.0.1" en dev local (sécurisé, n'accepte que les connexions locales)
#   - "0.0.0.0" dans Docker (accepte les connexions des autres conteneurs)
# Le docker-compose.yml definit HOST=0.0.0.0 pour le service fake-api.
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="[fake_api] %(message)s",
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# CHARGEMENT DU DATASET (au demarrage, en memoire)
# ----------------------------------------------------------------------

def ensure_csv(path: Path, url: str) -> None:
    """
    Garantit que le CSV existe et n'est pas vide.
    Sinon, le telecharge depuis l'URL S3 fournie par Jedha.
    """
    if path.exists() and path.stat().st_size > 1024:
        log.info(f"CSV trouve ({path.stat().st_size / 1e6:.1f} Mo) : {path.name}")
        return

    if path.exists():
        log.info(f"CSV present mais vide ({path.stat().st_size} bytes), retelechargement...")
    else:
        log.info(f"CSV absent, telechargement depuis {url}...")

    def _progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 // total_size)
            print(f"\r[fake_api] Telechargement : {pct}% "
                  f"({downloaded / 1e6:.1f} / {total_size / 1e6:.1f} Mo)",
                  end="", flush=True)

    try:
        urllib.request.urlretrieve(url, path, reporthook=_progress)
        print()  # newline apres la barre de progression
        log.info(f"OK ({path.stat().st_size / 1e6:.1f} Mo telecharges)")
    except Exception as e:
        raise SystemExit(
            f"\nERREUR : impossible de telecharger le CSV depuis {url}\n"
            f"Detail : {e}\n"
            f"Telecharge-le manuellement et place-le a : {path}"
        )


ensure_csv(CSV_PATH, CSV_URL)
log.info(f"Chargement de {CSV_PATH.name}...")
df_full = pd.read_csv(CSV_PATH)

# Nettoyage de colonnes parasites de type "Unnamed: 0"
df_full = df_full.loc[:, ~df_full.columns.str.contains("^Unnamed")]

# On retire le label : l'API en production ne le fournit jamais,
# c'est l'objet meme de la prediction
if TARGET_COL in df_full.columns:
    df_features = df_full.drop(columns=[TARGET_COL])
else:
    df_features = df_full.copy()

log.info(f"Dataset charge : {len(df_features):,} lignes, {len(df_features.columns)} colonnes")
log.info(f"Colonnes exposees : {list(df_features.columns)}")

# Pour le mode demo : on conserve aussi un sous-ensemble des fraudes
# uniquement, afin de pouvoir oversampler quand le client le demande.
# Note : on ne stocke jamais la colonne is_fraud dans la reponse.
if TARGET_COL in df_full.columns:
    _fraud_indices = df_full.index[df_full[TARGET_COL] == 1].tolist()
    log.info(f"Indices de fraudes disponibles pour mode demo : {len(_fraud_indices):,}")
else:
    _fraud_indices = []


# ----------------------------------------------------------------------
# FASTAPI APP
# ----------------------------------------------------------------------

app = FastAPI(
    title="Fake Payments API",
    description="Simulateur local pour le projet Bloc 3 RNCP38777 - detection de fraude",
    version="1.0",
)


@app.get("/", response_class=HTMLResponse)
def root():
    return f"""
    <html><body style="font-family:sans-serif;max-width:720px;margin:40px auto;">
      <h2>Fake Payments API</h2>
      <p>Simulateur local pour le projet de detection de fraude.</p>
      <ul>
        <li><a href="/health">/health</a> - etat du service</li>
        <li><a href="/current-transactions">/current-transactions</a> - 1 paiement</li>
        <li><a href="/current-transactions?n=10">/current-transactions?n=10</a> - 10 paiements</li>
      </ul>
      <p>Dataset charge : <b>{len(df_features):,}</b> lignes, <b>{len(df_features.columns)}</b> colonnes.</p>
    </body></html>
    """


@app.get("/health")
def health():
    return {"status": "ok", "rows_loaded": int(len(df_features))}


@app.get("/current-transactions")
def current_transactions(
    n: int = Query(1, ge=1, le=1000, description="Nombre de transactions a renvoyer (1-1000)"),
    fraud_rate: float = Query(
        0.0, ge=0.0, le=1.0,
        description=(
            "Mode demo : ratio de fraudes a injecter (0.0 = sampling pur naturel ~0.39%, "
            "0.1 = 10% de fraudes pour rendre la demo plus visuelle). "
            "Le contenu reste de vraies transactions du dataset, ne contient JAMAIS is_fraud."
        ),
    ),
):
    """
    Renvoie n transactions tirees aleatoirement, format Pandas split.

    Si fraud_rate > 0, on oversample les fraudes du dataset historique
    pour rendre la demo plus visuelle (sinon il faut ~10 min pour voir
    une fraude au taux naturel de 0.39%).
    """
    if fraud_rate > 0 and _fraud_indices:
        n_fraud = int(round(n * fraud_rate))
        n_legit = n - n_fraud
        # Echantillonne fraudes + legitimes puis melange
        fraud_idx = random.sample(_fraud_indices, min(n_fraud, len(_fraud_indices)))
        sample_fraud = df_features.loc[fraud_idx]
        sample_legit = df_features.sample(n=n_legit, random_state=random.randint(0, 10**6))
        sample = pd.concat([sample_fraud, sample_legit]).sample(frac=1)
    else:
        sample = df_features.sample(n=n, random_state=random.randint(0, 10**6))

    payload = sample.to_dict(orient="split")
    return JSONResponse(content=payload)


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    log.info(f"Demarrage sur http://{HOST}:{PORT}")
    log.info("Ctrl+C pour arreter.")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
