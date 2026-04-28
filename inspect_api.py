"""
inspect_api.py
==============
Inspecte l'API des paiements (locale ou distante) et la compare au CSV
d'entrainement, afin d'identifier les features utilisables pour
construire le modele ML.

Le contrat est simple :
  - les colonnes utilisables pour l'entrainement = (CSV ∩ API) - PII - label
  - tout ce qui est dans le CSV mais pas dans l'API doit etre soit derive,
    soit abandonne
  - tout ce qui est dans l'API mais pas dans le CSV est suspect

Sorties produites :
  - api_sample.json : echantillon brut de la reponse API
  - api_report.md   : rapport markdown lisible et reutilisable
  - terminal        : resume

Configuration par defaut : pointe vers le simulateur local (fake_api.py).
Modifie API_URL si tu veux interroger un autre service.

Usage :
  # Terminal 1
  python fake_api.py

  # Terminal 2
  python inspect_api.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests


# =============================================================================
# CONFIGURATION
# =============================================================================

API_URL = "http://127.0.0.1:8000/current-transactions"
CSV_PATH = Path(__file__).parent / "fraudTest.csv"
OUT_SAMPLE = Path(__file__).parent / "api_sample.json"
OUT_REPORT = Path(__file__).parent / "api_report.md"

# Nombre de transactions a demander a l'API pour avoir un bon echantillon
SAMPLE_SIZE = 50

# Colonnes a exclure de l'entrainement : PII et identifiants uniques
PII_OR_LEAKY = {
    "cc_num",       # Numero de carte = identifiant unique + PII sensible
    "first", "last",
    "street",
    "trans_num",    # Identifiant unique de transaction (fuite memoire)
    "unix_time",    # Redondant avec trans_date_trans_time
    "dob",          # Date de naissance brute (utiliser l'age derive a la place)
}

# Le label cible n'est par definition pas dans l'API
TARGET = "is_fraud"


# =============================================================================
# FETCH
# =============================================================================

def fetch_api(url: str, timeout: int = 15, max_retries: int = 3) -> dict | list:
    """Recupere la reponse JSON. Retry simple en cas d'erreur transitoire."""
    print(f"[1/4] Appel : {url}")

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                params={"n": SAMPLE_SIZE} if "current-transactions" in url else None,
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
            print(f"      OK (status {response.status_code}, "
                  f"{len(response.content)} bytes)")
            return payload
        except (requests.exceptions.HTTPError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"      Tentative {attempt}/{max_retries} echouee. "
                      f"Retry dans {wait}s.")
                time.sleep(wait)
            else:
                sys.exit(
                    f"\nERREUR : impossible de joindre {url}\n"
                    f"Verifie que fake_api.py tourne dans un autre terminal.\n"
                    f"Detail : {e}"
                )


# =============================================================================
# NORMALIZE
# =============================================================================

def normalize_to_dataframe(payload) -> pd.DataFrame:
    """Convertit n'importe quel format de reponse en DataFrame."""
    print("[2/4] Normalisation en DataFrame...")

    if isinstance(payload, list):
        df = pd.DataFrame(payload)
    elif isinstance(payload, dict):
        # Format Pandas split (le plus probable)
        if "columns" in payload and "data" in payload:
            df = pd.DataFrame(payload["data"], columns=payload["columns"])
        elif "data" in payload and isinstance(payload["data"], list):
            df = pd.DataFrame(payload["data"])
        else:
            df = pd.DataFrame([payload])
    else:
        raise ValueError(f"Format inattendu : {type(payload)}")

    # Nettoyage colonnes parasites
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    print(f"      DataFrame API : {df.shape[0]} lignes x {df.shape[1]} colonnes")
    return df


# =============================================================================
# LOAD CSV
# =============================================================================

def load_csv(path: Path, n: int = 5_000) -> pd.DataFrame:
    """Charge un echantillon du CSV (suffit pour l'analyse de schema)."""
    print(f"[3/4] Chargement CSV : {path.name}")

    if not path.exists() or path.stat().st_size < 1024:
        sys.exit(
            f"\nERREUR : CSV manquant ou vide a {path}\n"
            f"Demarre d'abord fake_api.py qui le telechargera automatiquement, "
            f"ou recupere-le manuellement depuis :\n"
            f"  https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv"
        )

    df = pd.read_csv(path, nrows=n)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    print(f"      DataFrame CSV : {df.shape[0]} lignes (sample) x {df.shape[1]} colonnes")
    return df


# =============================================================================
# COMPARE
# =============================================================================

def compare_schemas(df_api: pd.DataFrame, df_csv: pd.DataFrame) -> dict:
    cols_api = set(df_api.columns)
    cols_csv = set(df_csv.columns)

    common = cols_csv & cols_api
    only_csv = cols_csv - cols_api
    only_api = cols_api - cols_csv

    usable = common - PII_OR_LEAKY - {TARGET}

    return {
        "common": sorted(common),
        "only_csv": sorted(only_csv),
        "only_api": sorted(only_api),
        "usable_for_training": sorted(usable),
        "excluded_pii": sorted(common & PII_OR_LEAKY),
    }


# =============================================================================
# REPORT
# =============================================================================

def write_outputs(diff: dict, df_api: pd.DataFrame, df_csv: pd.DataFrame, payload) -> None:
    print("[4/4] Generation des sorties...")

    # JSON brut
    OUT_SAMPLE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    # Rapport markdown
    md = []
    md.append("# Rapport de comparaison API <-> CSV")
    md.append("")
    md.append(f"- Source API : `{API_URL}`")
    md.append(f"- Source CSV : `{CSV_PATH.name}`")
    md.append("")
    md.append("## Dimensions")
    md.append("")
    md.append(f"- API : **{df_api.shape[0]} lignes**, **{df_api.shape[1]} colonnes**")
    md.append(f"- CSV : **{df_csv.shape[0]} lignes** (sample), **{df_csv.shape[1]} colonnes**")
    md.append("")

    md.append("## Colonnes communes (CSV ∩ API)")
    md.append("")
    for c in diff["common"]:
        md.append(f"- `{c}`")
    md.append("")

    md.append("## Colonnes uniquement dans le CSV")
    md.append("(typiquement le label `is_fraud` que l'API ne fournit pas, c'est normal)")
    md.append("")
    if diff["only_csv"]:
        for c in diff["only_csv"]:
            md.append(f"- `{c}`")
    else:
        md.append("- _(aucune)_")
    md.append("")

    md.append("## Colonnes uniquement dans l'API")
    md.append("(suspect : a investiguer si non vide)")
    md.append("")
    if diff["only_api"]:
        for c in diff["only_api"]:
            md.append(f"- `{c}`")
    else:
        md.append("- _(aucune - parfait, schemas alignes)_")
    md.append("")

    md.append("## Colonnes exclues (PII / leaky)")
    md.append("")
    for c in diff["excluded_pii"]:
        md.append(f"- `{c}`")
    md.append("")

    md.append("## ➜ Features utilisables pour l'entrainement ML")
    md.append("")
    md.append("(communes aux deux sources, hors PII, hors label)")
    md.append("")
    for c in diff["usable_for_training"]:
        md.append(f"- `{c}`")
    md.append("")

    md.append("## Types de donnees (10 premieres colonnes communes)")
    md.append("")
    md.append("| Colonne | Type API | Type CSV |")
    md.append("|---|---|---|")
    for c in diff["common"][:10]:
        t_api = str(df_api[c].dtype) if c in df_api.columns else "n/a"
        t_csv = str(df_csv[c].dtype) if c in df_csv.columns else "n/a"
        md.append(f"| `{c}` | {t_api} | {t_csv} |")
    md.append("")

    OUT_REPORT.write_text("\n".join(md), encoding="utf-8")


def print_summary(diff: dict) -> None:
    print()
    print("=" * 70)
    print("RESUME")
    print("=" * 70)
    print(f"Communes              : {len(diff['common'])}")
    print(f"Uniquement CSV        : {len(diff['only_csv'])} -> {diff['only_csv']}")
    print(f"Uniquement API        : {len(diff['only_api'])} -> {diff['only_api']}")
    print(f"Exclues (PII/leaky)   : {len(diff['excluded_pii'])} -> {diff['excluded_pii']}")
    print()
    print(f"Features utilisables  : {len(diff['usable_for_training'])}")
    for c in diff["usable_for_training"]:
        print(f"  - {c}")
    print()
    print(f"Detail brut API       : {OUT_SAMPLE.name}")
    print(f"Rapport markdown      : {OUT_REPORT.name}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    payload = fetch_api(API_URL)
    df_api = normalize_to_dataframe(payload)
    df_csv = load_csv(CSV_PATH)
    diff = compare_schemas(df_api, df_csv)
    write_outputs(diff, df_api, df_csv, payload)
    print_summary(diff)


if __name__ == "__main__":
    main()
