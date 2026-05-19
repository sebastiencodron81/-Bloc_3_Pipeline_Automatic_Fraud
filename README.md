# 🚨 Pipeline de détection de fraude — Architecture Kappa

> Certification RNCP 38777 — Bloc 3 | Jedha Lead Bootcamp
> **Auteur** : Sébastien Codron · `sebastien.codron375@gmail.com`

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.4-F7931E?logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![MLflow](https://img.shields.io/badge/MLflow-2.13-0194E2?logo=mlflow&logoColor=white)](https://mlflow.org/)

---

## 🎥 Démonstration vidéo

▶️ **Voir la démo en action** : https://youtu.be/DLZHrRfsCUQ

Vidéo courte de l'infrastructure en production : Kafka, scoring ML, dashboard temps réel, persistance PostgreSQL.

---

## 📋 Présentation du projet

Ce projet construit un **pipeline de données temps réel** capable de détecter la fraude sur les paiements par carte bancaire en moins de 5 secondes, et de produire un rapport quotidien automatisé.

L'architecture suit le **pattern Kappa** (Jay Kreps, 2014) : un pipeline 100 % streaming centré sur Apache Kafka, sans duplication de logique batch/stream.

### Le besoin métier

Une institution financière a deux exigences distinctes :

1. **Notification immédiate** dès qu'une fraude est détectée (latence < 5 s) → flux streaming
2. **Rapport quotidien** des paiements et fraudes de la veille → agrégation SQL programmée

### Le résultat

- **555 719 transactions** historiques pour entraîner et valider
- **0,39 %** de fraudes (déséquilibre extrême, comme dans la réalité)
- **Latence end-to-end** : moins de 5 secondes du paiement à l'alerte
- **Démarrage en une commande** : `docker compose up -d`

---

## 🏗️ Architecture

L'architecture s'articule autour de **3 zones** :

```
   SOURCE                    PROCESSING (Kafka)                 CONSUMPTION
   ──────                    ─────────────────                  ───────────
   API HF Spaces  ─►   producer  ─►  raw-transactions   ─►   storage   ─►  PostgreSQL
   (streaming)         (Pydantic +     ─►  predictor                       (Gold table)
                        SHA-256)        ─►  fraud-predictions  ─►  alerter   ─►  Webhook
                                        ─►  dlq-raw                          dashboard
                                            (quarantaine)                    Streamlit
```

> 🎨 **Schéma visuel haute définition** : [`architecture.html`](./architecture.html) — ouvre-le dans un navigateur en plein écran (F11)
> 📄 **Document détaillé** : [`architecture.md`](./architecture.md)

### Stack technique

| Couche | Technologie | Rôle |
|---|---|---|
| **Source** | API REST distante (Hugging Face Spaces) | Flux temps réel de paiements au format JSON |
| **Bus de messages** | Apache Kafka 7.5 | Streaming + DLQ + replay via offsets |
| **Validation** | Pydantic v2 | Schéma strict + coerce numbers to str |
| **ML** | scikit-learn 1.4 | RandomForest balanced, pipeline reproductible |
| **Tracking ML** | MLflow + MinIO | Experiments + registry + artefacts S3-compatible |
| **Persistance** | PostgreSQL 16 | UPSERT idempotent sur `payments_gold` |
| **Dashboard** | Streamlit | KPIs temps réel + agrégations |
| **Alerting** | Webhook / SMTP | Notification fraude avec rate-limiting |
| **Orchestration** | Docker Compose | 12 services en une commande |
| **Monitoring** | Kafka UI + logs Docker | Lag consumers + débit |

---

## 🚀 Quick start

### Pré-requis

- Docker Desktop installé
- Python 3.10+
- Git

### En 4 commandes

```bash
# 1. Cloner et entrer dans le projet
git clone <repo-url> fraud-detection-pipeline
cd fraud-detection-pipeline

# 2. Installer les dépendances Python
pip install -r requirements.txt

# 3. Entraîner le modèle (5-10 min) → produit model.pkl
jupyter notebook notebook_eda_baseline.ipynb
# (exécuter toutes les cellules)

# 4. Lancer la stack complète
docker compose up -d
docker compose logs -f
```

### Accès aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| 📊 Dashboard Streamlit | http://localhost:8501 | — |
| 📡 Kafka UI | http://localhost:8080 | — |
| 🔬 MLflow | http://localhost:5000 | — |
| 💾 MinIO Console | http://localhost:9001 | minio / miniominio |
| 🌐 API source | https://sdacelo-real-time-fraud-detection.hf.space | — (publique) |

---

## 🎯 Couverture des 5 compétences du Bloc 3

| Compétence | Composants démontrant la maîtrise |
|---|---|
| **C1** — Système temps réel | Kafka topics, producer cron 6 s, consumer streaming, latence < 5 s |
| **C2** — Pipeline ETL/ELT | Producer (Extract) → Kafka → Consumer (Transform : features + predict) → Postgres (Load) |
| **C3** — Automatisation | Cron polling, retries exponentiels, idempotence offsets Kafka, Docker Compose |
| **C4** — Monitoring & gouvernance | Kafka UI, dashboard Streamlit, hash SHA-256 cc_num (RGPD), exclusion PII |
| **C5** — Qualité & correction | Validation Pydantic, DLQ topic dédié, UPSERT idempotent, rate-limit alertes |

---

## 📂 Structure du projet

```
fraud-detection-pipeline/
├── 📄 README.md                                    Ce fichier
├── 📄 architecture.md                              Architecture détaillée (markdown)
├── 🎨 architecture.html                            Schéma visuel HTML/SVG
├── 📋 Dossier_Projet_RNCP38777_Bloc3_Codron.docx  Dossier projet (15 pages)
├── 🎤 Soutenance_RNCP38777_Bloc3_Fraude.pptx      Slides de soutenance (5 min)
├── 📊 Dictionnaire_Donnees_Fraud.xlsx             Documentation des 22 colonnes
│
├── 🐳 Dockerfile                                   Image Python commune
├── 🐳 docker-compose.yml                           Orchestration des 12 services
├── 📦 requirements.txt                             Dépendances Python
├── 🔑 .env.example                                 Template variables sensibles
│
├── 🧪 notebook_eda_baseline.ipynb                  EDA + entraînement RandomForest
├── 🔧 features.py                                  Feature engineering partagé
├── 📐 schemas.py                                   Schémas Pydantic
│
├── 🔍 inspect_api.py                               Inspecteur de schéma API/CSV
│
├── ▶️  producer.py                                  Producer Kafka + DLQ
├── 🤖 consumer_predictor.py                        Consumer ML scoring
├── 💾 consumer_storage.py                          Consumer persistance Postgres
├── 🚨 consumer_alerter.py                          Consumer alertes mail/webhook
│
├── 📊 dashboard.py                                 Dashboard Streamlit
└── 📅 daily_report.py                              Rapport quotidien batch
```

---

## 🔬 Modèle ML

| Aspect | Choix |
|---|---|
| Algorithme | `RandomForestClassifier(class_weight='balanced')` |
| Hyperparamètres | n_estimators=150, max_depth=20, min_samples_leaf=10 |
| Features brutes | 10 colonnes (montant, catégorie, géoloc, ville, état, …) |
| Features dérivées | 7 features (age, hour, day_of_week, is_night, distance_haversine, amt_log) |
| Pipeline | `FraudFeatureBuilder + ColumnTransformer + RandomForest` (un seul artefact pickle) |
| Métriques | PR-AUC (Average Precision), F1, Recall (adaptées au déséquilibre 0,39 %) |
| Tracking | MLflow + artefacts MinIO S3-compatible |

> 💡 **No train-serve skew** : le `FraudFeatureBuilder` est importé depuis [`features.py`](./features.py) à la fois par le notebook d'entraînement et par le consumer en production — garantie que les transformations sont strictement identiques.

---

## 🛡️ Conformité & qualité

### RGPD
- **Pseudonymisation** systématique du numéro de carte (`cc_num`) par hash SHA-256 dans le producer **avant publication Kafka**
- **Exclusion PII de l'inférence** : `first`, `last`, `street`, `dob` brut ne sont jamais utilisés comme features
- **Variables d'environnement** pour les secrets (jamais en dur dans le code)

### Qualité de la donnée
- **Validation Pydantic stricte** sur 21 champs avec types, regex, bornes
- **Dead Letter Queue** dédiée (`dlq-raw`) pour isoler les messages corrompus
- **UPSERT idempotent** sur Postgres → aucun doublon même après replay complet du topic
- **Retry exponentiel** sur fetch API et publication Kafka (2s/4s/8s/16s/32s)

---

## 🛣️ Perspectives d'évolution

### Court terme
- Avro + Schema Registry pour la compatibilité ascendante des schémas
- Prometheus + Grafana pour le monitoring système (CPU, RAM, lag)
- Tests Great Expectations en CI

### Moyen terme
- Detection de drift automatisée (Evidently AI)
- Réentraînement automatique périodique avec validation A/B
- Feature store (table d'agrégats utilisateur)

### Long terme
- Modèle ensembliste (RandomForest supervisé + Isolation Forest)
- TLS Kafka, RBAC Postgres, secrets dans Vault
- Cluster Kafka multi-brokers + partitionnement par hash de `trans_num`

---

## 📝 Auteur

**Sébastien Codron**
Jedha Lead Bootcamp — Promotion 2026
RNCP 38777 — Bloc 3 — Niveau 7 (Bac+5)

📧 `sebastien.codron375@gmail.com`

---

*Pipeline conçu et implémenté dans le cadre de la certification RNCP 38777 - Bloc 3.*
