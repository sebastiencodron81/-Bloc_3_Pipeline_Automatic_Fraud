# Architecture finale — Pipeline de détection de fraude

**Projet** : Bloc 3 RNCP38777 — Pipeline automatisé de détection de fraude
**Auteur** : Sébastien Codron
**Stack** : Python 3.10 · Kafka · PostgreSQL · MLflow · MinIO · Streamlit · Docker Compose
**Pattern architectural** : **Architecture Kappa** (Jay Kreps, 2014) — pipeline 100 % streaming, replay possible via les offsets Kafka, pas de duplication batch/stream

---

## 1. Contexte et constat de départ

L'API d'origine (`real-time-payments-api.herokuapp.com`) **n'est plus opérationnelle** (Heroku a sunset son free tier en novembre 2022). Les données temps réel n'existent donc plus comme prévu par l'énoncé. La solution adoptée est le **replay streaming** : un service local rejoue le dataset `fraudTest.csv` ligne par ligne, exposé via HTTP, en respectant strictement le contrat de la vraie API (mêmes colonnes, sans `is_fraud`).

C'est une pratique standard d'ingénierie data : les banques font tourner leurs pipelines sur des replays historiques pour les tests, et toute l'industrie utilise du **service stubbing** pour découpler dev et dépendances tierces. Le pipeline ne fait pas la différence et bascule sans modification de code le jour où une vraie source serait disponible.

---

## 2. Besoins métier à couvrir

| Besoin | Latence | Mode | Couvert par |
|---|---|---|---|
| Notifier dès qu'une fraude est détectée | < 5 s | Streaming | Topic Kafka + consumer alerte mail |
| Vérifier chaque matin tous les paiements et fraudes de la veille | quelques minutes | Batch | Persistance Postgres + dashboard Streamlit + cron rapport quotidien |

---

## 3. Architecture cible

> 📊 **Schéma visuel haute résolution** : voir `architecture_diagram.svg` (ou ouvrir `architecture.html` dans un navigateur pour le plein écran). Le schéma ASCII ci-dessous reste pour les revues techniques rapides en terminal.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PHASE PRÉPARATOIRE                              │
│                                                                         │
│  fraudTest.csv ──► Notebook EDA ──► Feature engineering ──► RandomForest│
│                                                                  │      │
│                                                                  ▼      │
│                                                     joblib.dump(model)  │
│                                                                  │      │
│                                                                  ▼      │
│                                                          MLflow tracking│
│                                                          + MinIO artifacts
└──────────────────────────────────────────────┬──────────────────────────┘
                                               │ artefact
                                               │ versionne
   ┌───────────────────────────────────────────┼─────────────────────────┐
   │                          PIPELINE EN PRODUCTION                     │
   │                                           │                         │
   │  fake_api.py (replay CSV via HTTP)        │                         │
   │       │                                   │                         │
   │       ▼                                   ▼                         │
   │  producer.py ──► Kafka topic ──► consumer_predictor.py              │
   │  (poll 12s,        raw-           (charge modele MLflow,            │
   │   valide schema,   transactions    feature engineering,             │
   │   hash cc_num)                     predict, publie)                 │
   │       │                                   │                         │
   │       │ schema KO                         ▼                         │
   │       └──► topic dlq-raw       Kafka topic fraud-predictions        │
   │                                           │                         │
   │            ┌──────────────────────────────┼──────────────────────┐  │
   │            ▼                              ▼                      ▼  │
   │    consumer_alerter.py         consumer_storage.py     Streamlit    │
   │    (mail si fraude)            (insert PostgreSQL)     dashboard    │
   │                                           │                         │
   │                                           ▼                         │
   │                                    table payments_gold              │
   │                                           │                         │
   │                                           ▼                         │
   │                                  Cron 6h matin                      │
   │                                  (SELECT yesterday → mail rapport)  │
   │                                                                     │
   │  ─────────────  OBSERVABILITE ET GOUVERNANCE  ───────────────       │
   │                                                                     │
   │  Kafka UI (consumer lag, debit) · Streamlit metrics · logs Docker  │
   │  Hash SHA-256 cc_num avant publication (RGPD)                       │
   │  Tests qualite Great Expectations (notebook hebdo)                  │
   │  Lineage YAML versionne Git                                         │
   │                                                                     │
   └─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Choix d'architecture justifiés

### 4.1 Kappa (pipeline 100 % streaming) avec couche de prédiction unique

L'architecture suit le **modèle Kappa** défini par Jay Kreps en 2014 : un seul pipeline streaming gère l'intégralité du traitement, sans batch layer indépendant. Toutes les transactions sont ingérées en continu via Kafka, validées, scorées par le modèle, puis stockées dans PostgreSQL. Le rapport quotidien est une simple requête SQL agrégée sur la table de service, pas un batch recompute indépendant.

**Pourquoi Kappa et pas Lambda ?**

| Critère | Lambda | **Kappa (choix)** |
|---|---|---|
| Pipelines à maintenir | 2 (batch + speed) | 1 seul |
| Code dupliqué (batch/streaming) | Oui | Non |
| Risque de divergence entre couches | Élevé | Nul (source unique) |
| Recomputation historique | Via batch layer | Via replay Kafka (offset 0) |
| Complexité opérationnelle | Élevée | Faible |
| Volume cible | Massif (Po) | Modéré à massif |

Pour ce projet (555 k transactions, débit modéré, équipe d'un développeur), **Kappa est plus pragmatique**. Le replay Kafka couvre les besoins de recomputation sans nécessiter un Spark batch parallèle.

### 4.2 Kafka comme bus de messages

| Bénéfice | Raison technique |
|---|---|
| Découplage producer / consumer | Un consumer peut crasher sans bloquer le producer |
| Persistance | Les messages restent dans le topic même si aucun consumer n'est branché |
| Multi-consumer | Sur le topic `fraud-predictions`, on a 3 consumers en parallèle (mail, Postgres, Streamlit) sans modifier le producer |
| DLQ et retry natifs | Topic dédié `dlq-raw` pour les messages invalides |
| Standard industrie | Compétence transférable pour le candidat, attendu par les jurys RNCP |

### 4.3 PostgreSQL pour le stockage Gold

| Bénéfice | Raison |
|---|---|
| Requêtable en SQL | Le rapport quotidien est un simple `SELECT WHERE date = yesterday` |
| Robuste, transactionnel | Aucune perte de prédiction même en cas de crash consumer |
| Connecté nativement à Streamlit | Via SQLAlchemy |
| Limite suffisante | Quelques millions de transactions par mois sans souci |

### 4.4 MLflow + MinIO pour le cycle de vie du modèle

| Composant | Rôle |
|---|---|
| MLflow | Tracking des expériences (paramètres, métriques), registry des modèles |
| MinIO | Stockage S3-compatible auto-hébergé pour les artefacts (.pkl) |

Le `consumer_predictor` charge le modèle dynamiquement par alias (`models:/fraud-detector@production`), ce qui permet de déployer une nouvelle version sans redéployer le consumer.

### 4.5 Streamlit pour le dashboard

Choix pragmatique : Python natif, déploiement en une commande, dashboard interactif avec graphes. Lit deux sources :
- **Live buffer** : les N derniers messages du topic `fraud-predictions` (via consumer Kafka intégré)
- **Historique** : requête sur `payments_gold` pour les statistiques de la veille / semaine

### 4.6 Docker Compose pour l'orchestration

Tous les composants tournent dans des conteneurs orchestrés par `docker-compose up`. Avantages : reproductibilité, isolation, démo en une commande.

---

## 5. Description détaillée des composants

### 5.1 Phase préparatoire

**`notebook_eda_baseline.ipynb`**
- EDA : profil du déséquilibre (0,39 % fraudes), distributions, corrélations
- Feature engineering : `age`, `hour`, `day_of_week`, `is_night`, `distance_km`, log-transform de `amt`
- Pipeline scikit-learn unique (FeatureBuilder + ColumnTransformer + RandomForestClassifier)
- Évaluation : Precision-Recall AUC (métrique adaptée au déséquilibre), F1, Recall@95% Precision, matrice de confusion
- Export `model.pkl` via `joblib.dump`
- Logging dans MLflow

### 5.2 Source temps réel

**`fake_api.py`**
- Charge `fraudTest.csv` en mémoire (auto-téléchargement si absent)
- Expose `/current-transactions` qui renvoie n transactions au format Pandas split
- Retire la colonne `is_fraud` pour respecter le contrat d'une vraie API
- Endpoint `/health` pour le monitoring

### 5.3 Producer Kafka

**`producer.py`**
- Poll `fake_api.py` toutes les 12 secondes (paramétrable)
- Valide le schéma reçu via Pydantic (rejette les champs manquants ou typés incorrectement)
- **Pseudonymise `cc_num`** par hash SHA-256 (RGPD)
- Publie dans le topic `raw-transactions` (clé = `trans_num` pour ordre garanti par client)
- En cas d'invalidité : publie dans `dlq-raw` avec le motif d'erreur
- Métriques exposées : nb messages publiés, nb rejets DLQ

### 5.4 Consumer prédiction

**`consumer_predictor.py`**
- S'abonne au topic `raw-transactions` dans le groupe `fraud-consumer-group`
- Au démarrage : charge le modèle depuis MLflow (alias `production`)
- Pour chaque message : applique feature engineering identique à l'entraînement (Pipeline scikit-learn) puis prédit
- Publie le message enrichi (`is_fraud`, `score`) dans `fraud-predictions`
- Idempotent : reprend après crash sans rejouer les messages déjà traités (offset commits)

### 5.5 Consumer storage

**`consumer_storage.py`**
- Consomme `fraud-predictions`
- Insère chaque message dans `payments_gold` (PostgreSQL)
- Upsert sur `trans_num` pour idempotence

### 5.6 Consumer alerter

**`consumer_alerter.py`**
- Consomme `fraud-predictions`
- Filtre `is_fraud == 1`
- Envoie un mail via SMTP (ou webhook Slack en alternative)
- Rate limiting : maximum 1 mail par minute pour éviter le spam en cas d'incident

### 5.7 Dashboard

**`dashboard.py` (Streamlit)**
- Vue temps réel : 100 dernières transactions (live buffer)
- Vue historique : statistiques agrégées sur Postgres
- Graphes : taux de fraude par heure, par catégorie de marchand, par état US
- Filtres interactifs

### 5.8 Rapport quotidien

**`daily_report.py` (cron)**
- Tourne tous les jours à 6h
- Requête `SELECT * FROM payments_gold WHERE date(created_at) = current_date - interval '1 day'`
- Génère un rapport JSON + PDF
- Envoi par mail à l'équipe

---

## 6. Stratégie de qualité des données

### 6.1 Validation à l'ingestion (compétence C5)

Schéma Pydantic strict dans le producer :

```python
class PaymentSchema(BaseModel):
    trans_date_trans_time: datetime
    cc_num: str = Field(pattern=r"^\d{12,19}$")
    amt: float = Field(ge=0, le=50_000)
    category: str
    gender: Literal["M", "F"]
    lat: float = Field(ge=-90, le=90)
    long: float = Field(ge=-180, le=180)
    # ... autres champs
```

Tout message non conforme part dans **`dlq-raw`** avec :
- Le payload original
- L'erreur de validation
- Un timestamp

### 6.2 Tests de qualité périodiques

**`quality_checks.py`** (cron hebdo) avec **Great Expectations** :
- `expect_column_values_to_not_be_null` sur les features critiques
- `expect_column_mean_to_be_between` sur `amt` (alerte si dérive)
- `expect_column_proportion_of_unique_values_to_be_between` sur `cc_num`

### 6.3 Reconciliation

Job quotidien : compare `count(producer)` vs `count(payments_gold)` vs `count(dlq-raw)`. L'écart doit être 0.

---

## 7. Stratégie de gouvernance et sécurité

### 7.1 RGPD (compétence C4)

- **Pseudonymisation** : `cc_num` haché en SHA-256 dans le producer **avant** publication Kafka
- **Exclusion de l'inférence** : `first`, `last`, `street`, `dob` ne sont pas utilisés comme features
- **Droit à l'oubli** : la table `payments_gold` ne contient que des `cc_num_hash`, pas de PII identifiantes
- **Chiffrement** : TLS sur tous les flux Kafka (config production)

### 7.2 Lineage et catalog

Fichier `lineage.yaml` versionné Git documente :
- Chaque dataset (source, propriétaire, fréquence)
- Chaque transformation (logique, code source)
- Chaque consommateur

Suffisant pour ce projet ; à industrialiser via DataHub si scale.

### 7.3 Secrets

Aucun credential en clair. Variables d'environnement via `.env` (gitignored) chargées par Docker Compose.

---

## 8. Stratégie de monitoring (compétence C4)

| Quoi | Outil | Métrique |
|---|---|---|
| Lag des consumers | **Kafka UI** (Provectus) | Nb messages non traités par consumer group |
| Débit du producer | Logs producer + Kafka UI | Messages/sec |
| Taux de fraude observé | Streamlit | % is_fraud=1 sur 1h, 24h |
| Latence end-to-end | Logs avec timestamps | p50, p95, p99 |
| Santé conteneurs | `docker-compose ps` + healthchecks | up/down |
| Data drift | Notebook hebdo | comparaison distribution `amt`, `category` API vs CSV ref |

---

## 9. Mapping aux 5 compétences du Bloc 3

| Compétence RNCP | Composants couvrants |
|---|---|
| **C1** — Système temps réel adapté à la vélocité, au volume, à la typologie | Kafka topics, producer cron 12s, consumer prediction streaming, latence < 5s |
| **C2** — Pipeline ETL/ELT entre bases | Producer (Extract API) → Kafka (Transport) → Consumer (Transform : features + predict) → Postgres (Load) |
| **C3** — Automatisation et performance | Cron polling, consumers en boucle, idempotence offsets Kafka, Docker Compose pour reproductibilité, MLflow registry pour swap de modèle sans redeploy |
| **C4** — Monitoring et gouvernance | Kafka UI, dashboard Streamlit, hash cc_num (RGPD), lineage YAML, exclusion PII des features |
| **C5** — Qualité et correction d'erreurs | Validation Pydantic, DLQ topic dédié, tests Great Expectations, reconciliation quotidienne, retries Kafka natifs |

---

## 10. Fichiers livrés

| Fichier | Rôle |
|---|---|
| `architecture.md` | Ce document |
| `fake_api.py` | Simulateur de l'API temps réel (replay CSV) |
| `inspect_api.py` | Inspecteur de schéma API ↔ CSV |
| `notebook_eda_baseline.ipynb` | EDA + entraînement modèle baseline |
| `producer.py` | Producer Kafka |
| `consumer_predictor.py` | Consumer prédiction |
| `consumer_storage.py` | Consumer persistance Postgres |
| `consumer_alerter.py` | Consumer alerte mail |
| `dashboard.py` | Dashboard Streamlit |
| `daily_report.py` | Rapport quotidien batch |
| `schemas.py` | Schémas Pydantic partagés |
| `docker-compose.yml` | Orchestration de la stack complète |
| `requirements.txt` | Dépendances Python |
| `README.md` | Mode d'emploi et démarrage |

---

## 11. Réponse au cahier des charges Jedha

| Élément demandé | Livrable |
|---|---|
| Schéma de l'infrastructure | Section 3 + 4 de ce document |
| Code source | Tous les `.py` et `.ipynb` listés en section 10 |
| Vidéo de démonstration | À enregistrer (Vidyard) avec `docker-compose up` puis dashboard Streamlit |
| Élément collectant et stockant des données | producer + Postgres |
| Élément consommant des données | dashboard Streamlit + alerter |
| Processus ETL | producer → kafka → consumer (transform + predict) → Postgres |

---

## 12. Limites assumées et perspectives d'évolution

Cette section anticipe les questions du jury sur les limites du périmètre actuel et présente les évolutions naturelles vers une architecture **production-grade**.

### 12.1 Limites assumées (proportionnées au scope académique)

| Domaine | État actuel | Justification |
|---|---|---|
| Cluster Kafka | 1 broker, 1 partition par topic | Suffisant pour démontrer le pattern. À scaler horizontalement en prod. |
| Schémas Kafka | JSON (validation Pydantic côté producer) | Avro + Schema Registry serait plus rigoureux mais hors scope du Bloc 3. |
| Sécurité Kafka | PLAINTEXT en dev local | TLS + SASL en prod. |
| RGPD | Hash SHA-256 cc_num + exclusion PII des features | Pseudonymisation réversible (chiffrement déterministe) requise pour GDPR strict en banque. |
| Postgres | 1 instance, sans partitionnement | Suffisant pour 555 k transactions ; partitionnement par mois nécessaire au-delà. |
| Monitoring technique | Kafka UI + logs Docker | Prometheus + Grafana + Loki en prod. |
| Détection de drift | Notebook hebdomadaire ad-hoc | Evidently AI en CI pour automatiser. |

### 12.2 Perspectives d'évolution prioritaires

**Court terme (1-2 sprints)** :
1. **Avro + Schema Registry** sur Kafka — garantit la compatibilité ascendante des schémas
2. **Prometheus + Grafana** — métriques système (CPU, RAM, lag consumer) avec alerting
3. **Tests Great Expectations en CI** — automatisation du contrôle qualité

**Moyen terme (1 trimestre)** :
4. **Detection de drift automatisée** — Evidently AI compare distributions API vs CSV référence
5. **Réentraînement automatique** — pipeline MLOps déclenché par seuil de drift ou périodique
6. **Feature store** — table d'agrégats utilisateur en streaming (nb tx 24h, montant moyen)

**Long terme** :
7. **Modèle ensembliste** — RandomForest supervisé + Isolation Forest non-supervisé pour fraudes nouvelles
8. **Sécurité production** — TLS Kafka, RBAC Postgres, secrets dans HashiCorp Vault
9. **Migration vers cluster Kafka multi-brokers** + partitionnement par `trans_num` hash

---

*Document à intégrer dans le dossier de projet RNCP38777 Bloc 3.*
