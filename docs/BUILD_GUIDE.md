# Build guide — Real-time retail analytics platform on Microsoft Fabric

Work through the phases in order; each one ends with a **✅ Done when** check. Don't move on until
it passes — debugging five layers at once is miserable.

**Realistic time:** 3–5 weeks part-time for a first-timer (the 8–9 days in the original plan assumes
you already know Fabric). The Fabric trial lasts **60 days**, so start the trial only when Phase 0's
local setup is finished, and screenshot everything as you go.

---

## Phase 0 — Accounts and local setup (day 1–2)

### 0.1 Fabric trial with a Gmail address
Fabric doesn't accept personal emails directly. The documented route:
1. Create an **Azure free account** at azure.microsoft.com with your Gmail (a card is needed for
   identity verification; you aren't charged unless you upgrade). This also gives you Blob Storage.
2. Azure portal → **Microsoft Entra ID → Users → New user** → e.g. `aditya@<yourtenant>.onmicrosoft.com`.
3. Sign in to **app.fabric.microsoft.com** as that user → profile icon → **Free trial** → pick a
   region close to you (e.g. Central India) → Activate.

Use this Entra user for *everything* in Fabric (connections, Outlook alerts, API sign-in).

Trial limits to know: 60 days; Copilot, data agents and AI functions are **not** included.

### 0.2 Install on your laptop (Windows)
| Tool | Why |
|---|---|
| Python 3.11 + `pip install -r data_generator/requirements.txt` | generators, producer, loaders |
| SQL Server 2022 **Developer** edition + SSMS | the OLTP source (free) |
| Microsoft **ODBC Driver 18** for SQL Server | Python → SQL Server |
| **On-premises data gateway** (standard mode) | lets Fabric reach your local SQL Server |
| Node.js 18+ | API client (Phase 11) |
| Git + a GitHub repo | Fabric Git integration + your portfolio |

### 0.3 Generate the data
```bash
cd data_generator
python generate_data.py
```
✅ **Done when** `output/` has `sql/`, `blob/`, `salesforce/` folders (≈5k customers, ≈58k orders,
≈150k order lines, 30 inventory files, ≈10k reviews, ≈4k support cases).

Optional but recommended: run the local test (see README) so you've seen every notebook work before
touching the cloud.

---

## Phase 1 — Workspace and lakehouse (30 min)
1. Fabric → **Workspaces → New workspace** `retail-analytics-dev`, licence mode **Trial**.
2. **New item → Lakehouse** `lh_retail` (leave "schemas" off — table names use `bronze_/silver_/gold_` prefixes).
3. **New item → Environment** `env_retail` → Public libraries → add `great_expectations` (pin a
   1.x version, e.g. `1.3.*` or newer) → **Publish** (takes a few minutes).
4. Import notebooks: **Import → Notebook → From this computer** → select all `notebooks/*.ipynb`
   (build them first with `python tools/build_notebooks.py`).
5. Open each notebook → attach **lh_retail** as default lakehouse. On `nb_04_data_quality`, also
   select environment `env_retail`.

✅ **Done when** `nb_00_common` runs and prints `nb_00_common loaded`.

---

## Phase 2 — SQL Server + Mirroring (CDC) (day 3–4)

### 2.1 Load the OLTP database
1. SSMS: run `sql/01_create_source_db.sql`.
2. `python load_sql_server.py` (add `--server "localhost\\INSTANCE"` if you used a named instance).
3. Enable **mixed authentication** (Server properties → Security), restart SQL Server, make sure the
   **SQL Server Agent** service is running and set to Automatic.
4. Run `sql/02_mirroring_login.sql` (change the password!).

### 2.2 Gateway
Install the **on-premises data gateway** on the same laptop, sign in with your Fabric user, register
a new gateway `laptop-gw`. Your laptop must be on and online for mirroring to replicate.

### 2.3 Mirror the database
Workspace → **New item → Mirrored SQL Server database** → new connection:
server `localhost` (or `YOURPC\INSTANCE`), database `RetailOLTP`, gateway `laptop-gw`,
SQL auth `fabric_login`. Select all 5 tables → **Mirror database**.
Wait for status *Running* and row counts to match.

### 2.4 Expose mirrored tables in the lakehouse
`lh_retail` → Tables → **New shortcut → Microsoft OneLake** → pick the mirrored database → select the
5 tables. Rename the shortcuts to `src_customers`, `src_products`, `src_stores`, `src_orders`,
`src_order_items` (names used in `nb_00_common.TBL`).

✅ **Done when** `SELECT COUNT(*) FROM src_orders` in a notebook matches SQL Server, and after running
`python simulate_sql_changes.py` the new orders appear in `src_orders` within a minute or two.
(That's CDC-based replication — write it down, it's a great interview story.)

---

## Phase 3 — Batch sources: Blob, REST API, Salesforce (day 5–6)

### 3.1 Blob Storage
Azure portal → **Storage account** (Standard LRS, cheapest region) → copy the connection string into
`data_generator/.env` → `python upload_to_blob.py`.

### 3.2 Salesforce (optional but resume-worthy)
1. Sign up for a free **Salesforce Developer Edition** org.
2. Create the 5 custom Case fields listed at the top of `load_salesforce.py`.
3. Put credentials + security token in `.env` → `python load_salesforce.py`.

No Salesforce? Use `crm/salesforce_case_export.csv` from Blob — same columns, same code path.
Be honest about which one you did on your resume.

### 3.3 Copy activities
Create pipeline `pl_retail_daily` and add the four Copy activities exactly as in
[`pipelines/pl_retail_daily.md`](../pipelines/pl_retail_daily.md) (FX from REST, inventory and reviews
from Blob, cases from Salesforce/Blob). For the **first run** set `fx_lookback_days = 600` so FX covers
all order history. Run just these activities (select them → Run).

✅ **Done when** `Files/landing/{fx,inventory,reviews,crm}` all contain files.

---

## Phase 4 — Streaming: Eventstream + Kafka producer (day 7)
1. **New item → Eventstream** `es_pos_events` → **Add source → Custom endpoint** → publish.
2. Select the source → **Details → Kafka → SAS Key Authentication** → copy bootstrap server, topic,
   connection string into `.env`.
3. Add destination **Lakehouse** → `lh_retail`, table `bronze_pos_events`, input format **JSON** → publish.
4. `python pos_stream_producer.py --rate 3 --minutes 5`

✅ **Done when** the Eventstream shows incoming events and `bronze_pos_events` has rows.

Talking point: the producer uses the standard **Kafka protocol** (`confluent-kafka`), so the same
code would work against Confluent Cloud or Azure Event Hubs — Eventstream is the Kafka-compatible
broker here. Say it that way; don't claim you ran a Kafka cluster.

---

## Phase 5 — Bronze & Silver notebooks (day 8–10)
Run in order, reading every markdown cell:
1. `nb_01_bronze_ingest` — landing files → bronze tables (incremental via checkpoints).
2. `nb_02_silver_dimensions` — first run loads 5,000 customers as version 1.
3. `nb_03_silver_facts` — order lines, POS stream, FX gap-fill, inventory, reviews, cases.

**See SCD2 work:**
```bash
python simulate_sql_changes.py --customers 40 --products 10
```
wait ~2 minutes for mirroring, re-run `nb_02` and look at the last cell: those customers now have
two rows (`is_current` false/true, `valid_to` = change time).

✅ **Done when** re-running `nb_01`/`nb_03` with no new data leaves row counts unchanged
(idempotency), and `silver_quarantine` holds the injected bad reviews and negative stock rows.

---

## Phase 6 — Data-quality gate (day 11)
Run `nb_04_data_quality`. Then break something on purpose, e.g. in a scratch cell:
```python
spark.sql("INSERT INTO silver_review SELECT * FROM silver_review LIMIT 3")
```
re-run `nb_04` → it must **fail** with "Critical data-quality checks failed". Fix it
(`DELETE` the duplicates or re-run `nb_03`), re-run, pass.

✅ **Done when** `ops_dq_results` has two runs: one failed, one passed.

---

## Phase 7 — Gold star schema (day 12–13)
Run `nb_05_gold_star_schema`. Then check in the SQL analytics endpoint (`sql/03_sql_endpoint_views.sql`
has ready queries):
* no online sales with `customer_sk = -1`;
* a customer you changed in Phase 5 has old sales on the old version and new sales on the new one;
* no `fx_inr_per_unit` nulls.

Create the four views from `sql/03_sql_endpoint_views.sql`.

---

## Phase 8 — Churn model with MLflow (day 14–16)
1. `nb_07_churn_train` — trains logistic regression and gradient boosting, validates **out of time**,
   registers the winner as `retail_churn_model`. (`nb_06_churn_features` is pulled in via `%run`.)
2. Open the experiment `retail-churn-experiment` → compare runs; open the model → version 1.
3. `nb_08_churn_score` → `gold_churn_scores`.

On this synthetic data expect ROC-AUC around 0.8, driven mostly by recency. Report numbers as
"on synthetic data" — and be ready to explain why recency dominates.

---

## Phase 9 — Orchestration + alerts (day 17–18)
Finish `pl_retail_daily` and `pl_retail_master` per the pipeline spec: notebook activities with
retries, the DQ gate with no retry, weekly retrain branch, wrapper pipeline with failure email,
daily schedule. Test the failure path by making `nb_04` fail (Phase 6 trick).

✅ **Done when** you have an alert email from a failed run and a green run in the Monitor hub.

---

## Phase 10 — Power BI (day 19–21)
Follow [`powerbi/semantic_model.md`](../powerbi/semantic_model.md) and [`measures.dax`](../powerbi/measures.dax).

## Phase 11 — Activator alerts (day 22)
Follow [`activator/activator_rules.md`](../activator/activator_rules.md); trigger with
`--anomaly return_spike --store S003`.

## Phase 12 — API for GraphQL (day 23)
Follow [`api/README.md`](../api/README.md).

## Phase 13 — Make it portfolio-ready (day 24–25)
* Workspace settings → **Git integration** → connect to your GitHub repo → commit.
* README: architecture diagram, 6–8 screenshots (mirroring status, Eventstream canvas, SCD2 rows,
  DQ failure + alert email, MLflow comparison, pipeline run, dashboard, Activator alert).
* A 3–4 minute Loom/YouTube walkthrough beats any bullet point.
* **Clean up** before the trial ends: pause schedules, stop Eventstream, delete the Azure storage
  account (the only thing that can cost money).

---

## Troubleshooting
| Symptom | Likely cause / fix |
|---|---|
| Mirroring stuck / CDC error | SQL Server Agent not running; login lacks permissions (see `02_mirroring_login.sql` note); tables without PK |
| Mirroring stops overnight | laptop asleep → gateway offline. It resumes when the laptop is back |
| `bronze_pos_events` missing columns | Eventstream input format must be **JSON**; check the destination's schema preview |
| `nb_03` FX warning "no FX rate" | FX backfill too short → re-run the FX copy with `fx_lookback_days = 600` |
| `ModuleNotFoundError: great_expectations` | `env_retail` not published or not attached to `nb_04` |
| Spark session slow to start | normal on trial capacity (1–3 min); run notebooks sequentially, not in parallel |
| Capacity throttling | stop the Eventstream and producer when idle; check the Capacity Metrics app |
