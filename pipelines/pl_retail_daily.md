# Pipeline `pl_retail_daily` — orchestration spec

Build this in **Fabric > workspace > New item > Data pipeline**. Fabric pipelines are built in the
UI, so this file is the exact spec to follow (and to screenshot for your README). Once built, turn
on **Git integration** for the workspace so the pipeline definition is versioned in your repo.

## Flow

```
┌──────────────── ingest (run in parallel) ─────────────────┐
│ cp_fx_rates_rest      Copy  REST (Frankfurter)  → Files/landing/fx/         │
│ cp_inventory_blob     Copy  Blob inventory/*.csv → Files/landing/inventory/ │
│ cp_reviews_blob       Copy  Blob reviews/*.jsonl → Files/landing/reviews/   │
│ cp_cases_salesforce   Copy  Salesforce Case     → Files/landing/crm/        │
└───────────────────────────────┬──────────────────────────────┘
                                │ all succeeded
                     nb_bronze  (nb_01_bronze_ingest)
                                │
                     nb_silver_dims (nb_02) ──► nb_silver_facts (nb_03)
                                │
                     nb_dq_gate (nb_04)  ── fails on critical DQ ──┐
                                │                                   │
                     nb_gold (nb_05)                                │
                                │                                   │
                     if_weekly ─► nb_churn_train (nb_07)            │
                                │                                   │
                     nb_churn_score (nb_08)                         │
                                │                                   │
                     semantic_model_refresh (optional)              │
                                                                    ▼
   pl_retail_master: Invoke pl_retail_daily ── On fail ──► alert email ──► Fail activity
```

## Pipeline parameters

| Name | Type | Default | Used by |
|---|---|---|---|
| `fx_lookback_days` | int | `7` | FX copy relative URL (set to `600` for the first backfill run) |
| `train_model` | bool | `false` | force a retrain outside the weekly schedule |

## Activities

### 1. `cp_fx_rates_rest` — Copy data (REST → Lakehouse Files)
* **Source**: connection type **REST**, base URL `https://api.frankfurter.dev/`, anonymous auth.
* **Relative URL** (Add dynamic content):
  ```
  @concat('v2/rates?from=', formatDateTime(addDays(utcNow(), mul(-1, pipeline().parameters.fx_lookback_days)), 'yyyy-MM-dd'), '&base=INR&quotes=USD,GBP,EUR,SGD')
  ```
* **Destination**: Lakehouse `lh_retail` → Files → folder `landing/fx`, file name
  `@concat('fx_rates_', formatDateTime(utcNow(), 'yyyyMMddHHmmss'), '.json')`, format **JSON**.
* Overlapping windows are fine: Silver de-duplicates on (date, currency).

### 2. `cp_inventory_blob` / `cp_reviews_blob` — Copy data (Blob → Files)
* **Source**: Azure Blob Storage connection (account key or SAS), container `retail-landing`,
  folder `inventory/` (and `reviews/`), wildcard `*`, **Binary** format (copy files as-is).
* **Filter by last modified**: start = `@addHours(utcNow(), -26)` so each run only copies new files
  (skip this on the very first run).
* **Destination**: `lh_retail` Files → `landing/inventory` (and `landing/reviews`), Binary, preserve hierarchy.

### 3. `cp_cases_salesforce` — Copy data (Salesforce → Files)
* **Source**: **Salesforce** connection (OAuth sign-in to your Developer Edition org). Use query:
  ```sql
  SELECT Legacy_Case_Id__c, Legacy_Customer_Id__c, SuppliedEmail, Subject, Description,
         Status, Priority, Origin, Reason, Opened_At__c, Resolved_At__c, CSAT_Score__c
  FROM Case WHERE SystemModstamp >= LAST_N_DAYS:2
  ```
  (First run: remove the `WHERE` clause to backfill.)
* **Destination**: Files → `landing/crm`, **DelimitedText** with header,
  file name `@concat('cases_', formatDateTime(utcNow(), 'yyyyMMddHHmmss'), '.csv')`.
* **Skipping Salesforce?** Copy `crm/salesforce_case_export.csv` from Blob instead (same columns).

### 4–9. Notebook activities
All notebook activities: **Settings → Base parameters** `pipeline_run_id` = `@pipeline().RunId`.

| Activity | Notebook | Timeout | Retry | Retry interval | Depends on |
|---|---|---|---|---|---|
| `nb_bronze` | nb_01_bronze_ingest | 30 min | 2 | 120 s | all 4 copies **Succeeded** |
| `nb_silver_dims` | nb_02_silver_dimensions | 30 min | 2 | 120 s | nb_bronze Succeeded |
| `nb_silver_facts` | nb_03_silver_facts | 45 min | 2 | 120 s | nb_silver_dims Succeeded |
| `nb_dq_gate` | nb_04_data_quality | 20 min | **0** | – | nb_silver_facts Succeeded |
| `nb_gold` | nb_05_gold_star_schema | 45 min | 2 | 120 s | nb_dq_gate Succeeded |
| `nb_churn_train` | nb_07_churn_train | 60 min | 1 | 300 s | inside `if_weekly` |
| `nb_churn_score` | nb_08_churn_score | 20 min | 2 | 120 s | if_weekly Completed |

Why these choices (interview talking points):
* **Retries on transient steps** (Spark session start-up, throttling, network) — every notebook is
  idempotent (MERGE / checkpointed streams / `replaceWhere`), so a retry can never double-load data.
* **No retry on the DQ gate** — a failed quality check is a *data* problem; retrying won't fix it.
* Copies set to **Retry 3, interval 60 s**: external APIs fail transiently more than Spark does.

### 10. `if_weekly` — If condition
Expression: `@or(equals(dayOfWeek(utcNow()), 1), pipeline().parameters.train_model)`
True branch: `nb_churn_train`. (Retraining daily on slowly changing data wastes capacity units.)

### 11. `semantic_model_refresh` — optional
Direct Lake models pick up new Delta data automatically. Add a **Semantic model refresh** activity
only if you switch to Import mode or want to force framing right after Gold.

### 12. Failure alerting — wrapper pipeline (the reliable pattern)
**Gotcha worth knowing:** if you wire several activities' **On fail** handles into one email
activity, Fabric (like Azure Data Factory) treats multiple dependencies as **AND** — the email would
only fire if *all* of them failed. Two patterns that work:

**A. Wrapper pipeline (recommended).** Create `pl_retail_master` with:
1. `run_daily` — **Invoke pipeline** activity → `pl_retail_daily`, *Wait on completion* = on.
2. `alert_on_failure` — **Office 365 Outlook** activity connected to `run_daily`'s **On fail**.
   * To: your email · Subject: `@concat('[FAILED] pl_retail_daily — run ', pipeline().RunId)`
   * Body (HTML):
     ```
     @concat('<p>Pipeline <b>pl_retail_daily</b> failed.</p>',
             '<p>Master run id: ', pipeline().RunId, '<br/>Triggered: ', pipeline().TriggerTime, '</p>',
             '<p>Check the Monitor hub, then <code>ops_run_log</code> and <code>ops_dq_results</code>.</p>')
     ```
3. `fail_run` — **Fail** activity after `alert_on_failure`, so the master run still shows as failed
   (otherwise a handled failure looks green in the Monitor hub).

Schedule the **master** pipeline, not the child.

**B. One alert per activity.** Give each critical activity its own small Outlook activity on
**On fail**. Simple, but noisy to maintain.

(Bonus: Fabric's Real-Time hub also exposes *job events* for workspace items; an Activator rule on
"pipeline run failed" is a third option once you're comfortable with Activator.)

## Schedule
`pl_retail_master` → **Schedule**: daily at 06:30 (Asia/Kolkata). Keep the Eventstream running separately
(it's real-time and doesn't need the pipeline). **Pause the schedule and stop the Eventstream when
you're not demoing** to save trial capacity.

## Monitoring
* **Monitor hub**: every run, duration, and activity-level errors.
* `ops_run_log`: per-table row counts and metrics written by the notebooks.
* `ops_dq_results`: every GX check result per run → Power BI "Data quality" page.
