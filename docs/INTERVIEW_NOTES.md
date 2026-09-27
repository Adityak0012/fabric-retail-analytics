# Interview notes — what to say, and what you'll be asked

## Resume bullet (honest version)

The original bullet ("Architected a production-grade…") will get you probed hard — "production"
implies real users, SLAs and on-call, and an interviewer at a services firm will ask about all
three. This version is still strong and survives follow-up questions:

> **Real-time retail analytics platform (Microsoft Fabric, personal project)** — Built an end-to-end
> medallion lakehouse ingesting 5 source types: SQL Server via Mirroring (CDC), Kafka-protocol POS
> events via Eventstream, a REST FX API, Azure Blob files and Salesforce. Implemented SCD Type 2
> dimensions with Delta MERGE, a star schema with point-in-time joins, a Great Expectations quality
> gate (26 checks) that blocks bad data from Gold, and a churn model tracked in MLflow (ROC-AUC 0.84,
> out-of-time validation, synthetic data). Orchestrated with Fabric pipelines (retries, failure
> alerts), served via Direct Lake Power BI, GraphQL API and Activator real-time alerts.

Shorter two-line version for a one-page resume:
> Built a Microsoft Fabric lakehouse (Bronze/Silver/Gold) ingesting 5 sources incl. SQL Server CDC
> mirroring and Kafka streaming; SCD2 dimensions, star schema, Great Expectations DQ gate, MLflow
> churn model (AUC 0.84), orchestrated pipelines with alerting and Power BI/Activator serving.

Update the numbers with *your* run's results. Only list what you actually built.

## Concepts you must be able to explain (with this project as the example)

**Medallion architecture.** Bronze = raw, append-only, all strings, lineage columns
(`_source_file`, `_ingested_at`); Silver = typed, deduplicated, conformed, history (SCD2), bad rows
quarantined; Gold = business-shaped star schema for BI/ML. Why: reprocess from Bronze if logic
changes; a bad file never breaks ingestion.

**SCD types.** Type 1 overwrites (stores), Type 2 keeps history with `valid_from/valid_to/is_current`
(customers, products). Our SCD2 is a single atomic MERGE using the *staged union* trick: changed rows
appear twice — once keyed (to close the old version) and once with a NULL key (to insert the new one).
Follow-up: "What about two changes between runs?" → snapshot comparison collapses them; reading the
CDC change feed would capture each one.

**Surrogate keys & unknown member.** Facts join on `*_sk`, not business keys, because one customer
has many versions. `-1` "Unknown" rows mean facts never get dropped by an inner join.

**Point-in-time join.** `txn_ts >= valid_from AND txn_ts < valid_to` → a sale is attributed to the
customer's city *at the time*. Why `9999-12-31` instead of NULL for open rows: simpler BETWEEN logic.

**Idempotency.** Every step can be re-run safely: MERGE on keys, streaming checkpoints,
`replaceWhere` for daily score partitions, insert-only merge for quarantine. That's what makes
pipeline retries safe. (The local test proves it: re-runs don't change row counts.)

**Incremental loading.** Watermark on `updated_at` for orders (with `>=` + MERGE so boundary rows
aren't lost); `availableNow` streaming for files (Spark tracks processed files for you).

**Exactly-once streaming into Silver.** Structured Streaming + `foreachBatch` + MERGE on `event_id`.
The producer deliberately sends duplicates; Silver has none.

**Mirroring vs Copy.** Mirroring continuously replicates via CDC into Delta in OneLake with no
pipeline code; Copy activity is scheduled batch. Trade-off: mirroring needs the gateway/source online
and supports specific sources and data types.

**Data quality gate.** Critical vs warning severity; critical failure raises → pipeline stops
before Gold → alert. Results stored per run → trend in Power BI. Row-level rules (negative stock)
quarantine rows; table-level GX checks decide pass/fail.

**ML without leakage.** Features use data ≤ T, label uses (T, T+90d]. Out-of-time validation
(train on T−180, validate on T−90) instead of a random split. Baseline logistic regression vs
gradient boosting — the simpler model won on PR-AUC, and saying so shows judgement.
Metric choice: PR-AUC and precision@top-10% because retention teams can only call the top slice.

**Direct Lake.** Power BI reads Delta/Parquet from OneLake straight into VertiPaq — no import
refresh, near-import speed. OPTIMIZE/V-Order keeps files healthy.

**Orchestration.** Retries only where failures are transient; no retry on the DQ gate; wrapper
pipeline for alerting because multiple failure dependencies are AND-ed.

**Cost awareness.** Trial capacity is shared by everything; streaming runs continuously, so pause
it when idle. Retrain weekly, score daily.

## Likely questions — practise out loud
1. Walk me through what happens when a customer changes their city.
2. How do you guarantee the pipeline doesn't double-count if a notebook is retried?
3. What happens if the FX API is down for two days?
4. How would you scale this to 100 million order lines? (partitioning by date, incremental Gold
   via MERGE instead of overwrite, Z-order/liquid clustering, CDF for incremental Silver)
5. Why a star schema instead of one wide table?
6. How did you choose the churn definition and window? What would the business do with the score?
7. What would you change to make this truly production-ready? (dev/test/prod workspaces with
   deployment pipelines, service principal instead of a user, secrets in Key Vault, CI running the
   local test, monitoring SLAs, PII masking/row-level security)

Question 7 is your chance to show you know the gap between a portfolio project and production —
that's exactly what makes an interviewer trust the rest of your answers.
