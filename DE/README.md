# SMS Databricks Kafka Pipeline — End-to-End Reference Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  Subscription Management System  •  Databricks Medallion Architecture       ║
║  Kafka → Bronze → Silver (DLT) → Gold → Delta Sharing                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

## Repository Structure

```
sms_pipeline/
├── config.py                        # All environment & pipeline configuration
├── 01_bronze_kafka_ingestion.py     # Layer 1: Raw Kafka → Bronze Delta tables
├── 02_silver_dlt_pipeline.py        # Layer 2: DLT expectations, cleansing, joins
├── 03_gold_aggregations.py          # Layer 3: Business aggregations → Gold
├── 04_governance_access_control.py  # Unity Catalog tags, masks, grants, Delta Sharing
├── 05_record_simulator.py           # Test data generator (Kafka + file modes)
└── README.md                        # This file
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ SOURCE SYSTEMS                                                                │
│  Subscription Mgmt System → Kafka Topics (6 entities)                        │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ Kafka Structured Streaming
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER  (01_bronze_kafka_ingestion.py)                                 │
│                                                                              │
│  ┌──────────┐ ┌──────┐ ┌─────────┐ ┌───────┐ ┌────────┐ ┌─────────┐       │
│  │sub_raw   │ │user_ │ │catalog_ │ │order_ │ │device_ │ │payment_ │       │
│  │          │ │raw   │ │raw      │ │raw    │ │raw     │ │raw      │       │
│  └──────────┘ └──────┘ └─────────┘ └───────┘ └────────┘ └─────────┘       │
│  • append-only  • raw JSON preserved  • Kafka metadata  • DLQ tables         │
│  • CDF enabled  • row_hash for dedup  • _ingested_at audit column            │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ Delta CDF (readChangeFeed)
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ SILVER LAYER  (02_silver_dlt_pipeline.py)  — Delta Live Tables               │
│                                                                              │
│  DLT Expectations:  expect_all_or_drop  │  expect_or_fail  │  expect_or_warn │
│                                                                              │
│  ┌────────────┐ ┌──────┐ ┌─────────┐ ┌───────┐ ┌────────┐ ┌─────────┐     │
│  │subscription│ │user  │ │catalog  │ │order  │ │device  │ │payment  │     │
│  └────────────┘ └──────┘ └─────────┘ └───────┘ └────────┘ └─────────┘     │
│            │         │                    │          │                       │
│            └─────────┴────────────────────┴──────────┘                      │
│                              ▼ JOIN                                          │
│                   ┌──────────────────────┐                                   │
│                   │ subscription_enriched │  ← fact table w/ all dims        │
│                   └──────────────────────┘                                   │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ Batch / foreachBatch streaming
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ GOLD LAYER  (03_gold_aggregations.py)                                        │
│                                                                              │
│  agg_subscription_metrics_daily      agg_subscription_metrics_monthly        │
│  agg_revenue_by_plan                 agg_user_cohort_analysis                │
│  agg_device_distribution             agg_payment_performance                 │
│  dim_subscription_status_snapshot    fact_subscription_lifecycle             │
│                                                                              │
│  • Delta MERGE (upsert)  • Partitioned  • CDF enabled  • Optimised write     │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ Delta Sharing
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ CONSUMERS                                                                    │
│  sms_external_share  →  Analytics Team, Finance Team, External Partners      │
│  Direct UC access    →  BI Tools (Power BI / Tableau), Databricks SQL        │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Entities & Relationships

```
 User (user_ref_id PK)
  │
  ├──< Subscription (sub_ref_id PK, plan_code FK, user_ref_id FK)
  │         │
  │         ├──< Order (order_ref_id PK, sub_ref_id FK, user_ref_id FK, payment_ref_id FK)
  │         │              │
  │         │         Payment (payment_ref_id PK)
  │         │
  │         └──< Device (device_ref_id PK, sub_ref_id FK, user_ref_id FK)
  │
  └── Catalog (plan_code PK)  ←── referenced by Subscription.plan_code
```

---

## File-by-File Guide

### `config.py`
Central configuration for Kafka brokers, topics, Unity Catalog paths, Delta Sharing recipients, access control groups, and PII column lists.  Update this before deploying to a new environment.

### `01_bronze_kafka_ingestion.py`
Reads all 6 Kafka topics as Structured Streaming jobs.  Each message is:
1. Deserialized as JSON against the typed schema
2. Enriched with Kafka metadata (topic, partition, offset, timestamp)
3. Hashed (MD5) for downstream deduplication
4. Written to an append-only Bronze Delta table with CDF enabled
5. Malformed records routed to a `*_dlq` Dead Letter Queue table

### `02_silver_dlt_pipeline.py`
A Delta Live Tables pipeline file.  Deploy as a DLT pipeline in Databricks.  Provides:
- **DLT Expectations** on every entity (drop, fail, or warn on violations)
- **Cleansing**: whitespace trimming, case normalisation, safe type casting
- **Derived columns**: age bands, days remaining, live status, net amounts, etc.
- **Joins**: `subscription_enriched` joins all 6 entities into one analytical fact

### `03_gold_aggregations.py`
Reads Silver tables and produces 8 Gold aggregation tables.  Can run as:
- `python 03_gold_aggregations.py batch` — scheduled job after Silver refresh
- `python 03_gold_aggregations.py streaming` — continuous foreachBatch mode

### `04_governance_access_control.py`
Run once (idempotent) after pipeline bootstrap:
1. **Unity Catalog Tags** — catalog, schema, table, and column tags
2. **Column Masking Functions** — email, phone, name, DOB, address, card, bank
3. **Column Mask Application** — bound to PII columns via `ALTER COLUMN SET MASK`
4. **Row-Level Security** — country-based filter for support staff
5. **GRANT Statements** — group-to-privilege mapping across all layers
6. **Delta Sharing** — 3 shares (full, finance, partner) with recipients
7. **Audit Log Table** — governance event tracking

### `05_record_simulator.py`
Standalone Python script with 4 modes:

| Mode | Description |
|------|-------------|
| `kafka` | Continuous real-time publish to Kafka topics |
| `files` | Writes NDJSON files for Auto Loader testing |
| `rolling` | Drops batches of files at a configurable interval |
| `stats` | Prints record field stats (null rates, shapes) |

Add `--chaos` to any mode to inject malformed records, duplicates, and late arrivals.

---

## Access Control Matrix

| Group | Bronze | Silver (non-PII) | Silver (PII) | Gold |
|-------|--------|-----------------|--------------|------|
| `sms_admins` | Full | Full | Full (unmasked) | Full |
| `sms_data_engineers` | Full | Full | Full (unmasked) | Full |
| `sms_analysts` | ✗ | SELECT | SELECT (partial mask) | Full Gold |
| `sms_finance` | ✗ | ✗ | SELECT (partial mask) | Revenue + Payment only |
| `sms_support` | ✗ | SELECT (non-PII) | Enriched only (masked) | Limited Gold |
| `sms_external` | ✗ | ✗ | ✗ | Aggregates via Delta Share |

### Column Masking Rules

| Column | Admins/Engineers | Analysts/Finance | Support/External |
|--------|-----------------|-----------------|-----------------|
| `email` | full value | `u***@domain.com` | `***@***.***` |
| `phone_number` | full value | `*****7890` | `**********` |
| `full_name` | full value | `J***` | `***` |
| `date_of_birth` | full date | `1990-**-**` | `****-**-**` |
| `address` | full value | `*** MASKED ***` | `*** MASKED ***` |
| `card_number` | full value | `**** **** **** 1234` | `**** **** **** 1234` |
| `bank_account` | full value | `****5678` | `****5678` |

---

## Delta Sharing Structure

```
sms_external_share  →  analytics_team     (all Gold tables)
sms_finance_share   →  finance_team       (revenue + payment + monthly metrics)
sms_partner_share   →  external_partner   (monthly metrics + device distribution)
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install pyspark delta-spark kafka-python faker tqdm
```

### 2. Configure
Edit `config.py` with your Kafka broker addresses, storage account paths, and Databricks catalog name.

### 3. Generate test data (stats preview)
```bash
python 05_record_simulator.py --mode stats
```

### 4. Start the simulator (file mode, no Kafka required)
```bash
python 05_record_simulator.py --mode files --entities all --records 50000 --output ./test_data
```

### 5. Start the simulator against Kafka
```bash
python 05_record_simulator.py \
    --mode kafka \
    --entities all \
    --rate 100 \
    --duration 600 \
    --chaos \
    --kafka-broker your-broker:9092
```

### 6. Run Bronze ingestion (Databricks cluster)
```bash
# Submit as a Databricks Job or run notebook
python 01_bronze_kafka_ingestion.py
```

### 7. Deploy Silver DLT Pipeline
Create a DLT pipeline in Databricks UI:
- Source file: `02_silver_dlt_pipeline.py`
- Continuous mode: ✓
- Target catalog: `sms_catalog`
- Target schema: `silver`

### 8. Run Gold Aggregations
```bash
# Batch mode (scheduled after Silver)
python 03_gold_aggregations.py batch

# Streaming mode (continuous)
python 03_gold_aggregations.py streaming
```

### 9. Apply Governance
```bash
# Run once as a Databricks Job with admin privileges
python 04_governance_access_control.py
```

---

## Chaos Testing Scenarios

The `--chaos` flag tests:

| Scenario | DLT Response |
|----------|-------------|
| Null primary key | `expect_all_or_drop` drops record |
| Invalid status enum | `expect_all_or_drop` drops record |
| Future start_date | `expect_or_fail` halts pipeline |
| Malformed JSON | Routed to `*_dlq` Dead Letter table |
| Duplicate records | Downstream dedup via APPLY CHANGES |
| Late-arriving records | Watermark-based handling |
| Extra unexpected fields | Schema evolution / ignored |

---

## Monitoring Queries

```sql
-- DLT expectation violations
SELECT * FROM event_log('<pipeline_id>')
WHERE details:flow_progress:data_quality:expectations IS NOT NULL;

-- DLQ record count by entity
SELECT _entity_name, _parse_error, count(*) as error_count
FROM sms_catalog.bronze.subscription_dlq
GROUP BY ALL ORDER BY error_count DESC;

-- Gold freshness check
SELECT 'agg_sub_daily' as table_name, max(_computed_at) as last_refreshed
FROM sms_catalog.gold.agg_subscription_metrics_daily
UNION ALL
SELECT 'payment_perf', max(_computed_at)
FROM sms_catalog.gold.agg_payment_performance;

-- Active subscriber MRR by country
SELECT country_code, sum(mrr) as total_mrr, sum(arr) as total_arr
FROM sms_catalog.gold.agg_subscription_metrics_monthly
WHERE year_month = date_format(current_date(), 'yyyy-MM')
GROUP BY country_code ORDER BY total_mrr DESC;
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP` | Kafka broker address(es) | `localhost:9092` |

Sensitive values (Kafka credentials, storage keys) should be stored in **Databricks Secrets** and referenced as `{{secrets/scope/key}}` in `config.py`.
