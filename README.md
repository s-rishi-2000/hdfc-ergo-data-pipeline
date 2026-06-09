# 🏥 HDFC ERGO Health Insurance Data Engineering Platform

## 📖 Project Overview

This project is a robust, scalable Data Engineering pipeline built for HDFC ERGO health insurance data, tailored for the Indian healthcare market. It processes data from operational databases (SSMS/PgAdmin), transforms it through a Medallion architecture (Bronze → Silver → Gold), and serves it to BI analysts via Snowflake.

The pipeline is designed to handle a production scale of **3-4 GB containing 1 Million+ claims**, while gracefully handling **~15% intentional data quality issues** (Nulls, Duplicates, Invalid Formats, Inconsistent Values).

---

## 🏗️ Architecture

We utilize the modern **Lakehouse to Warehouse pattern**:

```
[SSMS / PgAdmin]
        │
        ▼ (ADF Copy Activity)
[ADLS Gen2 Raw Zone] (CSV/Parquet)
        │
        ▼ (Databricks Auto Loader / Read)
[Databricks Bronze Layer] (Delta Tables - Raw/String Cast)
        │
        ▼ (Cleansing, Dedup, Standardization)
[Databricks Silver Layer] (Delta Tables - Typed/Enriched)
        │
        ▼ (SCD2, PII Masking, SK Generation, Snowflake Joins)
[Databricks Gold Layer] (Delta Tables - Business Ready)
        │
        ▼ (Snowflake Spark Connector)
[Snowflake] (Serving Layer for BI/Tableau/PowerBI)
```

*(See diagram.svg for visual reference)*

---

## 🛠️ Tech Stack

- **Cloud**: Microsoft Azure (ADLS Gen2, Azure Data Factory, Azure Key Vault)
- **Compute & Lakehouse**: Databricks (PySpark, Spark SQL, Delta Lake)
- **Governance**: Databricks Unity Catalog
- **Serving Layer**: Snowflake
- **Version Control**: Git / GitHub
- **Languages**: Python (PySpark), SQL

---

## 📂 Repository Structure

```
hdfc-ergo-data-pipeline/
├── Dataset/                      # 100-row dummy CSVs for local/dev testing
├── notebook/
│   ├── bronze/                   # Raw ingestion (Cast to String, Audit Cols)
│   ├── silver/                   # Cleansing, Dedup, Type Casting, SK Enrichment
│   └── gold/                     # SK Generation, SCD2, PII Masking, Snowflake Joins
├── resources/                    # Databricks Job YAML / Configs
├── databricks_workflow_dev/      # Exported Databricks workflow JSON definitions
├── dummy/                        # Test scripts / synthetic data generation
├── diagram.svg                   # Architecture diagram
└── README.md                     # Project documentation (You are here)
```

---

## 🛡️ Core Data Engineering Standards (The "Rulebook")

### 1. Anti-Fragility (Bronze Layer)
- **All incoming data is explicitly cast to STRING.** Bad source data (e.g., text in a date column) will never crash ingestion.
- Audit columns `_ingested_at` and `_source_file` are appended to all tables.

### 2. Safe Casting & Quarantine (Silver Layer)
- Safe casting via **regex validation** and `try_to_date()`/`try_to_timestamp()`. Invalid data becomes **NULL rather than crashing** the pipeline.
- **Financials are strictly cast to DECIMAL(15,2)** to avoid floating-point math errors.
- Negative amounts are converted to absolute values using `ABS()`.
- **Deduplication** uses `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY _ingested_at DESC)`.

### 3. Orphan Handling (Gold Layer - Facts)
- Fact tables use **COALESCE(LOOKUP, -1)** for Foreign Keys.
- If a claim references a **missing member**, it maps to an "Unknown" bucket **(SK = -1)** rather than dropping the financial record.
- This ensures **no revenue loss due to missing dimensions**.

### 4. PII Masking (Gold Layer - Dimensions)
- `name_masked`: First letter + `***`
- `aadhar_masked`: `XXXX-XXXX-` + last 4 digits
- `pan_masked`: First 3 letters + `XXXXXXXXXX`

### 5. SCD Type 2 (Gold Layer - Dimensions)
- `Dim_Members` and `Dim_Providers` track history using:
  - `_valid_from`: When the record became active
  - `_valid_to`: When the record expired
  - `_is_current`: Boolean flag (1 = current, 0 = historical)

### 6. Snowflake Schema Design
- **Conformed `Dim_Geography`** shared across Members, Providers, and Facilities.
- **Date hierarchy** split into `Dim_Date` and `Dim_Month`.
- Star schema pattern for optimal BI query performance.

---

## 🚀 Environment Setup & Azure Migration Guide

*This guide is specifically designed to rebuild the environment if the Azure free trial expires.*

### Step 1: Azure Infrastructure

Create the following resources in the Azure Portal (or use saved ARM Templates):

1. **ADLS Gen2 Storage Account**: Create containers for `raw`, `bronze`, `silver`, `gold`.
2. **Azure Data Factory**: Linked services to SSMS and ADLS Gen2.
3. **Azure Key Vault**: Store Snowflake credentials and ADLS access keys.
4. **Databricks Workspace**: Deploy a **Premium/Unified workspace**.

### Step 2: Databricks Unity Catalog Setup

1. Enable **Unity Catalog** in the Databricks Workspace.
2. Create **Catalogs** for environment separation:
   - `hdfc_ergo_dev` (100-row dummy data)
   - `hdfc_ergo_perf` (1GB volume test data)
   - `hdfc_ergo_prod` (Live ADF data)
3. Inside each Catalog, create **Schemas**: `bronze`, `silver`, `gold`.

### Step 3: Cluster Configuration

1. Install the **Snowflake Spark Connector (JAR)** on the Databricks cluster.
2. Link the cluster to the **Azure Key Vault** using Secret Scopes.

### Step 4: Import Git Repository

1. Link the Databricks workspace to this Git repository.
2. Pull the **main branch** for the working pipeline.
3. Pull the **perf branch** for volume testing/optimizations.

---

## 🔄 Workflow Execution

The pipeline execution is managed via the **Databricks Job definition** (found in `databricks_workflow_dev/` or `resources/transformation.yml`).

The **dependency flow** ensures referential integrity:

1. **Extract (Bronze)**: 15 tables run in **parallel**.
2. **Transform (Silver)**: 
   - Dimensions run in parallel
   - Facts **wait on Bronze Dimensions** for SK-to-Business-Key enrichment
3. **Load (Gold)**: 
   - Foundation Dims load first
   - `Dim_Geography` derives from Silver
   - Snowflake Dims look up Geography
   - Core `Fact_Claims` loads
   - Satellite Facts load last
4. **Publish**: Gold tables sync to Snowflake for BI consumption *(Future/Prod phase)*

---

## 🌿 Branch Strategy

- **main**: Stable, working pipeline logic using **overwrite mode** (Dev/Functional testing).
- **perf**: Volume testing branch (1GB+ data). Includes optimizations like `broadcast()` joins, Hash-based Surrogate Keys, and `partitionBy()`. **Never merge this back into main** if it contains synthetic data generation scripts.

---

## 📈 Future Roadmap (Prod Scale-Up)

- [ ] Implement **Delta MERGE (Upsert)** in Silver/Gold layers to transition from Full Load to Incremental Batch Processing.
- [ ] Implement **High Watermark tracking** in ADF to only extract new/updated rows from SSMS.
- [ ] Build the **publish notebooks** to sync Gold Delta Tables to Snowflake.
- [ ] Apply **Partition Pruning** (`PARTITION BY claim_date`) and **Z-Ordering** (`ZORDER BY member_sk`) on Fact tables.

---
Disclaimer: This repository is a purely personal, self-made educational project developed for learning data engineering. It is not affiliated with, sponsored by, or endorsed by HDFC ERGO General Insurance Company. All data schemas, configurations, and scripts are simulated and use dummy data.
