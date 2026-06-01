🏥 HDFC ERGO Health Insurance Data Engineering Platform
📖 Project Overview
This project is a robust, scalable Data Engineering pipeline built for HDFC ERGO health insurance data, tailored for the Indian healthcare market. It processes data from operational databases (SSMS/PgAdmin), transforms it through a Medallion architecture (Bronze → Silver → Gold), and serves it to BI analysts via Snowflake.

The pipeline is designed to handle a production scale of 3-4 GB containing 1 Million+ claims, while gracefully handling ~15% intentional data quality issues (Nulls, Duplicates, Invalid Formats, Inconsistent Values).

🏗️ Architecture
We utilize the modern Lakehouse to Warehouse pattern:

[SSMS / PgAdmin]        │       ▼ (ADF Copy Activity)[ADLS Gen2 Raw Zone] (CSV/Parquet)       │       ▼ (Databricks Auto Loader / Read)[Databricks Bronze Layer] (Delta Tables - Raw/String Cast)       │       ▼ (Cleansing, Dedup, Standardization)[Databricks Silver Layer] (Delta Tables - Typed/Enriched)       │       ▼ (SCD2, PII Masking, SK Generation, Snowflake Joins)[Databricks Gold Layer] (Delta Tables - Business Ready)       │       ▼ (Snowflake Spark Connector)[Snowflake] (Serving Layer for BI/Tableau/PowerBI)
(See diagram.svg for visual reference)

🛠️ Tech Stack
Cloud: Microsoft Azure (ADLS Gen2, Azure Data Factory, Azure Key Vault)
Compute & Lakehouse: Databricks (PySpark, Spark SQL, Delta Lake)
Governance: Databricks Unity Catalog
Serving Layer: Snowflake
Version Control: Git / GitHub
Languages: Python (PySpark), SQL
📂 Repository Structure
text

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
🛡️ Core Data Engineering Standards (The "Rulebook")
1. Anti-Fragility (Bronze)
All incoming data is explicitly cast to STRING. Bad source data (e.g., text in a date column) will never crash ingestion.
Audit columns _ingested_at and _source_file are appended.
2. Safe Casting & Quarantine (Silver)
Safe casting via regex validation and try_to_date/try_to_timestamp. Invalid data becomes NULL rather than crashing the pipeline.
Financials are strictly cast to DECIMAL(15,2) to avoid floating-point math errors. Negative amounts are converted to absolute values using ABS().
Deduplication uses ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY _ingested_at DESC).
3. Orphan Handling (Gold Facts)
Fact tables use COALESCE(LOOKUP, -1) for Foreign Keys. If a claim references a missing member, it maps to an "Unknown" bucket (SK = -1) rather than dropping the financial record.
4. PII Masking (Gold Dimensions)
name_masked: First letter + ***
aadhar_masked: XXXX-XXXX- + last 4 digits
pan_masked: First 3 letters + XXXXXXXXXX
5. SCD Type 2 (Gold Dimensions)
Dim_Members and Dim_Providers track history using _valid_from, _valid_to, and _is_current columns.
6. Snowflake Schema Design
Conformed Dim_Geography shared across Members, Providers, and Facilities.
Date hierarchy split into Dim_Date and Dim_Month.
🚀 Environment Setup & Azure Migration Guide
This guide is specifically designed to rebuild the environment if the Azure free trial expires.

Step 1: Azure Infrastructure
Create the following resources in the Azure Portal (or use saved ARM Templates):

ADLS Gen2 Storage Account: Create containers for raw, bronze, silver, gold.
Azure Data Factory: Linked services to SSMS and ADLS Gen2.
Azure Key Vault: Store Snowflake credentials and ADLS access keys.
Databricks Workspace: Deploy a Premium/Unified workspace.
Step 2: Databricks Unity Catalog Setup
Enable Unity Catalog in the Databricks Workspace.
Create Catalogs for environment separation:
hdfc_ergo_dev (100-row dummy data)
hdfc_ergo_perf (1GB volume test data)
hdfc_ergo_prod (Live ADF data)
Inside each Catalog, create Schemas: bronze, silver, gold.
Step 3: Cluster Configuration
Install the Snowflake Spark Connector (JAR) on the Databricks cluster.
Link the cluster to the Azure Key Vault using Secret Scopes.
Step 4: Import Git Repository
Link the Databricks workspace to this Git repository.
Pull the main branch for the working pipeline.
Pull the perf branch for volume testing/optimizations.
🔄 Workflow Execution
The pipeline execution is managed via the Databricks Job definition (found in databricks_workflow_dev or resources/transformation.yml).
The dependency flow ensures referential integrity:

Extract (Bronze): 15 tables run in parallel.
Transform (Silver): Dimensions run in parallel; Facts wait on Bronze Dimensions for SK-to-Business-Key enrichment.
Load (Gold): Foundation Dims load first -> Dim_Geography derives from Silver -> Snowflake Dims look up Geography -> Core Fact_Claims loads -> Satellite Facts load last.
Publish: Gold tables sync to Snowflake for BI consumption (Future/Prod phase).
🌿 Branch Strategy
main: Stable, working pipeline logic using overwrite mode (Dev/Functional testing).
perf: Volume testing branch (1GB+ data). Includes optimizations like broadcast() joins, Hash-based Surrogate Keys, and partitionBy(). Never merge this back into main if it contains synthetic data generation scripts.
📈 Future Roadmap (Prod Scale-Up)
 Implement Delta MERGE (Upsert) in Silver/Gold layers to transition from Full Load to Incremental Batch Processing.
 Implement High Watermark tracking in ADF to only extract new/updated rows from SSMS.
 Build the publish notebooks to sync Gold Delta Tables to Snowflake.
 Apply Partition Pruning (PARTITION BY claim_date) and Z-Ordering (ZORDER BY member_sk) on Fact tables.
