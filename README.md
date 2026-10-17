# 🚖 Uber Data Engineering Pipeline: PySpark & dbt Medallion Architecture

## 📌 Project Overview
This repository contains an end-to-end, production-grade data engineering pipeline built on **Databricks**. The project implements a strict **Medallion Architecture** (Bronze, Silver, Gold layers) to process Uber trip data. 

Rather than relying on static, hardcoded scripts, this pipeline emphasizes modern data engineering best practices: **dynamic configuration**, **Python Object-Oriented Programming (OOP) for PySpark transformations**, and **analytics engineering using dbt (Data Build Tool)** for modular dimensional modeling and historical state tracking.

---

## 🛠️ Tech Stack & Tools
* **Compute & Orchestration:** Databricks (Serverless Compute), Databricks Workflows
* **Storage:** Databricks Volumes, Delta Lake
* **Data Processing:** Python, PySpark (Structured Streaming, DataFrames, Window Functions)
* **Analytics Engineering:** dbt Cloud, dbt Snapshots, Jinja Templating, SQL
* **Version Control:** Git, GitHub

---

## 🏗️ Architecture & Data Flow

<img width="834" height="203" alt="image" src="https://github.com/user-attachments/assets/a18d4653-440e-4df0-874b-7e10d32ea833" />

The pipeline processes multiple entities (`customers`, `drivers`, `trips`, `locations`, `payments`, `vehicles`) through three refined layers:

### 🥉 Bronze Layer: Dynamic Raw Ingestion
The Bronze layer acts as the immutable historical archive of our source data.
* **PySpark Structured Streaming:** Implemented `spark.readStream` to incrementally load new raw CSV files from Databricks Volumes into Delta tables.
* **Cost Optimization:** Utilized the `once=True` trigger to process only new, unprocessed files (tracked via checkpoint locations) and immediately shut down compute, mimicking cost-efficient batch processing on a streaming architecture.
* **Dynamic Execution:** Replaced repetitive boilerplate code with a dynamic Python loop. A single script iterates through an entity list, dynamically injecting file paths, checkpoint directories, and target table names to ingest all source tables simultaneously.

### 🥈 Silver Layer: Cleansing, OOP & Upserts
The Silver layer provides a cleansed, deduplicated, and conformed "source of truth."
* **Python OOP Utilities:** Engineered a custom Python class (`transformations`) inside `custom_utils.py` to handle generic operations, keeping the main notebook clean and modular.
* **Dynamic Delta MERGE (Upserts):** Built a reusable `upsert` method that uses Python list comprehensions to auto-generate `MERGE` conditions dynamically based on provided primary keys. It validates incoming records against the `last_updated_timestamp` to ensure outdated records never overwrite newer data.
* **Data Quality & Transformations:** Handled deduplication using PySpark Window functions (partitioning by unique keys and ordering descending by timestamp). Applied complex transformations, including regex for phone number standardization, string splitting for email domains, and complex `when/otherwise` case statements for payment status derivations.

### 🥇 Gold Layer: Dimensional Modeling & SCD Type 2 via dbt
The Gold layer is optimized for business intelligence and reporting, structured as a Star Schema (Fact and Dimension tables).
* **dbt Cloud Integration:** Configured dbt Cloud to connect to the Databricks SQL warehouse, mapping the Bronze and Silver Delta tables as formal dbt `sources.yml` for full data lineage tracking.
* **Slowly Changing Dimensions (SCD Type 2):** Leveraged **dbt Snapshots** (timestamp strategy) to track historical state changes in dimension tables (e.g., `dim_customers`, `dim_locations`). This ensures that changes over time (like a trip status moving from "pending" to "completed") are accurately preserved with valid `from` and `to` dates.
* **Jinja Templating:** Used dbt Jinja macros (`{% for column in columns %}`) to dynamically generate repetitive `SELECT` statements for the `fact_trips` table.
* **Custom Schema Management:** Overrode default dbt deployment behaviors by creating a custom `generate_schema_name.sql` macro, forcing models to deploy cleanly into `silver` and `gold` Databricks schemas without the default user prefixes.

## 📂 Repository Structure

```text
pyspark_dbt_project/
├── pyspark_jobs/
│   ├── bronze_ingestion.py        # Dynamic PySpark readStream logic
│   ├── silver_transformation.py   # Cleansing and object instantiation
│   └── utils/
│       └── custom_utils.py        # Python OOP class for deduplication & upserts
└── dbt_project/
    ├── dbt_project.yml            # Project config, materialization rules
    ├── models/
    │   ├── sources.yml            # Registers Databricks tables as dbt sources
    │   └── gold/
    │       └── fact_trips.sql     # Jinja-templated incremental fact table
    ├── snapshots/
    │   └── scd_dimensions.yml     # SCD Type 2 Snapshot configurations
        └── fact_trips.yml         # Snapshot referencing trips.sql to land facts in Gold
    └── macros/
        └── generate_schema_name.sql # Macro to enforce clean schema naming





