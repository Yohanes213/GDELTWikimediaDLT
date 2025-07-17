# GDELT & Wikimedia Delta Live Tables (DLT) Lakehouse Pipeline

## Overview

This project implements a robust data pipeline that ingests, processes, and analyzes two real-world data sources—**GDELT Events** and **Wikimedia EventStreams**—using Databricks Delta Live Tables (DLT). The pipeline covers the full workflow from raw data ingestion to CI/CD automation with GitHub Actions, and provides SQL queries and dashboards for data exploration and visualization.

---

## Data Sources

- **GDELT Events**: Global event data, updated every 15 minutes ([GDELT Project](http://data.gdeltproject.org/events/)).
- **Wikimedia EventStreams**: Real-time stream of Wikipedia edit events ([Wikimedia RecentChange Stream](https://stream.wikimedia.org/v2/stream/recentchange)).

---

## Pipeline Architecture

The pipeline is organized into the following layers for each data source:

- **Staging**: Download and store raw data in Azure Blob Storage.
- **Bronze**: Ingest raw data into Delta tables with schema enforcement.
- **Silver**: Data cleaning, quality checks, and flattening.
- **Gold**: (Not implemented; see below.)

### GDELT Pipeline
- **Staging**: Downloads 15-minute interval CSV ZIPs, extracts, and uploads to Azure Blob Storage (`notebooks/01_download_gdelt.py`).
- **Bronze**: Loads raw data into Delta tables with a detailed schema (`notebooks/dlt_pipeline.py`).
- **Silver**: Cleans data, applies data quality rules, and adds readable timestamps. DQ rules are defined in `src/dq_rules/gdelt.py`.

### Wikimedia Pipeline
- **Staging**: Buffers and uploads batches of JSON events from the SSE stream (`notebooks/02_download_wikimedia.py`).
- **Bronze**: Ingests raw JSON into Delta tables with a nested schema (`notebooks/dlt_pipeline.py`).
- **Silver**: Cleans, filters, and flattens data, applies DQ rules from `src/dq_rules/wikimedia.py`.

### Gold Layer
- **Not Implemented**: No direct relationship between GDELT and Wikimedia datasets, so no Gold layer was created.

---

## Project Structure

```
GDELTWikimediaDLT/
├── images/                # Pipeline, dashboard, and Genie screenshots
├── notebooks/             # DLT pipeline and data download scripts
│   ├── dlt_pipeline.py
│   ├── 01_download_gdelt.py
│   └── 02_download_wikimedia.py
├── sql/                   # SQL queries for analysis and dashboards
│   ├── top_countries_gdelt.sql
│   ├── global_gdelt_top_10_countries.sql
│   ├── silver_gdelt_secure_view.sql
│   └── en_wiki_events_by_minute.sql
├── src/                   # Config, DQ rules, and schemas
│   ├── config.py
│   ├── download_and_upload_gdelt.py
│   ├── dq_rules/
│   └── schemas/
├── databricks.yml         # Asset bundle config for Databricks deployment
└── README.md
```

---

## Setup & Usage

### Prerequisites
- Databricks workspace (with DLT enabled)
- Azure Blob Storage account
- Python 3.8+
- Databricks CLI (for CI/CD)

### 1. Configure Azure Blob Storage
- Update connection details in `src/config.py` as needed.

### 2. Download & Upload Data
- **GDELT**: Run `notebooks/01_download_gdelt.py` to download and upload GDELT data for a specified time range.
- **Wikimedia**: Run `notebooks/02_download_wikimedia.py` to stream and upload Wikimedia events in batches.

### 3. Deploy & Run the DLT Pipeline
- Use the Databricks UI or the CLI with the provided `databricks.yml` asset bundle.
- The main pipeline notebook is `notebooks/dlt_pipeline.py`.

### 4. Explore Data with SQL
- Use the SQL queries in the `sql/` folder for analysis and dashboarding.

---

## CI/CD with GitHub Actions

Automated deployment is set up using GitHub Actions and Databricks Asset Bundles:

- On every push to `main`, the workflow:
  1. Installs the Databricks CLI
  2. Validates the asset bundle
  3. Deploys the pipeline to Databricks
  4. Runs the pipeline

**Secrets required:**
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

See the sample workflow in the project documentation and `.github/workflows/` (if present).

---

## Visualizations & Dashboards

- **SQL Queries**: Located in `sql/`, e.g.:
  - `top_countries_gdelt.sql`: Top countries by GDELT event count
  - `global_gdelt_top_10_countries.sql`: Top 10 countries globally
  - `silver_gdelt_secure_view.sql`: Secure view for analytics
  - `en_wiki_events_by_minute.sql`: Wikipedia edit activity by minute
- **Dashboards**: Use query results to build dashboards in Databricks or BI tools.
- **Screenshots**: See `images/` for pipeline, dashboard, and Genie AI assistant examples.

---

## Genie: AI-Powered Data Exploration

Leverage Genie, the Databricks AI assistant, to ask natural language questions about your data and generate instant SQL queries, visualizations, or summaries.

Example questions:
- Which countries have the highest number of GDELT events in the last month?
- What are the most common event types in the GDELT dataset?
- What are the peak hours for English Wikipedia edit activity?
- Who are the top users editing English Wikipedia recently?
- Is there a correlation between spikes in GDELT events and Wikipedia edit activity?

---

## Challenges & Lessons Learned

- **Schema Complexity**: Mapping and enforcing schemas for both datasets, especially nested JSON, required careful design.
- **DLT Limitations**: Direct streaming from Wikimedia EventStreams to DLT was not feasible; a batch-based workaround was implemented.
- **CI/CD Evolution**: Adoption of Databricks Asset Bundles streamlined deployment and configuration.

---

## License

This project is for educational purposes. See individual files for license details if present.