# aws-healthcare-data-lake

An end-to-end AWS data lake pipeline that ingests public CMS Medicare data, transforms it through Medallion Architecture layers, and makes it queryable via Athena. Built to mirror production-grade data engineering patterns used in healthcare analytics.

---

## Overview

This pipeline processes the CMS Medicare Physician and Other Practitioners dataset through three structured layers: raw ingestion, standardized transformation, and aggregated business output. The pipeline is event-driven — a file upload to S3 Bronze automatically triggers the full transformation chain via Lambda.

The Glue ETL jobs are implemented as local PySpark scripts written in Glue-compatible style, allowing the full transformation logic to run without incurring Glue compute costs while keeping all other services (S3, Athena, Lambda, CloudWatch) real and live on AWS.

---

## Architecture

```
CMS Medicare CSV (data.cms.gov)
           │
           ▼
     S3 Bronze Layer
  s3://bucket/bronze/cms_providers/year=2022/
           │
           ▼
   PySpark ETL Script
  (Glue-compatible, runs locally)
           │
           ▼
     S3 Silver Layer
  s3://bucket/silver/cms_providers/state=NY/
           │
           ▼
    Athena SQL Aggregation
           │
           ▼
      S3 Gold Layer
  s3://bucket/gold/provider_state_summary/
           │
           ▼
   Lambda + CloudWatch
  (event-driven automation + alerting)
```

**Event chain:** File upload to Bronze → S3 event → Lambda → Glue Crawler → PySpark ETL → Silver output

---

## S3 Bucket Structure

```
aws-healthcare-data-lake/
├── bronze/
│   └── cms_providers/
│       └── year=2022/
│           └── Medicare_Physician_Other_Practitioners.csv
│
├── silver/
│   └── cms_providers/
│       ├── state=AK/
│       │   └── part-0000.parquet
│       ├── state=AL/
│       │   └── part-0000.parquet
│       └── ... (one partition per state)
│
└── gold/
    └── provider_state_summary/
        └── part-0000.parquet
```

### Why each layer exists

**Bronze** is the immutable landing zone. The raw CSV is stored exactly as received from CMS with no modifications. If anything goes wrong downstream, Bronze is the source of truth you can always reprocess from. Never transform data in place at this layer.

**Silver** is the engineering layer. The PySpark ETL job reads Bronze, standardizes column names to snake_case, filters invalid records, casts types correctly, deduplicates on provider NPI, adds derived columns, converts to Parquet, and partitions by state. Silver is what analysts and downstream jobs actually consume.

**Gold** is the business layer. An Athena SQL aggregation reads Silver and produces one summary row per state: total providers, total services, total Medicare payment, and average payment per service. Gold is optimized for reporting and dashboarding — no one building a report should scan millions of raw provider rows.

---

## Dataset

**Source:** CMS Medicare Physician and Other Practitioners by Provider  
**URL:** https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider/data
**File:** Provider and Services CSV (~570MB)  
**Year:** 2024  
**License:** Public domain (U.S. government open data)

Each row in the dataset represents a unique provider-service combination: a doctor or medical practice, the procedure they billed Medicare for, the number of times they billed it, and the total Medicare payment amount.

---

## Tech Stack

| Layer | Tool | Role |
|---|---|---|
| Storage | AWS S3 | All three Medallion layers |
| Transformation | PySpark (local, Glue-compatible) | Bronze to Silver ETL |
| Schema | AWS Glue Data Catalog | Table definitions for Athena |
| Query | AWS Athena | Silver verification + Gold aggregation |
| Automation | AWS Lambda | Event-driven pipeline trigger |
| Alerting | AWS CloudWatch | Glue job failure alarm |
| IaC | Terraform | Infrastructure as code |
| Language | Python | ETL scripts and Lambda function |

---

## Project Milestones

### Milestone 1 — S3 Bucket and Medallion Structure
Create the S3 bucket, define the three-layer prefix structure, and upload the raw CMS CSV to Bronze. Document naming conventions.

### Milestone 2 — Glue Crawler and Data Catalog
Configure schema discovery on the Bronze layer. Register the table definition in the Glue Data Catalog. Verify row count via Athena.

### Milestone 3 — Glue ETL Job: Bronze to Silver
Write and run the PySpark transformation script. Output partitioned Parquet files to Silver. Validate partition structure in S3.

### Milestone 4 — Gold Aggregation via Athena SQL
Write a CREATE TABLE AS SELECT query that aggregates Silver to one row per state. Export sample output.

### Milestone 5 — Lambda Automation
Wire up an S3 event trigger to a Lambda function that starts the pipeline on file arrival. Add CloudWatch alerting for job failures.

---

## Repository Structure

```
aws-healthcare-data-lake/
├── README.md
├── terraform/
│   └── s3.tf
├── glue_jobs/
│   └── cms_bronze_to_silver.py
├── lambda/
│   └── trigger_pipeline_on_s3.py
├── sql/
│   └── gold_provider_state_summary.sql
└── data/
    └── sample_gold_output.csv
```

---

## Interview Framing

This pipeline mirrors the architecture I built at Cedar Gate Technologies: raw healthcare data lands in S3 Bronze, a PySpark ETL job standardizes and partitions it to Silver, and Athena aggregations produce Gold-layer summaries queryable by business users. Lambda automation triggers the full pipeline on file arrival so there is no manual intervention. Infrastructure is defined in Terraform. The dataset is public CMS Medicare data, so the full codebase and sample outputs are shareable.

---

## Setup and Reproduction

Prerequisites: Python 3.8+, PySpark, AWS CLI configured with appropriate IAM permissions, Terraform.

Detailed setup instructions will be added as each milestone is completed.

---

## Author

Manisha  
Data Engineer | AWS | Azure | PySpark  
[GitHub](https://github.com/maneeshayE)  
[LinkedIn](https://www.linkedin.com/in/manisha-yadav-31a526186/)
