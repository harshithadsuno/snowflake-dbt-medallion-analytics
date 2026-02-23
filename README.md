# Snowflake + dbt Medallion Architecture Warehouse

## Project Overview

This project implements a layered data warehouse using Snowflake and dbt Cloud following the Medallion Architecture pattern (Bronze → Silver → Gold).

The goal is to transform raw CRM and ERP data into analytics-ready dimensional models suitable for BI and reporting.

---

## Architecture

### 🔹 Bronze Layer
- Raw source ingestion
- Minimal transformation
- Materialized as views

### 🔹 Silver Layer
- Data cleansing and standardization
- Date normalization
- Derived and corrected measures
- Materialized as tables

### 🔹 Gold Layer
- Dimensional modeling
- Fact and dimension tables
- Star schema structure
- Analytics-ready datasets

---

## Fact Table

**gold_fact_sales**

Grain:
> One row per order_number and product_key

Measures:
- sales_amount
- quantity
- price

Foreign Keys:
- product_key
- customer_key

---

## Dimensions

- gold_dim_customers
- gold_dim_products

Surrogate keys are used to improve join performance and maintain dimensional integrity.

---

## Tech Stack

- Snowflake (Data Warehouse)
- dbt Cloud (Transformation & Modeling)
- GitHub (Version Control)

---

## Current State

✔ Bronze, Silver, and Gold layers implemented  
✔ Dimensional modeling complete  
✔ Version controlled in GitHub  

Future Enhancements:
- Incremental fact model
- SCD Type 2 snapshots
- dbt tests expansion
- CI/CD workflow

---

## Author

Harshitha Devi Sunkara  
Data Analyst evolving into Analytics Engineer
