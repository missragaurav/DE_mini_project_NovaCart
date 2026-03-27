#  NovaCart Data Engineering Project

##  Overview

This project implements an **end-to-end data pipeline** using **Databricks, PySpark, and Delta Lake** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw data from Azure Blob Storage, cleans and transforms it, and produces analytics-ready datasets and KPIs for business insights.

---

##  Architecture

```
Azure Blob Storage
        ↓
     Bronze Layer (Raw Data)
        ↓
     Silver Layer (Cleaned & Transformed Data)
        ↓
     Gold Layer (Analytics & KPIs)
```

---

##  Tech Stack

* **Databricks**
* **PySpark**
* **Delta Lake**
* **Azure Blob Storage**
* **GitHub (Version Control)**

---

##  Project Structure

```
DE_mini_project_NovaCart
│
├── 01_bronze
│   └── raw.py
│
├── 02_silver
│   ├── customer_transform.py
│   ├── exchange_rates_transform.py
│   ├── silver_orders.py
│   ├── silver_order_items.py
│   └── silver_product.py
│
├── 03_gold
│   ├── gold_dim_tables.py
│   ├── gold_fact_table.py
│   └── gold_kpi_metrics.py
│
└── README.md
```

---

##  Bronze Layer (Raw Data)

* Ingests data directly from **Azure Blob Storage**
* Stores raw, unprocessed data
* No transformations applied
* Tables:

  * customers_bronze
  * orders_bronze
  * products_bronze
  * order_items_bronze
  * exchange_rates_bronze

---

##  Silver Layer (Data Cleaning & Transformation)

* Data is cleaned and standardized
* Handles:

  * Missing values (`null`, `\N`, `NA`, etc.)
  * Data type conversions
  * Date format normalization
* Adds:

  * `is_valid` flag for data quality tracking

###  Parallel Processing

All silver transformations are designed to run **independently and in parallel** using Databricks Jobs.

---

##  Gold Layer (Analytics Layer)

###  Dimension Tables

* `dim_customers`
* `dim_products`
* `dim_date`

###  Fact Table

* `fact_orders`
* Includes:

  * Revenue calculations
  * Currency conversion to USD
  * Order-level metrics

---

##  KPI Metrics

The project generates key business KPIs:

* Total Revenue
* Revenue by Country
* Revenue by Channel
* Completed Orders
* Completed Order Rate
* Average Order Value (AOV)
* Top 5 Products by Revenue
* Active Customers
* Customer Acquisition Trend
* Data Quality Score

---

##  Data Pipeline (Workflow)

* Built using **Databricks Jobs & Workflows**
* Steps:

  1. Bronze ingestion
  2. Parallel Silver transformations
  3. Gold dimension tables
  4. Gold fact table
  5. KPI calculations

---

##  Key Features

* ✔ End-to-end ETL pipeline
* ✔ Medallion Architecture implementation
* ✔ Parallel processing in Silver layer
* ✔ Data quality handling
* ✔ Currency conversion logic
* ✔ Star schema design (Fact + Dimensions)
* ✔ Scalable and modular pipeline

---

##  How to Run

1. Upload data to Azure Blob Storage
2. Run Bronze ingestion notebook
3. Execute Silver notebooks (parallel)
4. Run Gold dimension + fact notebooks
5. Run KPI notebook

---

##  Future Improvements

* Add real-time streaming (Structured Streaming)
* Implement CI/CD pipeline
* Add dashboard (Power BI / Tableau)
* Data quality monitoring with alerts

---

##  Author

**Gaurav Mishra**


