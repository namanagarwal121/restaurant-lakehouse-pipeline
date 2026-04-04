# 🍽️ Restaurant Lakehouse Pipeline

End-to-end **data engineering project** built on Databricks, implementing a modern **Lakehouse architecture** with streaming, CDC, and SCD Type 2 modeling.

---

## 📊 Architecture

![](diagrams/project_architecture.png)

---

## 🚀 Overview

This project simulates a real-world restaurant analytics platform that ingests data from multiple sources and transforms it into business-ready insights.

### Key Features:

* ⚡ **Streaming ingestion** using Event Hub (Kafka-compatible)
* 🗃️ **Batch ingestion** for historical and reference data
* 🔄 **CDC pipelines** using Delta Live Tables (DLT)
* 🧱 **Medallion architecture** (Bronze → Silver → Gold)
* 📈 **SCD Type 2 modeling** in Gold layer
* 🧩 **Star schema design** (facts & dimensions)
* 🤖 **Extensible for ML (sentiment analysis, recommendations)**

---

## 🏗️ Architecture Layers

### 🟫 Bronze Layer (Raw Ingestion)

* `orders_streaming` (real-time orders)
* `historical_orders`
* `customers`
* `restaurants`
* `menu_items`
* `customer_reviews`

✔ Raw, append-only ingestion
✔ Minimal transformations

---

### ⚪ Silver Layer (Transformation & Modeling)

#### Fact Tables

* `silver_orders` (CDC applied)
* `fact_order_items` (exploded items)
* `fact_reviews`

#### Dimension Tables

* `dim_customers`
* `dim_restaurants`
* `dim_menu_items`

✔ Cleaned & structured
✔ Schema normalization
✔ Star schema modeling

---

### 🟨 Gold Layer (Business Layer - SCD Type 2)

* `d_sales_summary` → Revenue metrics per restaurant/day
* `d_customer_360` → Customer lifetime metrics
* `d_restaurant_reviews` → Ratings & review analytics

✔ SCD Type 2 (history tracking)
✔ Incremental processing (CDC)
✔ Business-ready datasets

---

## 🔄 Data Flow

```text
Event Hub / SQL Sources
        ↓
     Bronze (DLT)
        ↓
     Silver (DLT + CDC)
        ↓
     Gold (SCD Type 2)
        ↓
 Dashboards / ML Models
```

---

## ⚙️ Pipelines

This project uses **3 separate Delta Live Tables pipelines**:

| Pipeline | Purpose                         |
| -------- | ------------------------------- |
| Bronze   | Data ingestion (stream + batch) |
| Silver   | Data cleaning, modeling, CDC    |
| Gold     | Aggregations, SCD Type 2        |

---

## 🔁 Orchestration

Pipelines are orchestrated using Databricks Workflows:

```text
Bronze → Silver → Gold
```

✔ Dependency-based execution
✔ Fault isolation
✔ Scalable scheduling

---

## 🛠️ Tech Stack

* **Databricks**
* **Delta Live Tables (DLT)**
* **Apache Spark (PySpark)**
* **Unity Catalog**
* **Azure Event Hub (Kafka API)**
* **Delta Lake**

---

## 📂 Repository Structure

```text
restaurant-lakehouse-pipeline/

restaurant-lakehouse-pipeline/
│
├── pipelines/
│   ├── bronze/
│   │   ├── bronze_batch.py
│   │   ├── bronze_streaming.py
│   │
│   ├── silver/
│   │   ├── silver_orders.py
│   │   ├── silver_dimensions.py
│   │
│   ├── gold/
│   │   ├── gold_metrics.py
│
├── ingestion/
│   └── synthetic_data/
│       ├── data/
│       ├── sql/
│       ├── 00_sql_db.py
│       ├── 01_historical_orders.py
│       ├── 02_reviews.py
│       ├── 03_run.py
│       ├── 04_eventhub_orders.py
│       ├── requirements.txt
│       └── README.md
│
├── config/
│   ├── dev.yaml
│   └── prod.yaml
│
├── resources/
│   └── workflows.json
│
├── diagrams/
│   └── project_architecture.png
│
├── commands_used.md
├── dashboard_metrics.md
├── README.md

```

---

## 🧠 Key Concepts Demonstrated

* Streaming + Batch unification
* Change Data Capture (CDC)
* Slowly Changing Dimensions (SCD Type 2)
* Data modeling (Star Schema)
* Pipeline orchestration
* Incremental data processing

