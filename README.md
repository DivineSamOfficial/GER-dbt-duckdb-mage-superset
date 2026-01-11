# 🧱 Global Electronics Retailer End-To-End Analytics Engineering Project

### DuckDB · dbt · Mage · Apache Superset

<p align="center">
  <b>A production-style local analytics stack demonstrating the full analytics lifecycle</b>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#tech-stack">Tech Stack</a> •
  <a href="#data-modeling">Data Modeling</a> •
  <a href="#orchestration">Orchestration</a> •
  <a href="#visualization">Visualization</a> •
  <a href="#how-to-run">How to Run</a>
</p>

---

## 🚀 Project Overview

This project demonstrates a **complete, production-style analytics engineering workflow** built locally using modern open-source tools.

The goal of the project is to simulate how data flows from **raw external sources** through **transformation, orchestration, and analytics**, ultimately delivering **business insights via interactive dashboards**.

The stack mirrors real-world analytics platforms used in industry, while remaining fully reproducible on a local machine.

---

## 🧠 Business Use Case

The dataset represents a **Global Electronics Retailer**, covering:

* Customers
* Products
* Stores
* Sales transactions
* Currency exchange rates

The analytics layer enables stakeholders to:

* Track total revenue, costs, and profit
* Analyze sales trends over time
* Understand geographic performance
* Identify top-performing products and categories

---

## 🏗️ Architecture

<p align="center">
  <img src="<ARCHITECTURE_DIAGRAM_URL>" alt="Architecture Diagram" width="800" />
</p>

### High-Level Flow

```
Google Sheets (CSV URLs)
        ↓
DuckDB Lake Views
        ↓
dbt Staging Models (incremental)
        ↓
dbt Marts (dimensions & facts)
        ↓
Apache Superset Dashboards
```

**Key Design Principles:**

* Clear separation of concerns
* Incremental transformations
* Single source of truth (DuckDB)
* BI layer decoupled from transformations

---

## 🧰 Tech Stack

| Layer           | Tool                    | Purpose                           |
| --------------- | ----------------------- | --------------------------------- |
| Warehouse       | **DuckDB**              | Analytical storage & query engine |
| Transformations | **dbt**                 | Data modeling & testing           |
| Orchestration   | **Mage**                | Pipeline execution & scheduling   |
| Visualization   | **Apache Superset**     | BI dashboards                     |
| Source          | **Google Sheets (CSV)** | External data source              |
| Language        | **Python / SQL**        | Core development                  |

---

## 🗂️ Repository Structure

```
GER-dbt-duckdb-mage-superset/
├── dev.duckdb
├── ge_dbt/                # dbt project (single source of truth)
│   ├── models/
│   │   ├── lake_view/
│   │   ├── staging/
│   │   └── marts/
│   ├── macros/
│   └── tests/
├── ge_mage/               # Mage orchestration
│   ├── pipelines/
│   └── mage_data/
├── logs/
│   └── dbt/               # dbt run logs (timestamped)
├── ge_venv/
└── README.md
```

---

## 🧪 Data Modeling (dbt)

### Layered Modeling Strategy

| Layer   | Schema    | Description                               |
| ------- | --------- | ----------------------------------------- |
| Lake    | `lake`    | DuckDB views over external CSV sources    |
| Staging | `staging` | Clean, incremental, source-aligned tables |
| Marts   | `marts`   | Business-ready dimensions & facts         |

---

### 🔹 Staging Models (`stg_`)

Staging models serve as the **contract layer** between raw data and analytics.

**Characteristics:**

* One model per source
* Incremental materialization
* No joins or aggregations
* Type casting and cleanup only

**Models:**

* `stg_customers`
* `stg_products`
* `stg_stores`
* `stg_exchange_rates`
* `stg_sales`

---

### 🔹 Mart Models (`dim_`, `fct_`)

Mart models are **business-facing** and optimized for BI.

**Dimensions:**

* `dim_customers`
* `dim_products`
* `dim_stores`

**Fact:**

* `fct_sales`

`fct_sales` is an **incremental append-only fact table** enriched with:

* Dimensional attributes
* Cost & revenue metrics
* Currency conversion

---

## 🔁 Orchestration (Mage)

Mage is used purely as an **orchestration engine**, not for transformations.

### Pipeline Overview

<p align="center">
  <img src="<PIPELINE_RUN_IMAGE_URL>" alt="Mage Pipeline Run" width="800" />
</p>

```
create_lake_views
        ↓
dbt_run_models
        ↓
dbt_test_models
```

### Key Orchestration Decisions

* dbt is executed via **Python subprocess** for stability
* Mage DBT UI blocks were intentionally avoided (known v0.9.x issues)
* Each dbt run writes full logs to `logs/dbt/`
* Mage UI shows clean, per-model execution status

---

## 📊 Visualization (Apache Superset)

Apache Superset serves as the **BI and dashboard layer**.

<p align="center">
  <img src="<SUPERSET_DASHBOARD_IMAGE_URL>" alt="Superset Dashboard" width="800" />
</p>

### Superset Highlights

* Connected directly to `dev.duckdb`
* Only mart-level models registered as datasets
* Centralized metric definitions
* Interactive filters and drill-downs

### Dashboard Components

* KPI Tiles (Revenue, Cost, Profit, Units Sold)
* Time-series sales trends
* Country-level performance
* Top products by revenue
* Global filters (date, geography, category)

### Key Insight Example

> **The United States accounts for the majority of total revenue, while desktop products dominate top-selling SKUs across categories.**

---

## ⚙️ How to Run Locally

### 1️⃣ Clone the Repository

```bash
git clone <REPO_URL>
cd GER-dbt-duckdb-mage-superset
```

### 2️⃣ Set Up Python Environment

```bash
python3 -m venv ge_venv
source ge_venv/bin/activate
pip install -r requirements.txt
```

### 3️⃣ Run dbt

```bash
cd ge_dbt
dbt run
dbt test
```

### 4️⃣ Start Mage

```bash
mage start ge_mage
```

### 5️⃣ Start Superset

```bash
superset run -p 8088 --with-threads
```

---

## 🧩 Design Decisions & Trade-offs

* **DuckDB** chosen for simplicity and analytical performance
* **Incremental models** used to simulate production-scale pipelines
* **Subprocess dbt execution** chosen for reliability over UI coupling
* **Wide fact table** optimized for BI consumption
* **Local-first architecture** for reproducibility

---

## 🚧 Future Enhancements

* Slowly Changing Dimensions (SCD Type 2)
* dbt Snapshots
* CI/CD for dbt
* Superset alerting
* Parquet lake storage
* Dockerized deployment

---

## 🎯 Outcome

This project demonstrates:

* Ownership of the **entire analytics lifecycle**
* Strong data modeling fundamentals
* Production-aligned tooling decisions
* Clear separation between ingestion, modeling, orchestration, and BI

It serves as a **portfolio-quality reference implementation** of modern analytics engineering.

---

## 📌 Author

**Divine Sam**
Analytics Engineer

---

> ⭐ If you found this project useful, feel free to star the repository and explore the implementation details.
