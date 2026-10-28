# 🏦 Banking Data Platform — End-to-End Azure Data Engineering Project

An end-to-end data platform built on Microsoft Azure, using the **Berka banking dataset** (PKDD'99): raw CSV files are ingested, cleaned and modeled into a **star schema** following the **Medallion architecture** (Bronze → Silver → Gold), then served to a **Power BI** dashboard focused on credit risk and customer behavior.

> 🚧 **Work in progress** — this project is being built step by step. See the [roadmap](#-roadmap) below.

---

## 🎯 Goals

- Build a production-like data pipeline on Azure, from ingestion to reporting
- Apply data quality rules to a real, messy dataset (Czech labels, encoded fields, hidden nulls)
- Design a star schema ready for analytics
- Deliver a Power BI dashboard: portfolio overview, credit risk, customer behavior

---

## 🏗️ Architecture

```
Berka CSV files
      │
      ▼
Azure Data Factory ──► ADLS Gen2 (raw)
                            │
                            ▼
          Azure Databricks (PySpark + Delta Lake)
          Bronze ──► Silver ──► Gold (star schema)
                            │
                            ▼
                        Power BI

Governance: Unity Catalog · Secrets: Azure Key Vault · Versioning: GitHub
```

*(A detailed architecture diagram will be added in `docs/`.)*

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Storage | Azure Data Lake Storage Gen2 |
| Ingestion & orchestration | Azure Data Factory |
| Processing | Azure Databricks, PySpark |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Security | Azure Key Vault |
| Reporting | Power BI (DAX) |
| Exploration | Python, Pandas, Jupyter |

---

## 📊 Dataset

**Berka dataset** — real anonymized data from a Czech bank (1993–1998), released for the PKDD'99 Discovery Challenge.

| Table | Rows | Description |
|---|---:|---|
| account | 4,500 | Bank accounts |
| client | 5,369 | Customers |
| disp | 5,369 | Client ↔ account relationships (owner / disponent) |
| order | 6,471 | Standing payment orders |
| trans | 1,056,320 | Transactions |
| loan | 682 | Loans granted |
| card | 892 | Credit cards issued |
| district | 77 | Demographic data per district |

The raw data is **not versioned** in this repository. Download it (e.g. from Kaggle, search "Berka dataset") and place the 8 files in `data/raw/`.

---

## 🔍 Data Quality Findings

The exploratory analysis ([`notebooks/01_exploration.ipynb`](notebooks/01_exploration.ipynb)) uncovered several issues that drive the Silver layer transformations. Highlights:

- **Gender hidden in the birth number**: `birth_number` is `YYMMDD` with **month + 50 for women**
- **Disguised nulls**: ~51% of transaction categories are empty — half as `NaN`, half as blank strings invisible to `isna()`
- **A third value in a binary column**: `type` should be credit/debit, but 16,666 rows contain `VYBER` (withdrawal)
- **One `?` turning numeric columns into text** in the district table
- **Czech labels** everywhere, mapped to English during transformation

➡️ Full report: [`docs/data_quality.md`](docs/data_quality.md)

---

## ⭐ Data Model (Gold Layer)

Star schema with 2 fact tables and 4 dimensions:

```
                 dim_date
                    │
dim_client ── fact_transactions ── dim_account
                    │
               dim_district

        (fact_loans shares the same dimensions)
```

➡️ Full design: [`docs/star_schema.md`](docs/star_schema.md)

---

## 🗺️ Roadmap

- [x] Data exploration & data quality report
- [x] Star schema design
- [x] GitHub repository setup
- [ ] Azure resources: Resource Group, ADLS Gen2, Key Vault
- [ ] Databricks workspace & Unity Catalog
- [ ] Data Factory ingestion pipeline (→ raw)
- [ ] Bronze layer (Auto Loader)
- [ ] Silver layer (cleaning, translation, MERGE, quality checks)
- [ ] Gold layer (star schema + aggregates)
- [ ] End-to-end orchestration & lineage
- [ ] Power BI dashboard
- [ ] Bonus: daily stock price ingestion via API

---

## 📁 Repository Structure

```
banking-data-platform/
├── data/raw/          # Source CSV files (not versioned)
├── docs/              # Data quality report, star schema, architecture
├── notebooks/         # Exploration and Databricks notebooks
└── README.md
```

---

## 👤 Author

**Ayoub HARRAB** — Data Engineer
[LinkedIn](https://www.linkedin.com/in/ayoub-harrab/) · [GitHub](https://github.com/ayoubharrab)