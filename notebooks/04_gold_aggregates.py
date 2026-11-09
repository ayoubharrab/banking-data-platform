# Databricks notebook source
G = "banking.gold"

def build(table: str, sql: str):
    spark.sql(f"CREATE OR REPLACE TABLE {G}.{table} AS {sql}")
    print(f"gold.{table}: {spark.table(f'{G}.{table}').count():,} rows")

# COMMAND ----------

build("agg_account_monthly", f"""
  SELECT t.account_id,
         d.year_month,
         CAST(date_trunc('month', MIN(d.date)) AS DATE)                AS month_start,
         COUNT(*)                                                     AS n_transactions,
         SUM(CASE WHEN t.trans_type = 'credit' THEN t.amount ELSE 0 END) AS total_credit,
         SUM(CASE WHEN t.trans_type = 'debit'  THEN t.amount ELSE 0 END) AS total_debit,
         SUM(t.signed_amount)                                         AS net_flow,
         max_by(t.balance, struct(t.date_key, t.trans_id))            AS end_of_month_balance
  FROM {G}.fact_transactions t
  JOIN {G}.dim_date d USING (date_key)
  GROUP BY t.account_id, d.year_month
""")

# COMMAND ----------

build("agg_client_profile", f"""
  WITH tx AS (
    SELECT client_id, COUNT(*) AS n_transactions, SUM(amount) AS total_volume,
           ROUND(AVG(balance), 2) AS avg_balance
    FROM {G}.fact_transactions GROUP BY client_id),
  ln AS (
    SELECT client_id, COUNT(*) AS n_loans, SUM(amount) AS loan_amount,
           MAX(CAST(is_default AS INT)) = 1 AS has_default
    FROM {G}.fact_loans GROUP BY client_id)
  SELECT c.client_id, c.gender, c.age, c.age_band,
         c.district_id, dd.district_name, dd.region,
         dp.disp_type                       AS role,          -- owner / disponent
         dp.account_id,
         COALESCE(cd.card_type, 'none')     AS card_type,
         COALESCE(tx.n_transactions, 0)     AS n_transactions,
         COALESCE(tx.total_volume, 0)       AS total_volume,
         tx.avg_balance,
         COALESCE(ln.n_loans, 0)            AS n_loans,
         COALESCE(ln.loan_amount, 0)        AS loan_amount,
         COALESCE(ln.n_loans, 0) > 0        AS has_loan,
         COALESCE(ln.has_default, false)    AS has_default
  FROM {G}.dim_client c
  JOIN banking.silver.disp dp      ON dp.client_id = c.client_id
  LEFT JOIN banking.silver.card cd ON cd.disp_id = dp.disp_id
  LEFT JOIN {G}.dim_district dd    ON dd.district_id = c.district_id
  LEFT JOIN tx ON tx.client_id = c.client_id      -- transactions are attributed to the account owner
  LEFT JOIN ln ON ln.client_id = c.client_id
""")

# COMMAND ----------

build("agg_district_risk", f"""
  WITH l AS (
    SELECT district_id, COUNT(*) AS n_loans, SUM(CAST(is_default AS INT)) AS n_defaults,
           SUM(amount) AS total_loan_amount, ROUND(AVG(amount), 2) AS avg_loan_amount
    FROM {G}.fact_loans GROUP BY district_id),
  a AS (SELECT district_id, COUNT(*) AS n_accounts FROM {G}.dim_account GROUP BY district_id),
  c AS (SELECT district_id, COUNT(*) AS n_clients  FROM {G}.dim_client  GROUP BY district_id)
  SELECT d.district_id, d.district_name, d.region, d.inhabitants, d.urban_ratio,
         d.avg_salary, d.unemployment_96, d.crimes_96,
         COALESCE(c.n_clients, 0)  AS n_clients,
         COALESCE(a.n_accounts, 0) AS n_accounts,
         COALESCE(l.n_loans, 0)    AS n_loans,
         COALESCE(l.n_defaults, 0) AS n_defaults,
         ROUND(100 * l.n_defaults / NULLIF(l.n_loans, 0), 1) AS default_rate_pct,
         COALESCE(l.total_loan_amount, 0) AS total_loan_amount,
         l.avg_loan_amount
  FROM {G}.dim_district d
  LEFT JOIN l USING (district_id)
  LEFT JOIN a USING (district_id)
  LEFT JOIN c USING (district_id)
""")

# COMMAND ----------

build("agg_monthly_kpis", f"""
  WITH m  AS (SELECT DISTINCT year_month FROM {G}.dim_date),
  tx AS (
    SELECT d.year_month, COUNT(*) AS n_transactions,
           SUM(CASE WHEN trans_type = 'credit' THEN amount ELSE 0 END) AS total_credit,
           SUM(CASE WHEN trans_type = 'debit'  THEN amount ELSE 0 END) AS total_debit,
           COUNT(DISTINCT account_id) AS active_accounts
    FROM {G}.fact_transactions t JOIN {G}.dim_date d USING (date_key) GROUP BY 1),
  acc AS (SELECT date_format(open_date, 'yyyy-MM') AS year_month, COUNT(*) AS new_accounts
          FROM {G}.dim_account GROUP BY 1),
  ln AS (
    SELECT d.year_month, COUNT(*) AS new_loans, SUM(amount) AS new_loan_amount
    FROM {G}.fact_loans l JOIN {G}.dim_date d USING (date_key) GROUP BY 1)
  SELECT m.year_month,
         COALESCE(tx.n_transactions, 0)  AS n_transactions,
         COALESCE(tx.total_credit, 0)    AS total_credit,
         COALESCE(tx.total_debit, 0)     AS total_debit,
         COALESCE(tx.active_accounts, 0) AS active_accounts,
         COALESCE(acc.new_accounts, 0)   AS new_accounts,
         COALESCE(ln.new_loans, 0)       AS new_loans,
         COALESCE(ln.new_loan_amount, 0) AS new_loan_amount
  FROM m
  LEFT JOIN tx  USING (year_month)
  LEFT JOIN acc USING (year_month)
  LEFT JOIN ln  USING (year_month)
""")

# COMMAND ----------

build("agg_monthly_kpis", f"""
  WITH m  AS (SELECT DISTINCT year_month FROM {G}.dim_date),
  tx AS (
    SELECT d.year_month, COUNT(*) AS n_transactions,
           SUM(CASE WHEN trans_type = 'credit' THEN amount ELSE 0 END) AS total_credit,
           SUM(CASE WHEN trans_type = 'debit'  THEN amount ELSE 0 END) AS total_debit,
           COUNT(DISTINCT account_id) AS active_accounts
    FROM {G}.fact_transactions t JOIN {G}.dim_date d USING (date_key) GROUP BY 1),
  acc AS (SELECT date_format(open_date, 'yyyy-MM') AS year_month, COUNT(*) AS new_accounts
          FROM {G}.dim_account GROUP BY 1),
  ln AS (
    SELECT d.year_month, COUNT(*) AS new_loans, SUM(amount) AS new_loan_amount
    FROM {G}.fact_loans l JOIN {G}.dim_date d USING (date_key) GROUP BY 1)
  SELECT m.year_month,
         COALESCE(tx.n_transactions, 0)  AS n_transactions,
         COALESCE(tx.total_credit, 0)    AS total_credit,
         COALESCE(tx.total_debit, 0)     AS total_debit,
         COALESCE(tx.active_accounts, 0) AS active_accounts,
         COALESCE(acc.new_accounts, 0)   AS new_accounts,
         COALESCE(ln.new_loans, 0)       AS new_loans,
         COALESCE(ln.new_loan_amount, 0) AS new_loan_amount
  FROM m
  LEFT JOIN tx  USING (year_month)
  LEFT JOIN acc USING (year_month)
  LEFT JOIN ln  USING (year_month)
""")

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {G}.fact_transactions ZORDER BY (account_id, date_key)"))

display(spark.sql(f"""
  SELECT 'agg_client_profile' AS t, COUNT(*) AS n, SUM(CAST(has_default AS INT)) AS check_value FROM {G}.agg_client_profile
  UNION ALL SELECT 'agg_district_risk', COUNT(*), SUM(n_loans)        FROM {G}.agg_district_risk
  UNION ALL SELECT 'agg_monthly_kpis',  COUNT(*), SUM(n_transactions) FROM {G}.agg_monthly_kpis
  UNION ALL SELECT 'agg_account_monthly', COUNT(*), SUM(n_transactions) FROM {G}.agg_account_monthly
"""))
display(spark.sql(f"SELECT * FROM {G}.agg_monthly_kpis ORDER BY year_month DESC LIMIT 6"))