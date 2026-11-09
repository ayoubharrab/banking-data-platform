# Databricks notebook source
from pyspark.sql import functions as F

S, G = "banking.silver", "banking.gold"
REF_DATE = "1999-01-01"   

def save_gold(df, table: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{table}")
    print(f"gold.{table}: {spark.table(f'{G}.{table}').count():,} rows")

# COMMAND ----------

dim_date = (spark.sql("SELECT explode(sequence(DATE'1993-01-01', DATE'1998-12-31', INTERVAL 1 DAY)) AS date")
    .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("date").alias("day"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend")))
save_gold(dim_date, "dim_date")

# COMMAND ----------

save_gold(spark.table(f"{S}.district").drop("_updated_ts"), "dim_district")

age = F.floor(F.months_between(F.lit(REF_DATE), "birth_date") / 12).cast("int")
dim_client = (spark.table(f"{S}.client")
    .select("client_id", "gender", "birth_date", "district_id",
            age.alias("age"),
            F.when(age < 25, "18-24").when(age < 35, "25-34").when(age < 45, "35-44")
             .when(age < 55, "45-54").when(age < 65, "55-64").otherwise("65+").alias("age_band")))
save_gold(dim_client, "dim_client")

# COMMAND ----------

disp = spark.table(f"{S}.disp")
card = spark.table(f"{S}.card")

owner = disp.filter("disp_type = 'owner'").select("account_id", F.col("client_id").alias("owner_client_id"), "disp_id")
has_disponent = disp.filter("disp_type = 'disponent'").select("account_id").distinct().withColumn("has_disponent", F.lit(True))

dim_account = (spark.table(f"{S}.account")
    .join(owner, "account_id", "left")
    .join(card.select("disp_id", "card_type"), "disp_id", "left")      
    .join(has_disponent, "account_id", "left")
    .select("account_id", "district_id", "frequency", "open_date", "owner_client_id",
            F.coalesce("has_disponent", F.lit(False)).alias("has_disponent"),
            F.coalesce("card_type", F.lit("none")).alias("card_type")))
save_gold(dim_account, "dim_account")

# COMMAND ----------

acc = spark.table(f"{G}.dim_account").select("account_id", "district_id", F.col("owner_client_id").alias("client_id"))

fact_transactions = (spark.table(f"{S}.trans")
    .join(acc, "account_id", "left")
    .select("trans_id", "account_id", "client_id", "district_id",
            F.date_format("trans_date", "yyyyMMdd").cast("int").alias("date_key"),
            "trans_type", "operation", "purpose",
            "amount", "signed_amount", "balance",
            "partner_bank", "partner_account"))
save_gold(fact_transactions, "fact_transactions")

# COMMAND ----------

fact_loans = (spark.table(f"{S}.loan")
    .join(acc, "account_id", "left")
    .select("loan_id", "account_id", "client_id", "district_id",
            F.date_format("loan_date", "yyyyMMdd").cast("int").alias("date_key"),
            "amount", "duration_months", "monthly_payment",
            "status", "status_label", "is_default"))
save_gold(fact_loans, "fact_loans")

# COMMAND ----------

checks = [("fact_transactions", "account_id",  "dim_account",  "account_id"),
          ("fact_transactions", "client_id",   "dim_client",   "client_id"),
          ("fact_transactions", "district_id", "dim_district", "district_id"),
          ("fact_transactions", "date_key",    "dim_date",     "date_key"),
          ("fact_loans",        "account_id",  "dim_account",  "account_id"),
          ("fact_loans",        "client_id",   "dim_client",   "client_id"),
          ("fact_loans",        "district_id", "dim_district", "district_id"),
          ("fact_loans",        "date_key",    "dim_date",     "date_key")]

for f, fk, d, pk in checks:
    orphans = spark.sql(f"SELECT COUNT(*) FROM {G}.{f} x LEFT ANTI JOIN {G}.{d} y ON x.{fk} = y.{pk}").first()[0]
    print(f"{f}.{fk:<12} -> {d:<13} orphans={orphans}  {'OK' if orphans == 0 else 'KO'}")

# COMMAND ----------

display(spark.sql(f"""
  SELECT d.region,
         COUNT(*)                                   AS n_loans,
         SUM(CAST(l.is_default AS INT))             AS n_defaults,
         ROUND(100 * AVG(CAST(l.is_default AS INT)), 1) AS default_rate_pct,
         ROUND(SUM(l.amount) / 1e6, 1)              AS total_amount_m
  FROM {G}.fact_loans l
  JOIN {G}.dim_district d USING (district_id)
  GROUP BY d.region
  ORDER BY default_rate_pct DESC
"""))