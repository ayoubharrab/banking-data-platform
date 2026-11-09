# Databricks notebook source
from pyspark.sql import functions as F

def yymmdd_to_date(col_name: str):
    return F.to_date(F.concat(F.lit("19"), F.substring(F.col(col_name), 1, 6)), "yyyyMMdd")

def save_silver(df, table: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"banking.silver.`{table}`")
    print(f"silver.{table}: {spark.table(f'banking.silver.`{table}`').count():,} rows")

# COMMAND ----------

names = ["district_id", "district_name", "region", "inhabitants",
         "munic_lt_500", "munic_500_1999", "munic_2000_9999", "munic_ge_10000",
         "n_cities", "urban_ratio", "avg_salary",
         "unemployment_95", "unemployment_96", "entrepreneurs_per_1000",
         "crimes_95", "crimes_96"]
types = ["INT", "STRING", "STRING", "INT", "INT", "INT", "INT", "INT", "INT",
         "DOUBLE", "INT", "DOUBLE", "DOUBLE", "INT", "INT", "INT"]

bronze = spark.table("banking.bronze.district")
src_cols = bronze.columns[:16]          

district = bronze.select([
    F.expr(f"try_cast(`{src}` AS {t})").alias(n)   
    for src, n, t in zip(src_cols, names, types)
])
save_silver(district, "district")

# COMMAND ----------

freq_map = {"POPLATEK MESICNE": "monthly",
            "POPLATEK TYDNE": "weekly",
            "POPLATEK PO OBRATU": "after_transaction"}
freq_expr = F.create_map([F.lit(x) for kv in freq_map.items() for x in kv])

account = (spark.table("banking.bronze.account")
           .select(F.col("account_id").cast("int"),
                   F.col("district_id").cast("int"),
                   freq_expr[F.col("frequency")].alias("frequency"),
                   yymmdd_to_date("date").alias("open_date")))
save_silver(account, "account")

# COMMAND ----------

display(spark.sql("""
  SELECT 'district' AS t, COUNT(*) AS n,
         SUM(CASE WHEN unemployment_95 IS NULL OR crimes_95 IS NULL THEN 1 ELSE 0 END) AS nulls
  FROM banking.silver.district
  UNION ALL
  SELECT 'account', COUNT(*),
         SUM(CASE WHEN frequency IS NULL OR open_date IS NULL THEN 1 ELSE 0 END)
  FROM banking.silver.account
"""))
display(spark.sql("SELECT frequency, COUNT(*) n, MIN(open_date), MAX(open_date) FROM banking.silver.account GROUP BY 1"))

# COMMAND ----------

c = spark.table("banking.bronze.client")
mm = F.substring("birth_number", 3, 2).cast("int")        

client = c.select(
    F.col("client_id").cast("int"),
    F.col("district_id").cast("int"),
    F.when(mm > 50, "F").otherwise("M").alias("gender"),
    F.to_date(F.concat(F.lit("19"), F.substring("birth_number", 1, 2),
                       F.lpad(F.when(mm > 50, mm - 50).otherwise(mm).cast("string"), 2, "0"),
                       F.substring("birth_number", 5, 2)), "yyyyMMdd").alias("birth_date"))
save_silver(client, "client")

# COMMAND ----------

disp = (spark.table("banking.bronze.disp")
        .select(F.col("disp_id").cast("int"), F.col("client_id").cast("int"),
                F.col("account_id").cast("int"), F.lower("type").alias("disp_type")))
save_silver(disp, "disp")

card = (spark.table("banking.bronze.card")
        .select(F.col("card_id").cast("int"), F.col("disp_id").cast("int"),
                F.col("type").alias("card_type"),
                yymmdd_to_date("issued").alias("issued_date")))   
save_silver(card, "card")

# COMMAND ----------

status_map = {"A": "finished_ok", "B": "finished_default",
              "C": "running_ok",  "D": "running_in_debt"}
status_expr = F.create_map([F.lit(x) for kv in status_map.items() for x in kv])

loan = (spark.table("banking.bronze.loan")
        .select(F.col("loan_id").cast("int"), F.col("account_id").cast("int"),
                yymmdd_to_date("date").alias("loan_date"),
                F.col("amount").cast("decimal(12,2)"),
                F.col("duration").cast("int").alias("duration_months"),
                F.col("payments").cast("decimal(12,2)").alias("monthly_payment"),
                F.col("status"),
                status_expr[F.col("status")].alias("status_label"),
                F.col("status").isin("B", "D").alias("is_default")))
save_silver(loan, "loan")

# COMMAND ----------

display(spark.sql("SELECT gender, COUNT(*) n, MIN(birth_date), MAX(birth_date), SUM(CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END) null_dates FROM banking.silver.client GROUP BY 1"))
display(spark.sql("SELECT disp_type, COUNT(*) n FROM banking.silver.disp GROUP BY 1"))
display(spark.sql("SELECT card_type, COUNT(*) n, MIN(issued_date), MAX(issued_date) FROM banking.silver.card GROUP BY 1"))
display(spark.sql("SELECT status, status_label, COUNT(*) n, ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (), 1) pct FROM banking.silver.loan GROUP BY 1,2 ORDER BY 1"))

# COMMAND ----------

def czech_map(d):
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])

order_ks = czech_map({"POJISTNE": "insurance", "SIPO": "household",
                      "LEASING": "leasing", "UVER": "loan_payment"})

order = (spark.table("banking.bronze.`order`")
         .select(F.col("order_id").cast("int"), F.col("account_id").cast("int"),
                 F.col("bank_to"), F.col("account_to").cast("bigint"),
                 F.col("amount").cast("decimal(12,2)"),
                 F.coalesce(order_ks[F.trim("k_symbol")], F.lit("unknown")).alias("purpose")))
save_silver(order, "order")

# COMMAND ----------

t = spark.table("banking.bronze.trans")

op_map = czech_map({"VYBER KARTOU": "card_withdrawal", "VKLAD": "cash_deposit",
                    "PREVOD Z UCTU": "collection_other_bank", "VYBER": "cash_withdrawal",
                    "PREVOD NA UCET": "remittance_other_bank"})
ks_map = czech_map({"POJISTNE": "insurance", "SLUZBY": "statement_fee", "UROK": "interest_credited",
                    "SANKC. UROK": "sanction_interest", "SIPO": "household",
                    "DUCHOD": "pension", "UVER": "loan_payment"})

trans_type = (F.when(F.col("type") == "PRIJEM", "credit")
               .when(F.col("type").isin("VYDAJ", "VYBER"), "debit"))   
amount = F.col("amount").cast("decimal(12,2)")

trans = t.select(
    F.col("trans_id").cast("int"), F.col("account_id").cast("int"),
    yymmdd_to_date("date").alias("trans_date"),
    trans_type.alias("trans_type"),
    F.coalesce(op_map[F.col("operation")],
               F.when(F.trim("k_symbol") == "UROK", "interest_credited"),
               F.lit("unknown")).alias("operation"),                    
    amount.alias("amount"),
    F.when(trans_type == "debit", -amount).otherwise(amount).alias("signed_amount"),
    F.col("balance").cast("decimal(12,2)"),
    F.coalesce(ks_map[F.trim("k_symbol")], F.lit("unknown")).alias("purpose"),  
    F.nullif(F.trim("bank"), F.lit("")).alias("partner_bank"),
    F.expr("try_cast(try_cast(account AS DOUBLE) AS BIGINT)").alias("partner_account"))  
save_silver(trans, "trans")

# COMMAND ----------

display(spark.sql("SELECT purpose, COUNT(*) n, ROUND(SUM(amount)) total FROM banking.silver.`order` GROUP BY 1 ORDER BY n DESC"))
display(spark.sql("SELECT trans_type, operation, COUNT(*) n FROM banking.silver.trans GROUP BY 1,2 ORDER BY 1,3 DESC"))
display(spark.sql("""
  SELECT COUNT(*) n,
         SUM(CASE WHEN trans_type IS NULL THEN 1 ELSE 0 END) null_type,
         SUM(CASE WHEN trans_date IS NULL THEN 1 ELSE 0 END) null_date,
         SUM(CASE WHEN operation = 'unknown' THEN 1 ELSE 0 END) unknown_op,
         SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END) negative_balance,
         SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) zero_amount,
         MIN(trans_date), MAX(trans_date)
  FROM banking.silver.trans
"""))