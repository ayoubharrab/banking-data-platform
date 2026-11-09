# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def yymmdd_to_date(col_name: str):
    return F.to_date(F.concat(F.lit("19"), F.substring(F.col(col_name), 1, 6)), "yyyyMMdd")

def save_silver(df, table: str, key: str):
    """Upsert into Silver: insert new rows, update changed rows, skip identical rows."""
    full = f"banking.silver.`{table}`"
    src = (df.dropDuplicates([key])                     
             .withColumn("_updated_ts", F.current_timestamp()))

    if not spark.catalog.tableExists(full):              
        src.write.saveAsTable(full)
        print(f"silver.{table}: created, {spark.table(full).count():,} rows")
        return

    cols = [c for c in df.columns if c != key]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in cols)   

    (DeltaTable.forName(spark, full).alias("t")
        .merge(src.alias("s"), f"t.`{key}` = s.`{key}`")
        .whenMatchedUpdateAll(condition=changed)          
        .whenNotMatchedInsertAll()
        .execute())

    m = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["operationMetrics"]
    print(f"silver.{table}: inserted={m.get('numTargetRowsInserted')} "
          f"updated={m.get('numTargetRowsUpdated')} total={spark.table(full).count():,}")

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
save_silver(district, "district", "district_id")


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
save_silver(account,  "account",  "account_id")


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
save_silver(client,   "client",   "client_id")


# COMMAND ----------

disp = (spark.table("banking.bronze.disp")
        .select(F.col("disp_id").cast("int"), F.col("client_id").cast("int"),
                F.col("account_id").cast("int"), F.lower("type").alias("disp_type")))
save_silver(disp, "disp", "disp_id")

card = (spark.table("banking.bronze.card")
        .select(F.col("card_id").cast("int"), F.col("disp_id").cast("int"),
                F.col("type").alias("card_type"),
                yymmdd_to_date("issued").alias("issued_date")))   # "931107 00:00:00" -> first 6 chars
save_silver(card, "card", "card_id")

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
save_silver(loan,     "loan",     "loan_id")


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
save_silver(order,    "order",    "order_id")


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
save_silver(trans,    "trans",    "trans_id")

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

# COMMAND ----------

S = "banking.silver"

DQ_RULES = []

pks = {"district": "district_id", "account": "account_id", "client": "client_id", "disp": "disp_id",
       "card": "card_id", "loan": "loan_id", "order": "order_id", "trans": "trans_id"}
for t, k in pks.items():
    DQ_RULES.append((f"pk_unique_{t}",
        f"SELECT COUNT(*) - COUNT(DISTINCT {k}) + COUNT_IF({k} IS NULL) FROM {S}.`{t}`", "error"))

fks = [("account", "district_id", "district", "district_id"),
       ("client",  "district_id", "district", "district_id"),
       ("disp",    "client_id",   "client",   "client_id"),
       ("disp",    "account_id",  "account",  "account_id"),
       ("card",    "disp_id",     "disp",     "disp_id"),
       ("loan",    "account_id",  "account",  "account_id"),
       ("order",   "account_id",  "account",  "account_id"),
       ("trans",   "account_id",  "account",  "account_id")]
for c, ck, p, pk in fks:
    DQ_RULES.append((f"fk_{c}_{ck}",
        f"SELECT COUNT(*) FROM {S}.`{c}` c LEFT ANTI JOIN {S}.`{p}` p ON c.{ck} = p.{pk}", "error"))

DQ_RULES += [
    ("notnull_trans", f"SELECT COUNT_IF(trans_date IS NULL OR trans_type IS NULL OR amount IS NULL) FROM {S}.trans", "error"),
    ("notnull_loan",  f"SELECT COUNT_IF(loan_date IS NULL OR status_label IS NULL) FROM {S}.loan", "error"),
    ("notnull_client",f"SELECT COUNT_IF(birth_date IS NULL OR gender IS NULL) FROM {S}.client", "error"),
    ("one_owner_per_account",
        f"SELECT COUNT(*) FROM (SELECT account_id FROM {S}.disp WHERE disp_type = 'owner' GROUP BY 1 HAVING COUNT(*) <> 1)", "error"),
    ("warn_negative_balance", f"SELECT COUNT_IF(balance < 0) FROM {S}.trans", "warn"),
    ("warn_zero_amount",      f"SELECT COUNT_IF(amount = 0) FROM {S}.trans", "warn"),
    ("warn_district_missing_stats", f"SELECT COUNT_IF(unemployment_95 IS NULL OR crimes_95 IS NULL) FROM {S}.district", "warn"),
    ("warn_order_unknown_purpose",  f"SELECT COUNT_IF(purpose = 'unknown') FROM {S}.`order`", "warn"),
]
print(f"{len(DQ_RULES)} rules defined")

# COMMAND ----------

from datetime import datetime, timezone

run_ts = datetime.now(timezone.utc)
results = []
for name, sql, severity in DQ_RULES:
    bad = spark.sql(sql).first()[0]
    status = "PASS" if bad == 0 else ("FAIL" if severity == "error" else "WARN")
    results.append((run_ts, name, severity, int(bad), status))

res_df = spark.createDataFrame(results, "run_ts timestamp, rule string, severity string, bad_rows long, status string")
res_df.write.mode("append").saveAsTable(f"{S}._dq_results")      
display(res_df.orderBy("status", "rule"))

failed = [r[1] for r in results if r[4] == "FAIL"]
if failed:
    raise Exception(f"Data quality check failed: {failed}")      
print("All blocking checks passed")