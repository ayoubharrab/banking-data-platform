# Databricks notebook source
STORAGE = "stbankingayoub"
RAW_PATH = f"abfss://raw@{STORAGE}.dfs.core.windows.net/berka"
CHECKPOINT_ROOT = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/_checkpoints"
CATALOG, SCHEMA = "banking", "bronze"

TABLES = ["account", "card", "client", "disp", "district", "loan", "order", "trans"]

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_bronze(table: str):
    checkpoint = f"{CHECKPOINT_ROOT}/{table}"
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
          .option("header", "true")
          .option("sep", ";")
          .option("pathGlobFilter", f"{table}.csv")   # only this table's file
          .load(f"{RAW_PATH}/*/")                      # every ingestion date folder
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))

    query = (df.writeStream
             .option("checkpointLocation", checkpoint)
             .trigger(availableNow=True)               # batch-style: process new files, then stop
             .toTable(f"{CATALOG}.{SCHEMA}.`{table}`"))  # backticks: "order" is a reserved word
    query.awaitTermination()
    print(f"{table}: done")

# COMMAND ----------

ingest_bronze("district")
display(spark.sql("SELECT COUNT(*) AS n, MIN(_source_file) FROM banking.bronze.district"))

# COMMAND ----------

for t in TABLES:
    ingest_bronze(t)

# COMMAND ----------

expected = {"account": 4500, "card": 892, "client": 5369, "disp": 5369,
            "district": 77, "loan": 682, "order": 6471, "trans": 1056320}

for t, exp in expected.items():
    n = spark.table(f"banking.bronze.`{t}`").count()
    print(f"{t:<10} {n:>9,}  {'OK' if n == exp else f'KO (expected {exp:,})'}")

# COMMAND ----------

for t in TABLES:
    df = spark.table(f"banking.bronze.`{t}`")
    rescued = df.filter("_rescued_data IS NOT NULL").count()
    types = {dt for c, dt in df.dtypes if not c.startswith("_")}
    print(f"{t:<10} cols={len(df.columns):>2}  types={types}  rescued={rescued}")