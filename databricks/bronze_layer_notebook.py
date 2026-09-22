# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Dynamic, Config-Driven, Incremental Ingestion
# MAGIC
# MAGIC ### What this notebook does
# MAGIC This notebook ingests raw source files (CSV, in this example) into **Bronze** Delta
# MAGIC tables. It is intentionally written so that **nothing about a specific table is
# MAGIC hard-coded** in the PySpark logic:
# MAGIC
# MAGIC - Column names/types, file paths, file format, primary key, partition column, etc.
# MAGIC   all come from a **JSON config file** (`schema/sales_orders_schema.json` in the repo).
# MAGIC - The notebook loops over **every** JSON config file it finds in the `schema/` folder,
# MAGIC   so adding a brand-new source table to the pipeline means *adding a new JSON file*,
# MAGIC   not writing new PySpark code.
# MAGIC
# MAGIC ### How incremental load is implemented
# MAGIC We use **Databricks Auto Loader** (`cloudFiles` format). Auto Loader keeps track,
# MAGIC in a **checkpoint location**, of exactly which source files it has already processed.
# MAGIC Every time this notebook runs:
# MAGIC 1. Auto Loader looks at the landing folder.
# MAGIC 2. It compares what's there against what the checkpoint says was already ingested.
# MAGIC 3. It reads and processes **only the new files** — nothing already-loaded is
# MAGIC    re-read or duplicated.
# MAGIC
# MAGIC This is the standard, production-grade way to do incremental (a.k.a. "delta") loads
# MAGIC into a Bronze layer in Databricks, and it needs no manual "last loaded timestamp"
# MAGIC bookkeeping from us — the checkpoint does that automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Widgets (dynamic runtime parameters)
# MAGIC Widgets let you change behaviour **without editing code** — you set them at the top
# MAGIC of the notebook UI, or pass them in when this notebook is triggered by a Databricks
# MAGIC Job/Workflow. This is what makes the notebook "dynamic": the same code runs for
# MAGIC dev/test/prod, or for different repo checkouts, just by changing widget values.

# COMMAND ----------

# dbutils is a Databricks-only helper object (auto-available in every notebook,
# you never import it yourself). dbutils.widgets creates the little input boxes
# you see at the top of a Databricks notebook.

dbutils.widgets.text(
    "repo_base_path",
    "/Workspace/Repos/<your-username>/medallion-bronze-demo",
    "1. Repo base path (where you cloned the GitHub repo in Databricks)",
)
dbutils.widgets.text(
    "schema_config_folder",
    "schema",
    "2. Folder (relative to repo_base_path) containing the *_schema.json config files",
)
dbutils.widgets.text(
    "catalog",
    "hive_metastore",
    "3. Unity Catalog catalog name (or 'hive_metastore' if you're not using UC)",
)
dbutils.widgets.text(
    "bronze_database",
    "bronze",
    "4. Database/schema name that will hold the Bronze Delta tables",
)
dbutils.widgets.dropdown(
    "environment", "dev", ["dev", "test", "prod"], "5. Environment (used to namespace paths)"
)

# .get() reads the CURRENT value typed into the widget box at run time.
repo_base_path = dbutils.widgets.get("repo_base_path")
schema_config_folder = dbutils.widgets.get("schema_config_folder")
catalog = dbutils.widgets.get("catalog")
bronze_database = dbutils.widgets.get("bronze_database")
environment = dbutils.widgets.get("environment")

print(f"repo_base_path       = {repo_base_path}")
print(f"schema_config_folder = {schema_config_folder}")
print(f"catalog               = {catalog}")
print(f"bronze_database       = {bronze_database}")
print(f"environment           = {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Imports

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
)

# `spark` is already created for you in every Databricks notebook — no need to build
# a SparkSession yourself the way you would in a plain PySpark script.
spark: SparkSession = spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Turn our JSON schema config into a real PySpark schema
# MAGIC
# MAGIC Our JSON files describe columns with simple string type names like `"string"`,
# MAGIC `"integer"`, `"double"`, `"date"`, `"timestamp"`. PySpark needs actual `DataType`
# MAGIC objects (`StringType()`, `IntegerType()`, ...). This small dictionary + function
# MAGIC is the "translator" between the two, and it's the key piece that makes the whole
# MAGIC notebook schema-agnostic: whatever columns are listed in the JSON, this builds a
# MAGIC matching `StructType` automatically.

# COMMAND ----------

# Maps the type names we use in our JSON config files to PySpark's actual type classes.
# Extend this dictionary if you introduce a new type (e.g. "long", "boolean") in a
# future schema config — you do NOT need to touch any code below this cell.
JSON_TYPE_TO_SPARK_TYPE = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def build_spark_schema(columns_config: list) -> StructType:
    """
    Convert the 'columns' list from our JSON config into a PySpark StructType.

    Example input (one element of the list):
        {"name": "order_id", "type": "string", "nullable": false, "description": "..."}

    This is what lets us hand Auto Loader an EXPLICIT schema (best practice for
    Bronze — see note in Step 5) without ever writing `StructField(...)` by hand
    for a specific table.
    """
    fields = []
    for col in columns_config:
        spark_type = JSON_TYPE_TO_SPARK_TYPE.get(col["type"].lower())
        if spark_type is None:
            raise ValueError(
                f"Unknown type '{col['type']}' for column '{col['name']}'. "
                f"Add it to JSON_TYPE_TO_SPARK_TYPE."
            )
        fields.append(
            StructField(col["name"], spark_type, nullable=col.get("nullable", True))
        )
    return StructType(fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Discover every schema config file in the repo
# MAGIC
# MAGIC Instead of pointing the notebook at ONE hard-coded JSON file, we scan the whole
# MAGIC `schema/` folder in the repo. **Every `*.json` file we find becomes one more table
# MAGIC that this notebook ingests**, in the same run, using the exact same logic below.
# MAGIC That's what makes this "dynamic for all configuration": scaling from 1 table to
# MAGIC 50 tables is just adding 49 more JSON files.

# COMMAND ----------

def discover_schema_configs(repo_base_path: str, schema_folder: str) -> list:
    """Return a list of full paths to every *.json file inside <repo>/<schema_folder>."""
    full_folder_path = os.path.join(repo_base_path, schema_folder)
    json_files = [
        os.path.join(full_folder_path, f)
        for f in os.listdir(full_folder_path)
        if f.endswith(".json")
    ]
    if not json_files:
        raise FileNotFoundError(f"No JSON schema config files found in {full_folder_path}")
    return json_files


def load_config(config_path: str) -> dict:
    """Read one JSON schema config file from disk into a Python dict."""
    with open(config_path, "r") as f:
        return json.load(f)


config_paths = discover_schema_configs(repo_base_path, schema_config_folder)
print(f"Found {len(config_paths)} schema config(s):")
for p in config_paths:
    print(f"  - {p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — The core ingestion function (runs once per table)
# MAGIC
# MAGIC This is the heart of the notebook. Everything inside it reads from `config`
# MAGIC (i.e. from the JSON file) — there is not a single hard-coded column name, path,
# MAGIC or table name in here. Read the inline comments for what each block does and why.

# COMMAND ----------

def run_bronze_ingestion(config: dict, catalog: str, bronze_database: str, environment: str) -> None:
    """
    Ingests one source table into Bronze, incrementally, using Auto Loader.

    Parameters
    ----------
    config : dict
        The parsed JSON schema config for this table (table_name, paths, columns, ...).
    catalog, bronze_database : str
        Where the Bronze Delta TABLE gets registered (catalog.database.table_name).
    environment : str
        Used only to namespace the checkpoint folder, so dev/test/prod runs never
        collide with each other.
    """

    table_name = config["table_name"]
    print(f"\n=== Starting Bronze ingestion for: {table_name} ===")

    # ------------------------------------------------------------------
    # 5a. Build the explicit schema for this source file from the config.
    #
    # Why explicit and not `inferSchema=True`? Schema inference means Spark
    # has to first SCAN the data to guess types — slow, and it can guess
    # wrong (e.g. a numeric-looking ID column gets read as a number and
    # loses leading zeros). Giving Auto Loader the schema explicitly is the
    # recommended Bronze-layer practice: fast, and 100% predictable types.
    # ------------------------------------------------------------------
    explicit_schema = build_spark_schema(config["columns"])

    # ------------------------------------------------------------------
    # 5b. Read options for the source file format, taken from the config's
    # "options" block (e.g. header row present, delimiter character).
    # Using **config["options"] means any option you add to the JSON later
    # (e.g. "quote", "escape") is automatically passed through to Spark.
    # ------------------------------------------------------------------
    reader_options = config.get("options", {})

    # ------------------------------------------------------------------
    # 5c. THIS is the incremental-load engine: Auto Loader ("cloudFiles").
    #
    # - format("cloudFiles")            -> tells Spark to use Auto Loader.
    # - cloudFiles.format                -> the underlying file format (csv/json/parquet...).
    # - cloudFiles.schemaLocation         -> where Auto Loader stores schema info between runs.
    # - .schema(explicit_schema)         -> the schema we built in step 5a.
    # - .load(landing_path)              -> the folder being watched for new files.
    #
    # spark.readStream (not spark.read) is what makes this a STREAMING read.
    # Combined with the checkpoint in the writer (5e), Spark guarantees each
    # source file is processed EXACTLY ONCE across all runs, even if this
    # notebook is re-run many times or the job restarts mid-way.
    # ------------------------------------------------------------------
    raw_stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", config["file_format"])
        .option("cloudFiles.schemaLocation", config["checkpoint_path"] + "/_schema")
        .options(**reader_options)
        .schema(explicit_schema)
        .load(config["landing_path"])
    )

    # ------------------------------------------------------------------
    # 5d. Add Bronze "metadata" columns.
    #
    # Bronze layer convention: never modify/clean the business data itself,
    # but DO add a few audit columns so every row can be traced back to
    # exactly which file it came from and when it was loaded. This is
    # invaluable for debugging and for reprocessing later if needed.
    # ------------------------------------------------------------------
    bronze_df = (
        raw_stream_df
        .withColumn("_source_file", F.col("_metadata.file_path"))       # which file this row came from
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())              # when WE loaded it
        .withColumn("_source_system", F.lit(config["source_system"]))   # which system it came from
    )

    # ------------------------------------------------------------------
    # 5e. Write the stream out as a Delta table, incrementally.
    #
    # - .trigger(availableNow=True)  -> process everything currently new, then
    #   STOP (instead of running forever). This is the right trigger for a
    #   notebook that's run on a schedule (e.g. an hourly Databricks Job) —
    #   each run does one incremental batch and finishes.
    # - .option("checkpointLocation", ...) -> THIS is what lets Spark remember,
    #   between separate runs of this notebook, which files were already
    #   processed. Delete this folder and Spark will reprocess everything
    #   from scratch, so never delete it accidentally in production.
    # - .partitionBy(...) -> physically splits the Delta files by the
    #   partition column(s) from the config (if any), which speeds up
    #   queries that filter on that column (e.g. WHERE order_date = ...).
    # - .toTable(...) -> registers/writes to a managed Delta table named
    #   catalog.database.table_name, built dynamically from our widgets +
    #   the table_name in the JSON config.
    # ------------------------------------------------------------------
    full_table_name = f"{catalog}.{bronze_database}.{table_name}"

    writer = (
        bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", config["checkpoint_path"])
        .option("mergeSchema", "true")  # tolerate new/extra columns appearing later
        .trigger(availableNow=True)
        .outputMode("append")  # Bronze is append-only: we never update/delete raw history
    )

    partition_cols = config.get("partition_by") or []
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    query = writer.toTable(full_table_name)
    query.awaitTermination()  # block until this run's available files are fully processed

    # ------------------------------------------------------------------
    # 5f. Small summary so you can see what happened in the notebook output.
    # ------------------------------------------------------------------
    row_count = spark.table(full_table_name).count()
    print(f"Bronze table '{full_table_name}' now has {row_count} total rows.")
    print(f"=== Finished Bronze ingestion for: {table_name} ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Driver: run the ingestion for every discovered config
# MAGIC
# MAGIC This loop is the only place that "wires everything together". It creates the
# MAGIC target database if needed, then calls `run_bronze_ingestion` once per JSON
# MAGIC config file found in Step 4. Add a 51st source table to the pipeline by adding
# MAGIC one more JSON file to the repo's `schema/` folder — nothing here needs to change.

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{bronze_database}")

for config_path in config_paths:
    config = load_config(config_path)

    # Namespace the checkpoint by environment so dev/test/prod (or different
    # developers) never step on each other's checkpoint/progress tracking.
    config["checkpoint_path"] = config["checkpoint_path"].rstrip("/") + f"/{environment}"

    run_bronze_ingestion(
        config=config,
        catalog=catalog,
        bronze_database=bronze_database,
        environment=environment,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Quick sanity check
# MAGIC Run this cell any time to see the latest state of a Bronze table, including the
# MAGIC audit columns added in Step 5d.

# COMMAND ----------

sample_config = load_config(config_paths[0])
sample_table = f"{catalog}.{bronze_database}.{sample_config['table_name']}"
display(spark.table(sample_table).orderBy(F.col("_ingested_at").desc()).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for next time you run this
# MAGIC - **To simulate new incoming data**: run `scripts/generate_sales_orders_data.py`
# MAGIC   again (it drops a brand-new timestamped file into the landing folder), then
# MAGIC   re-run this notebook (or Steps 6). Only the new file will be processed — you
# MAGIC   can re-run Step 7 to confirm the row count grew by exactly the new file's size.
# MAGIC - **To add a new source table**: drop a new `<table>_schema.json` file (same
# MAGIC   shape as `sales_orders_schema.json`) into the repo's `schema/` folder. No
# MAGIC   PySpark code changes needed.
# MAGIC - **To schedule this incrementally in production**: create a Databricks Job
# MAGIC   pointing at this notebook, on whatever schedule matches how often new files
# MAGIC   land (hourly/daily). `trigger(availableNow=True)` means each scheduled run
# MAGIC   processes whatever is new and then stops, ready for the next run.
