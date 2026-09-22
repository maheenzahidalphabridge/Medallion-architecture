# ============================================================
# BRONZE LAYER - DYNAMIC INCREMENTAL LOAD
# ============================================================
# Beginner explanation:
# This notebook reads configuration files instead of hardcoding
# table names. The same code can process customers, products,
# orders, and order_items.
#
# Flow:
# Landing JSON -> Bronze Delta -> Metadata Control Table
#
# Incremental logic:
# 1. Read previous watermark from metadata.
# 2. Filter only new records.
# 3. Write new records.
# 4. Save new watermark.
# ============================================================

from pyspark.sql import functions as F
import json
from datetime import datetime


# -----------------------------
# Configuration file locations
# -----------------------------

dbutils.widgets.text(
    "schema_config_path",
    "/Workspace/Users/<username>/Medallion-architecture/config/schema.json"
)

dbutils.widgets.text(
    "bronze_config_path",
    "/Workspace/Users/<username>/Medallion-architecture/config/bronze_config.json"
)

schema_path = dbutils.widgets.get("schema_config_path")
config_path = dbutils.widgets.get("bronze_config_path")


# -----------------------------
# Load JSON configuration files
# -----------------------------

with open(schema_path) as f:
    schema_config = json.load(f)

with open(config_path) as f:
    bronze_config = json.load(f)


catalog = bronze_config["target"]["catalog"]
bronze_schema = bronze_config["target"]["schema"]
metadata_schema = bronze_config["metadata"]["schema"]
control_table = bronze_config["metadata"]["control_table"]
source_path = bronze_config["source"]["base_path"]


# -----------------------------
# Create schemas
# -----------------------------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{metadata_schema}")


# -----------------------------
# Create metadata control table
# -----------------------------
# This table remembers the last successful load.
# Example:
# orders -> last processed order_date
# -----------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.{control_table}
(
 table_name STRING,
 last_watermark STRING,
 last_run_timestamp TIMESTAMP,
 status STRING,
 records_processed BIGINT
)
USING DELTA
""")


def get_watermark(table_name):
    """
    Gets the previous watermark.
    If the table has never loaded,
    return None.
    """

    result = spark.sql(f"""
    SELECT last_watermark
    FROM {catalog}.{metadata_schema}.{control_table}
    WHERE table_name='{table_name}'
    ORDER BY last_run_timestamp DESC
    LIMIT 1
    """)

    rows = result.collect()

    return rows[0][0] if rows else None



def save_metadata(table_name, watermark, count, status):
    """
    Saves pipeline execution information.
    """

    df = spark.createDataFrame(
        [(table_name,str(watermark),datetime.now(),status,count)],
        [
            "table_name",
            "last_watermark",
            "last_run_timestamp",
            "status",
            "records_processed"
        ]
    )

    df.write.mode("append").saveAsTable(
        f"{catalog}.{metadata_schema}.{control_table}"
    )



def process_table(table_name, table_config):
    """
    Complete Bronze processing for one table.
    """

    print("Processing:", table_name)

    input_path = f"{source_path}/{table_name}"

    target = f"{catalog}.{bronze_schema}.{table_name}"


    # Read raw landing data
    df = spark.read.format("json").load(input_path)


    watermark_column = table_config["watermark_column"]

    old_watermark = get_watermark(table_name)


    # Incremental filter
    # First run loads everything.
    if old_watermark:
        df = df.filter(
            F.col(watermark_column) > F.lit(old_watermark)
        )


    count = df.count()

    print("New records:", count)


    if count == 0:
        save_metadata(table_name,old_watermark,0,"NO_DATA")
        return


    # Bronze audit columns
    df = (
        df
        .withColumn("_load_timestamp",F.current_timestamp())
        .withColumn("_source_table",F.lit(table_name))
    )


    # Write Delta Bronze table
    df.write.format("delta").mode("append").saveAsTable(target)


    new_watermark = df.select(
        F.max(watermark_column)
    ).collect()[0][0]


    save_metadata(
        table_name,
        new_watermark,
        count,
        "SUCCESS"
    )


# -----------------------------
# Run all enabled tables
# -----------------------------

for table_name, table_config in bronze_config["tables"].items():

    if table_config["enabled"]:
        process_table(table_name, table_config)
