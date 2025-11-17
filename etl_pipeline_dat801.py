#!/usr/bin/env python3
"""
etl_pipeline_dat801.py

Databricks-friendly ETL + tiny ML demo script.

Usage (Databricks notebook / job):
    - Upload data.csv via Data -> Add Data -> Upload (goes to dbfs:/FileStore/tables/)
    - Or mount an Azure storage container and set MOUNT_PATH to your mount point.
    - Run this script as a Databricks job or import/call functions inside a notebook cell.

DO NOT hard-code secrets. Use Databricks Secret Scopes for sensitive values.
"""

import os
import sys
import logging
from datetime import datetime

# Basic imports for Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# sklearn for tiny demo training (only for small datasets)
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_dat801")


def get_spark():
    """Return a SparkSession (works inside Databricks or outside if spark is configured)."""
    try:
        spark = SparkSession.builder.getOrCreate()
        logger.info("SparkSession obtained.")
        return spark
    except Exception as e:
        logger.exception("Failed to get SparkSession.")
        raise


def dbfs_ls(path):
    """List DBFS path if running in Databricks and dbutils is available, else fallback to Spark filesystem."""
    try:
        # dbutils exists in Databricks notebook environment
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.fs.ls(path)
    except Exception:
        # Fallback using spark (works for dbfs:/ style in Databricks)
        spark = get_spark()
        try:
            return spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
        except Exception:
            return []


def path_exists(path):
    """Return True if path exists in DBFS (Databricks) or local FS."""
    try:
        # Try dbutils if available
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        # dbutils.fs.ls throws if path missing
        dbutils.fs.ls(path)
        return True
    except Exception:
        # fallback: if path begins with dbfs: or file:, check local /dbfs/ mounted path
        if path.startswith("dbfs:/"):
            local_path = path.replace("dbfs:/", "/dbfs/")
            return os.path.exists(local_path)
        return os.path.exists(path)


def read_csv_spark(spark, input_path, schema=None, header=True, infer_schema=False):
    """Read a CSV into a Spark DataFrame. input_path supports dbfs:/ or /mnt/..."""
    logger.info(f"Reading CSV from: {input_path}")
    if infer_schema:
        df = spark.read.option("header", header).option("inferSchema", True).csv(input_path)
    else:
        df = spark.read.option("header", header).schema(schema).csv(input_path)
    logger.info(f"Read {df.count()} rows.")
    return df


def clean_transform(df):
    """Basic cleaning & transformations. Returns a cleaned DataFrame."""
    # Ensure timestamp column exists; adapt to your CSV column name
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp_ts", to_timestamp(col("timestamp")))
    else:
        # try common alternatives
        for cand in ("time", "ts", "date"):
            if cand in df.columns:
                df = df.withColumn("timestamp_ts", to_timestamp(col(cand)))
                break

    # Example: handle nulls in 'value' and 'category'
    if "value" in df.columns:
        df = df.withColumn("value", when(col("value").isNull(), lit(0.0)).otherwise(col("value")))

    if "category" in df.columns:
        df = df.withColumn("category", when(col("category").isNull(), lit("unknown")).otherwise(col("category")))

    # Add a processing timestamp
    df = df.withColumn("_processed_at", lit(datetime.utcnow().isoformat()))
    logger.info("Cleaning and basic transforms applied.")
    return df


def write_delta(df, output_path):
    """Write DataFrame to Delta format (overwrites existing)."""
    logger.info(f"Writing Delta to {output_path}")
    # ensure path exists for DBFS when using /dbfs/ prefix
    try:
        df.write.format("delta").mode("overwrite").save(output_path)
        logger.info("Delta write complete.")
    except Exception:
        # fallback: try writing to local /dbfs/ path if input is dbfs:
        if output_path.startswith("dbfs:/"):
            local_path = output_path.replace("dbfs:/", "/dbfs/")
            os.makedirs(local_path, exist_ok=True)
            df.write.format("delta").mode("overwrite").save(local_path)
            logger.info("Delta write complete to local /dbfs/ path.")
        else:
            raise


def tiny_model_train_save(df, model_output_path):
    """Train a tiny sklearn RandomForest on small dataset and save model to DBFS path."""
    # Minimal safety check
    if "value" not in df.columns:
        logger.warning("Column 'value' not present - skipping model training.")
        return

    # Convert to pandas (only safe for tiny datasets)
    logger.info("Converting to pandas for tiny model training (only for demo).")
    pdf = df.select("value", "category").toPandas()

    if pdf.shape[0] < 10:
        logger.warning("Dataset too small for meaningful training; proceeding for demo only.")

    # Simple encoding
    pdf["cat_idx"] = pdf["category"].astype("category").cat.codes
    X = pdf[["cat_idx"]]
    y = pdf["value"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=20, random_state=42)
    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
    logger.info(f"Trained RandomForest - test score: {score:.4f}")

    # Save model - if path begins with dbfs:/ convert to /dbfs/
    if model_output_path.startswith("dbfs:/"):
        local_model_path = model_output_path.replace("dbfs:/", "/dbfs/")
    else:
        local_model_path = model_output_path

    os.makedirs(os.path.dirname(local_model_path), exist_ok=True)
    joblib.dump(model, local_model_path)
    logger.info(f"Model saved to: {local_model_path}")


def main(input_path=None, mount_path=None):
    """Main ETL flow."""
    spark = get_spark()

    # Default paths (common Databricks upload location)
    # 1) If user uploaded via UI: dbfs:/FileStore/tables/data.csv
    default_dbfs_upload = "dbfs:/FileStore/tables/data.csv"

    # 2) If user mounted container at /mnt/stdat801siraj/data
    mounted_path = None
    if mount_path:
        mounted_path = os.path.join(mount_path.rstrip("/"), "data.csv")

    # determine which path exists
    chosen_path = None
    if input_path and path_exists(input_path):
        chosen_path = input_path
    elif mounted_path and path_exists(mounted_path):
        chosen_path = mounted_path
    elif path_exists(default_dbfs_upload):
        chosen_path = default_dbfs_upload
    else:
        logger.error("No input CSV found. Tried paths: input_path, mount_path/data.csv, dbfs:/FileStore/tables/data.csv")
        logger.info("If you uploaded via the UI, use the Data -> Add Data feature and then re-run.")
        sys.exit(1)

    # Define a simple schema (adjust to your actual CSV fields)
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("note", StringType(), True)
    ])

    df_raw = read_csv_spark(spark, chosen_path, schema=schema, header=True, infer_schema=False)
    df_clean = clean_transform(df_raw)

    # Output delta location
    # If input was dbfs:/... then write to dbfs:/FileStore/delta/processed/
    if chosen_path.startswith("dbfs:/"):
        delta_out = "dbfs:/FileStore/delta/processed"
    elif mount_path:
        # write under mount
        delta_out = os.path.join(mount_path.rstrip("/"), "delta", "processed")
    else:
        # fallback local /dbfs/
        delta_out = "dbfs:/FileStore/delta/processed"

    # Write delta - convert dbfs:/ path to the format Spark expects (Databricks handles dbfs:)
    write_delta(df_clean, delta_out)

    # Tiny model training and save artifact
    model_out_path = "dbfs:/FileStore/models/rf_model.pkl"
    tiny_model_train_save(df_clean, model_out_path)

    # Quick verification read
    try:
        df_verify = spark.read.format("delta").load(delta_out)
        logger.info(f"Verification: delta has {df_verify.count()} rows and columns: {df_verify.columns}")
    except Exception as e:
        logger.exception("Failed to read back delta dataset for verification.")

    logger.info("ETL pipeline completed successfully.")


if __name__ == "__main__":
    # Allow passing paths via environment variables or args (simple)
    # Usage examples:
    #   python etl_pipeline_dat801.py
    #   python etl_pipeline_dat801.py /dbfs/FileStore/tables/data.csv
    #   python etl_pipeline_dat801.py --mount /mnt/stdat801siraj/data
    import argparse

    parser = argparse.ArgumentParser(description="ETL pipeline for DAT801 demo (Databricks-friendly).")
    parser.add_argument("input", nargs="?", default=None, help="input CSV path (dbfs:/ or /mnt/ or local)")
    parser.add_argument("--mount", default=None, help="mount path root if using mounted storage (e.g. /mnt/stdat801siraj/data)")
    args = parser.parse_args()

    input_arg = args.input
    mount_arg = args.mount

    main(input_path=input_arg, mount_path=mount_arg)
