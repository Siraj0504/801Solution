📘 DAT801A – Cloud Data Pipeline & Machine Learning Project
Azure + Databricks End-to-End Pipeline

Author: smohammed24@students.icms.edu.au

Workspace: adb-1200451892143447.7.azuredatabricks.net

📌 Project Overview

This project implements a complete cloud-based data engineering and machine learning pipeline using Microsoft Azure and Azure Databricks.
The pipeline covers:

Data Ingestion (Azure Blob Storage, Event Hubs)

Data Preparation & ETL (Azure Databricks + PySpark)

Machine Learning Model Training (Scikit-Learn via Databricks)

Analytics & Delta Tables (Databricks Lakehouse)

Visualisation (Databricks Visualisations / Power BI Ready)

The objective was to ingest retail sales data, clean it, transform it, store it as Delta, and train a predictive model on the processed dataset.

🗂️ Architecture Summary
Stage 1 — Ingestion

Data uploaded via Azure Blob Storage (stdat801siraj)

Event Hub namespace: ehns-dat801

Event Hub: eh-dat801 (supports streaming ingestion)

Stage 2 — Preparation

Databricks Workspace: db-ws-dat801

Compute Cluster: db-cluster-dat801

CSV loaded from DBFS: /FileStore/tables/data.csv

ETL performed using PySpark:

Schema inference

Cleaning missing values

Type casting

Feature engineering (e.g., TotalPrice = Quantity × UnitPrice)

Timestamp parsing

Stage 3 — Machine Learning

A small regression model was trained to predict numeric values based on the processed features.

Metrics recorded in Databricks MLflow:

Sample shape: (5465, 3)
🏃 View run inquisitive-hawk-172:
https://adb-1200451892143447.7.azuredatabricks.net/ml/experiments/1056520141816893/runs/71f43efb91a645dfae6619a1e37893e5

🧪 View experiment:
https://adb-1200451892143447.7.azuredatabricks.net/ml/experiments/1056520141816893

Train R2 Score: 0.9356036875472284
Test R2 Score: 0.9671273157271898

Model saved to:
 /dbfs/FileStore/models/rf_demo.pkl

Stage 4 — Analytics-Ready Storage

Cleaned Delta table created:

Database: dat801_db
Table: sales_processed
Location: dbfs:/FileStore/delta/processed

Stage 5 — Visualisation

Databricks SQL Visualisations created for revenue analysis

Power BI connection supported (via Databricks Runtime + HTTP Path)

🚀 Features Implemented

✔ Azure Resource Group & Storage Provisioning
✔ Event Hubs Creation (Namespace + Hub)
✔ Databricks Workspace & Cluster Setup
✔ CSV Upload + DBFS Management
✔ PySpark ETL Pipeline
✔ Delta Table Creation
✔ Model Training & MLflow Logging
✔ Visualisations / BI Preparation
✔ Exported Notebook & .py Scripts

📁 Repository Structure
├── README.md
├── notebooks/
│   └── ETL_Pipeline_Dat801.ipynb
├── etl_pipeline_dat801.py        # PySpark ETL script
├── send_events.py                # Event Hub producer script
├── data/                         # (Optional) Local copy of sample data
└── models/
    └── rf_demo.pkl               # Saved ML model (in DBFS path)

📄 Key Code Snippets
Read CSV
df_raw = spark.read.option("header", True).option("inferSchema", True)\
    .csv("dbfs:/FileStore/tables/data.csv")

Write Delta Table
df_clean.write.format("delta").mode("overwrite")\
    .save("dbfs:/FileStore/delta/processed")

spark.sql("""
CREATE TABLE IF NOT EXISTS dat801_db.sales_processed
USING DELTA
LOCATION 'dbfs:/FileStore/delta/processed'
""")

Train Model
from sklearn.ensemble import RandomForestRegressor
import joblib

model = RandomForestRegressor(n_estimators=50, random_state=42)
model.fit(X_train, y_train)

joblib.dump(model, "/dbfs/FileStore/models/rf_demo.pkl")

📊 Results Summary
Metric	Value
Sample Size	5465 rows
Train R² Score	0.9356
Test R² Score	0.9671
Model Output	/dbfs/FileStore/models/rf_demo.pkl
MLflow Tracking	Enabled

The model demonstrates strong predictive performance, indicating the dataset was cleaned and transformed successfully.

🧪 How to Run the Project
Using Databricks Notebook

Upload data.csv → Workspace / FileStore

Import notebook ETL_Pipeline_Dat801.ipynb

Attach to cluster db-cluster-dat801

Run all cells

Validate:

Delta table created

MLflow run logged

Model saved to DBFS

Using Script (etl_pipeline_dat801.py)
python etl_pipeline_dat801.py


(When executed inside Databricks or using databricks connect)

🖥️ Power BI Connectivity (Optional)

Power BI can connect to Databricks using:

Connector: Azure Databricks

Hostname: adb-1200451892143447.7.azuredatabricks.net

HTTP Path: (provided in Databricks cluster connection details)

Authentication:

User name = token

Password = Databricks Personal Access Token

🔐 Security Notes

No secrets are hard-coded

Use Databricks Secret Scopes for production

Rotate access keys frequently

Use private endpoints for enterprise deployments

🧹 Cleanup (to avoid Azure charges)
az group delete -n rg-dat801 --yes --no-wait


Deletes all created resources safely.
