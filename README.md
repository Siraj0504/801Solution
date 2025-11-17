# 📘 DAT801A – Cloud Data Pipeline & Machine Learning Project  
### Azure + Databricks End-to-End Pipeline

**Author:** smohammed24@students.icms.edu.au  
**Workspace:** adb-1200451892143447.7.azuredatabricks.net  

## 📌 Overview
This project delivers a full cloud-based data engineering and machine learning pipeline using Azure and Databricks.

## 🏗️ Architecture Stages
### Stage 1 — Ingestion
- Azure Blob Storage  
- Event Hubs (ehns-dat801 / eh-dat801)

### Stage 2 — Preparation
- PySpark ETL  
- Cleaning, transformation, TotalPrice calculation  
- CSV loaded from DBFS

### Stage 3 — Machine Learning
Sample model metrics:

Sample shape: (5465, 3)
Train R2 Score: 0.9356036875472284  
Test R2 Score: 0.9671273157271898  
Model saved: /dbfs/FileStore/models/rf_demo.pkl

### Stage 4 — Analytics
Delta table: dat801_db.sales_processed

### Stage 5 — Visualization
- Databricks built‑in visuals  
- Power BI ready output  

## 🧪 Validation
- MLflow experiment logged  
- Delta table validated  
- Visuals generated  

## 🧹 Cleanup
az group delete -n rg-dat801 --yes --no-wait

## ✔ Deliverables Completed
- Presentation  
- Report  
- README  
- Prototype code  
- Working video  
- Execution instructions  
