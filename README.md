# 📘 DAT801A – Cloud Data Pipeline & Machine Learning Project

### **Azure + Databricks End-to-End Data Engineering & ML Pipeline**

**Author:** *smohammed24@students.icms.edu.au*  
**Workspace:** `adb-1200451892143447.7.azuredatabricks.net`  
**Course:** DAT801A – Cloud Data & Machine Learning  
**Assessment:** Project / Prototype Submission  

---

# 🧭 1. Project Overview

This project demonstrates a complete **cloud-native data engineering and machine learning pipeline** built using **Microsoft Azure** and **Azure Databricks**. The objective is to ingest raw retail sales data, transform it into analytics-ready form, train a predictive machine learning model, and prepare the results for dashboard visualization.

📥 **Ingestion → 🧹 Preparation → 🤖 Machine Learning → 📊 Analytics → 📈 Visualization**

---

# 🏗️ 2. Architecture Summary

## **Stage 1 — Ingestion**
- Raw data uploaded to **Azure Blob Storage**:
  - Storage Account: `stdat801siraj`
- Real-time streaming using **Azure Event Hubs**:
  - Namespace: `ehns-dat801`
  - Event Hub: `eh-dat801`

---

## **Stage 2 — Data Preparation (ETL in Databricks)**

ETL performed using **PySpark**:
- Schema inference  
- Null-handling  
- Deduplication  
- Type casting  
- Timestamp parsing  
- Feature engineering (`TotalPrice = Quantity × UnitPrice`)

---

## **Stage 3 — Machine Learning (MLflow + scikit-learn)**

```
Sample shape: (5465, 3)
Train R2 Score: 0.9356
Test  R2 Score: 0.9671
Model: /dbfs/FileStore/models/rf_demo.pkl
```

---

## **Stage 4 — Delta Lake Storage**

```
Database: dat801_db
Table: sales_processed
Location: dbfs:/FileStore/delta/processed
```

---

## **Stage 5 — Visualizations**
- Databricks dashboards  
- Power BI integration  

---

# 📂 Repository Structure

```
├── README.md
├── notebooks/ETL_Pipeline_Dat801.ipynb
├── etl_pipeline_dat801.py
├── send_events.py
├── models/rf_demo.pkl
└── data/data.csv
```

---

# 🧪 Validation
- Schema checks  
- Duplicate checks  
- ML model validation  
- Delta table verification  

---

# 🧰 Key Code Snippets

```python
df_raw = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/tables/data.csv")
```

```python
df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/delta/processed")
```

```python
joblib.dump(model, "/dbfs/FileStore/models/rf_demo.pkl")
```

---

# 🏁 Conclusion

A complete **Azure–Databricks cloud data & ML pipeline**, fulfilling DAT801A assessment requirements.
