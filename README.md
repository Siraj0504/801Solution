# 📘 DAT801A – Cloud Data Pipeline & Machine Learning Project

### **Azure + Databricks End-to-End Data Engineering & ML Pipeline**

**Author:** *smohammed24@students.icms.edu.au*  
**Workspace:** `adb-1200451892143447.7.azuredatabricks.net`  
**Course:** DAT801A – Cloud Data & Machine Learning  
**Assessment:** Project / Prototype Submission  

---

# 🧭 1. Project Overview

This project demonstrates a complete **cloud-native data engineering and machine learning pipeline** built using **Microsoft Azure** and **Azure Databricks**. The objective is to ingest raw retail sales data, transform it into analytics-ready form, train a predictive machine learning model, and prepare the results for dashboard visualization.

This end-to-end solution reflects modern cloud data architecture best practices, following the workflow:

📥 **Ingestion → 🧹 Preparation → 🤖 Machine Learning → 📊 Analytics → 📈 Visualization**

---

# 🏗️ 2. Architecture Summary

## **Stage 1 — Ingestion**
- Raw data uploaded to **Azure Blob Storage**:
  - Storage Account: `stdat801siraj`
- Real-time streaming using **Azure Event Hubs**:
  - Namespace: `ehns-dat801`
  - Event Hub: `eh-dat801`
- Data consumed into Databricks using Event Hub consumer or batch ingestion.

---

## **Stage 2 — Data Preparation (ETL in Databricks)**

ETL performed using **PySpark**:

- Schema inference  
- Null-value handling  
- Deduplication  
- Data type casting  
- Timestamp parsing (`InvoiceDate_ts`)  
- Feature engineering (`TotalPrice = Quantity × UnitPrice`)  
- Cleaning invalid records  

Raw data path:

dbfs:/FileStore/tables/data.csv

yaml
Copy code

---

## **Stage 3 — Machine Learning (MLflow + scikit-learn)**

A Random Forest regression model was trained and tracked using MLflow.

### **ML Experiment Output**

Sample shape: (5465, 3)

Run:
https://adb-1200451892143447.7.azuredatabricks.net/ml/experiments/1056520141816893/runs/71f43efb91a645dfae6619a1e37893e5

Train R2 Score: 0.9356036875472284
Test R2 Score: 0.9671273157271898

Model saved at:
/dbfs/FileStore/models/rf_demo.pkl

yaml
Copy code

✔ MLflow tracking  
✔ Model stored in DBFS  
✔ Train/Test performance validated  

---

## **Stage 4 — Analytics-Ready Data (Delta Lake)**

Processed data stored in **Delta Lake**:

Database: dat801_db
Table: sales_processed
Location: dbfs:/FileStore/delta/processed

yaml
Copy code

Delta Lake benefits:
- ACID transactions  
- Time travel  
- Optimized reads/writes  
- Scalable for BI + ML workloads  

---

## **Stage 5 — Visualization Layer**

### ✔ Databricks Visualizations:
- Monthly revenue  
- Product revenue ranking  
- Country-level sales  
- Hourly sales trends  

### ✔ Power BI (Optional)
- Databricks SQL Endpoint  
- DirectQuery or Import  
- Token authentication  

---

# 📂 3. Repository / Workspace Structure

├── README.md
├── notebooks/
│ └── ETL_Pipeline_Dat801.ipynb
├── etl_pipeline_dat801.py
├── send_events.py
├── models/
│ └── rf_demo.pkl
└── data/
└── data.csv

yaml
Copy code

---

# 🧪 4. Validation & Testing

### ✔ Data Validation  
- Schema checks  
- Duplicate removal  
- Null-field handling  

### ✔ ML Validation  
- R² score comparison  
- MLflow experiment tracking  
- Model serialization  

### ✔ Delta Validation
```sql
SELECT COUNT(*) FROM dat801_db.sales_processed;
✔ Dashboard Validation
All visual charts verified

Trending metrics validated

🧰 5. Key Code Snippets
📥 Load CSV
python
Copy code
df_raw = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("dbfs:/FileStore/tables/data.csv")
🧹 Clean & Transform
python
Copy code
df = df_raw.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

df = df.withColumn(
    "InvoiceDate_ts",
    to_timestamp(col("InvoiceDate"), "d/M/yyyy H:mm")
)
💾 Save to Delta
python
Copy code
df.write.format("delta").mode("overwrite")\
    .save("dbfs:/FileStore/delta/processed")
🤖 Train ML Model
python
Copy code
model = RandomForestRegressor(n_estimators=50, random_state=42)
model.fit(X_train, y_train)

joblib.dump(model, "/dbfs/FileStore/models/rf_demo.pkl")
🧹 6. Cleanup
To avoid Azure usage charges:

bash
Copy code
az group delete -n rg-dat801 --yes --no-wait
📦 7. Deliverables Completed
✔ Cloud architecture
✔ ETL pipeline
✔ Delta Lake storage
✔ Machine Learning model
✔ MLflow tracking
✔ Databricks dashboards
✔ README documentation
✔ BI-ready dataset
✔ Presentation & report

🏁 8. Conclusion
This project showcases a full Azure + Databricks cloud data pipeline following the Lakehouse architecture. It includes ingestion, transformation, Delta Lake storage, ML training, model tracking, and dashboard visualization.

It fully meets the assessment requirements for DAT801A and provides a production-ready foundation for advanced analytics, automation, and large-scale ML workloads.
