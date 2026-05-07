# 🛒 E-Commerce Data Pipeline using PySpark

## 📌 Overview
This project implements an end-to-end data pipeline using PySpark to process e-commerce data.  
It covers ingestion, transformation, analytics, and storage.

---
## ⚙️ Tech Stack
- Python
- PySpark
- Pandas
- Parquet
---
## 📂 Dataset
- Customers
- Products
- Orders
- Order Items
---
## 🚀 Pipeline Flow
Raw Data → Ingestion → Transformation → Analytics → Storage → Output
---
## 🔹 Phases
1. Data Ingestion & Validation  
2. Order Analytics  
3. Customer 360 View  
4. Product Insights  
5. Window Functions  
6. Storage Optimization  
7. Final Pipeline  
---
## ⚠️ Challenges Faced
- Hadoop NativeIO error on Windows  
- Spark write failure  
---
## ✅ Solution
Used Pandas fallback to safely write output.
---
## 🧠 Learnings
- Transformations vs Actions  
- Joins & Shuffle  
- Window Functions

- ## 📸 Screenshots

### Spark UI
![Spark UI](screenshots/spark_ui.png)

### Output Files
![Output](screenshots/output.png)
