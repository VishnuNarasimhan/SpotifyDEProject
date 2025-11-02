# 🎧 Spotify Data Engineering Project

An end-to-end **data engineering pipeline** built using **Azure Data Factory**, **Databricks (PySpark)**, **Delta Lake**, and **Jinja** for dynamic configuration and automation.

---

## 🧭 Overview

This project replicates a **Spotify-style data platform**, where raw data flows through **Bronze → Silver → Gold** layers.  
It covers **data ingestion, transformation, and modeling**, showcasing real-world data engineering practices using Azure tools.

---

## 🧰 Tech Stack

- **Azure Data Factory (ADF)** – Orchestrates data movement and pipeline scheduling  
- **Azure Databricks (PySpark)** – Performs transformation and data modeling  
- **Azure Data Lake Storage (ADLS Gen2)** – Stores raw, processed, and curated data  
- **Delta Lake** – Provides ACID transactions, schema enforcement, and time travel  
- **Jinja** – Used for parameterized and reusable PySpark scripts  
- **Languages:** Python, SQL, JSON  

---

## 📁 Folder Structure
spotify-data-engineering-project/
│
├── adf/ # Azure Data Factory pipelines and configs
│ ├── factory/
│ ├── pipeline/
│ ├── dataset/
│ ├── linkedService/
│ └── publish_config.json
│
├── databricks/ # Databricks notebooks and PySpark scripts
│ ├── src/
│ │ └── silver/
│ │ └── silver_dimensions.py
│ └── notebooks/
|
└── README.md

---

## 🔄 Data Pipeline Flow

### 1️⃣ Ingestion (Bronze)
- Raw Spotify-like data (users, tracks, streams) is ingested via ADF.  
- Stored in ADLS in Parquet/JSON format.

### 2️⃣ Transformation (Silver)
- Databricks reads Bronze data and cleans it using **PySpark** with **Jinja templates**.  
- Generates structured Delta tables:  
  - `DimUser`  
  - `DimTrack`  
  - `FactStream`

### 3️⃣ Modeling (Gold)
- Aggregates and models the processed data for analytics and reporting.  
- Data is written back to ADLS in **Delta format** supporting **time travel** and ACID reliability.

---

## ✨ Key Features

- End-to-end orchestration with **ADF ↔ Databricks** integration  
- Modular and reusable PySpark code built with **Jinja templating**  
- Reliable data storage with **Delta Lake** (ACID + time travel)  
- Follows **Bronze–Silver–Gold** architecture for clarity and scalability  
- Clean project structure ready for portfolio or production reference

---

## 📊 Deliverables

- **ADF pipelines:** Automated ingestion and Databricks triggers  
- **Curated Delta tables:** `DimUser`, `DimTrack`, and `FactStream`  
- **Reusable Jinja templates:** For configurable PySpark transformations  
- **Docs and screenshots:** Architecture visuals and pipeline runs  

---

> ⚡ *A clean, production-style Data Engineering project showing how ADF, Databricks, Delta Lake, and Jinja work together to build scalable ETL pipelines.*

