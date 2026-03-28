# 🌴 Plantation Yield & Weather Analytics Pipeline

## 📌 Project Overview
This project is an end-to-end Data Engineering and Business Intelligence solution designed for the agriculture sector (specifically oil palm plantations). The goal is to analyze the correlation between environmental factors (precipitation, temperature, soil moisture) and Fresh Fruit Bunch (FFB) yield across multiple estates.

**Business Value:** Enables plantation managers to identify top-performing estates, track yield trends against weather anomalies, and make data-driven decisions for harvesting and irrigation strategies.

## 🛠️ Tech Stack & Architecture
- **Data Processing (ETL):** Apache Spark (PySpark) / Python
- **Data Visualization & BI:** Microsoft Power BI
- **Data Source:** Public Kaggle Dataset ("palm data" by MUNNA). Note: The raw dataset was deeply transformed and repurposed via PySpark specifically for agricultural FFB yield and weather correlation analysis.

## ⚙️ Data Pipeline (ETL Process)
The raw data was processed using **PySpark** to handle large-scale transformation before being loaded into the BI tool. 
Key transformations include:
1. **Data Cleaning:** Handling missing values in weather sensor data.
2. **Aggregation:** Calculating monthly averages for temperature and soil moisture.
3. **Feature Engineering:** Grouping FFB yield by `Estate_ID` and `Year`.
*(Note: The PySpark script `spark_etl_pipeline.py` is included in this repository).*

## 📊 Dashboard Highlights
The interactive Power BI dashboard provides three main insights:
1. **Executive Summary (KPIs):** Quick glance at Total Yield, Average Temperature, and Soil Moisture.
2. **Trend Analysis:** A time-series visualization tracking the impact of monthly precipitation on FFB yield.
3. **Estate Benchmarking & Correlation:** - Top 10 Estates by Yield.
   - Scatter plot proving the correlation between temperature and soil moisture density.

<img width="1600" height="1263" alt="image" src="https://github.com/user-attachments/assets/7e67f72b-1f66-46e9-9201-adfc123c2d57" />



## 🚀 How to Run
1. Clone this repository.
2. Run the PySpark script `spark_etl_pipeline.py` to generate the clean dataset.
3. Open `Plantation_Analytics_Dashboard_v1.pbix` using Power BI Desktop.
4. Refresh the data source to point to your local processed CSV/Database.
