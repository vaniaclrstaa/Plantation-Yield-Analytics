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

![Dashboard Preview]
<img width="847" height="661" alt="image" src="https://github.com/user-attachments/assets/3ef0d816-46ac-48e3-bed0-83e857b677c7" />
<img width="854" height="664" alt="image" src="https://github.com/user-attachments/assets/f818796c-a5ab-4fca-a37b-661e3d9b3ebe" />


## 🚀 How to Run
1. Clone this repository.
2. Run the PySpark script `spark_etl_pipeline.py` to generate the clean dataset.
3. Open `Plantation_Analytics_Dashboard_v1.pbix` using Power BI Desktop.
4. Refresh the data source to point to your local processed CSV/Database.
