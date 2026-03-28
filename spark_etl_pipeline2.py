from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, round, month, year, avg, sum

# 1. Menyalakan Mesin Spark
print("Memulai Spark Session...")
spark = SparkSession.builder \
    .appName("Plantation_Data_Processing") \
    .master("local[*]") \
    .config("spark.jars", "postgresql-42.7.10.jar") \
    .getOrCreate()

print("Spark Session Berhasil Dibuat!")

# 2. Fase Extract: Membaca Data Baru (Incremental)
start_time = time.time()
print("Membaca dataset masa depan (2023)...")
df_spark = spark.read.csv("data_masuk/data_masa_depan_2023.csv", header=True, inferSchema=True)
end_time = time.time()

# 3. Verifikasi Data
print(f"Waktu baca: {end_time - start_time:.2f} detik")
print(f"Total Baris Data Baru: {df_spark.count()}")

# 6. Fase Transform: Membersihkan Desimal & Ekstraksi Waktu
print("\nMemulai Transformasi & Agregasi Data Bulanan...")
df_clean = df_spark.withColumn("Precipitation", round(col("Precipitation"), 2)) \
                   .withColumn("SoilMoisture", round(col("SoilMoisture"), 2)) \
                   .withColumn("Average_Temp", round(col("Average_Temp"), 2)) \
                   .withColumn("FFB_Yield", round(col("FFB_Yield"), 2)) \
                   .withColumn("Year", year(col("Date"))) \
                   .withColumn("Month", month(col("Date")))

# 7. Fase Transform: Agregasi Group By Estate, Tahun, dan Bulan
df_monthly = df_clean.groupBy("Estate_ID", "Year", "Month") \
                     .agg(
                         sum("Precipitation").alias("Total_Precipitation"),
                         avg("SoilMoisture").alias("Avg_SoilMoisture"),
                         avg("Average_Temp").alias("Avg_Temp"),
                         sum("FFB_Yield").alias("Total_Yield")
                     )

# 8. Fase Transform Akhir: Pembulatan
df_monthly = df_monthly.withColumn("Total_Precipitation", round(col("Total_Precipitation"), 2)) \
                       .withColumn("Avg_SoilMoisture", round(col("Avg_SoilMoisture"), 2)) \
                       .withColumn("Avg_Temp", round(col("Avg_Temp"), 2)) \
                       .withColumn("Total_Yield", round(col("Total_Yield"), 2))

# 11. Fase Load: Memasukkan Data Baru ke PostgreSQL (APPEND)
print("\nMenghubungkan ke PostgreSQL dan menambahkan data baru...")
db_url = "jdbc:postgresql://localhost:5433/db_plantation_analytics"
db_properties = {
    "user": "postgres",
    "password": "postgre", # Pastikan sandi sesuai dengan server lokal Anda
    "driver": "org.postgresql.Driver"
}

# Mode 'append' digunakan untuk Incremental Load tanpa menghapus data historis
df_monthly.write.jdbc(url=db_url, table="monthly_summary", mode="append", properties=db_properties)
print("Fase Load Selesai! Data Warehouse berhasil DIUPDATE dengan data terbaru.")
