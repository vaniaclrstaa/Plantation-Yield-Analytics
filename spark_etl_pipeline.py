from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, round, month, year, avg, sum

# 1. Menyalakan Mesin Spark
print("Memulai Spark Session...")
spark = SparkSession.builder \
    .appName("Plantation_Data_Processing") \
    .master("local[*]") \
    .config("spark.jars", "file:///D:/hadoop/bin/postgresql-42.7.10.jar") \
    .getOrCreate()

print("Spark Session Berhasil Dibuat!")

# 2. Membaca Data Raksasa
# Perhatikan seberapa cepat Spark membaca ini dibandingkan Pandas
start_time = time.time()
print("Membaca dataset 1.8 juta baris...")
df_spark = spark.read.csv("massive_plantation_data.csv", header=True, inferSchema=True)
end_time = time.time()

# 3. Verifikasi Data
print(f"Waktu baca: {end_time - start_time:.2f} detik")
print(f"Total Baris: {df_spark.count()}")

# 4. Tampilkan Skema (Struktur Kolom dan Tipe Data)
df_spark.printSchema()

# 5. Tampilkan 5 baris pertama
df_spark.show(5)
# 6. Transformasi: Membersihkan Desimal & Ekstraksi Waktu
print("\nMemulai Transformasi & Agregasi Data Bulanan...")
df_clean = df_spark.withColumn("Precipitation", round(col("Precipitation"), 2)) \
                   .withColumn("SoilMoisture", round(col("SoilMoisture"), 2)) \
                   .withColumn("Average_Temp", round(col("Average_Temp"), 2)) \
                   .withColumn("FFB_Yield", round(col("FFB_Yield"), 2)) \
                   .withColumn("Year", year(col("Date"))) \
                   .withColumn("Month", month(col("Date")))

# 7. Agregasi: Group By Estate, Tahun, dan Bulan
df_monthly = df_clean.groupBy("Estate_ID", "Year", "Month") \
                     .agg(
                         sum("Precipitation").alias("Total_Precipitation"),
                         avg("SoilMoisture").alias("Avg_SoilMoisture"),
                         avg("Average_Temp").alias("Avg_Temp"),
                         sum("FFB_Yield").alias("Total_Yield")
                     )

# 8. Transformasi Akhir: Bulatkan Hasil Agregasi
df_monthly = df_monthly.withColumn("Total_Precipitation", round(col("Total_Precipitation"), 2)) \
                       .withColumn("Avg_SoilMoisture", round(col("Avg_SoilMoisture"), 2)) \
                       .withColumn("Avg_Temp", round(col("Avg_Temp"), 2)) \
                       .withColumn("Total_Yield", round(col("Total_Yield"), 2))

# 9. Verifikasi Hasil Akhir
df_monthly.show(5)
print(f"Total Baris Setelah Agregasi Bulanan: {df_monthly.count()}")
# 10. Fase Load: Menyimpan ke format Parquet
print("\nMenyimpan hasil agregasi ke dalam format Parquet...")
df_monthly.write.mode("overwrite").parquet("monthly_plantation_summary.parquet")
print("Arsitektur ETL Pipeline Selesai. Data berhasil diamankan!")
# 11. Fase Load: Memasukkan Data ke Data Warehouse (PostgreSQL)
print("\nMenghubungkan ke PostgreSQL dan memuat data...")
db_url = "jdbc:postgresql://localhost:5433/db_plantation_analytics"
db_properties = {
    "user": "postgres",
    "password": "postgre", # UBAH BAGIAN INI DENGAN SANDI ASLI ANDA
    "driver": "org.postgresql.Driver"
}

# Menulis dataframe bulanan ke tabel PostgreSQL bernama 'monthly_summary'
df_monthly.write.jdbc(url=db_url, table="monthly_summary", mode="overwrite", properties=db_properties)
print("Fase Load Selesai! Data Warehouse berhasil diperbarui. Pipeline ETL Sukses 100%.")