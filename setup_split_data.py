import pandas as pd
import os

print("1. Menyiapkan ruang karantina...")
os.makedirs('data_historis', exist_ok=True)
os.makedirs('data_masuk', exist_ok=True)

print("2. Membaca 1.8 juta baris data ke RAM...")
# Ganti nama file di bawah ini jika berbeda
df = pd.read_csv("massive_plantation_data.csv")

print("3. Mengekstrak Tahun...")
df['Date'] = pd.to_datetime(df['Date'])

print("4. Menyimpan Masa Lalu (2014-2022)...")
df_historis = df[df['Date'].dt.year <= 2022]
df_historis.to_csv("data_historis/data_masa_lalu.csv", index=False)

print("5. Menyimpan Masa Depan (2023)...")
df_baru = df[df['Date'].dt.year == 2023]
df_baru.to_csv("data_masuk/data_masa_depan_2023.csv", index=False)

print("Operasi Bedah Data Selesai 100%!")
