import pandas as pd

# 读取 Parquet 文件
parquet_file_path = "/Users/sunhao/Downloads/part-00000-b487f001-30ef-4770-a719-525e5ca4d999-c000.snappy.parquet"
df = pd.read_parquet(parquet_file_path)
print(f"Original Parquet file rows: {len(df)}")


# 将数据保存为 CSV 文件
csv_file_path = "wireless_dwm11.csv"
df.to_csv(csv_file_path, index=False)

print(f"Parquet file converted to CSV at: {csv_file_path}")
