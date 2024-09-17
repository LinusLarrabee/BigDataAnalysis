import pandas as pd

# 读取 Parquet 文件
parquet_file_path = "/Users/sunhao/Downloads/part-00000-401a3de7-8933-410a-a275-acf7530eb152-c000.snappy.parquet"
df = pd.read_parquet(parquet_file_path)
print(f"Original Parquet file rows: {len(df)}")


# 将数据保存为 CSV 文件
csv_file_path = "wireless_dwm_fin.csv"
df.to_csv(csv_file_path, index=False)

print(f"Parquet file converted to CSV at: {csv_file_path}")
