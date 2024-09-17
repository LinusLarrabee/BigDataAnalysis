import pandas as pd

# 读取 Parquet 文件
parquet_file_path = "/Users/sunhao/Downloads/part-00000-8bdd827c-81fb-49e5-9702-47d70e2a5cee-c000.snappy.parquet"
df = pd.read_parquet(parquet_file_path)
print(f"Original Parquet file rows: {len(df)}")


# 将数据保存为 CSV 文件
csv_file_path = "wireless_dwm.csv"
df.to_csv(csv_file_path, index=False)

print(f"Parquet file converted to CSV at: {csv_file_path}")
