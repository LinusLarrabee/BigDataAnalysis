import pandas as pd

# 读取 Parquet 文件
parquet_file_path = "/Users/sunhao/s3/output/dt=2024-09-20/part-00000-d763ed57-dd94-44e8-b271-394369b7ba03-c000.snappy.parquet"
df = pd.read_parquet(parquet_file_path)
print(f"Original Parquet file rows: {len(df)}")


# 将数据保存为 CSV 文件
csv_file_path = "ap_survey_09-25.csv"
df.to_csv(csv_file_path, index=False)

print(f"Parquet file converted to CSV at: {csv_file_path}")
