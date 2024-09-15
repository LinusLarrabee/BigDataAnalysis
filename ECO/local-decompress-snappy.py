import pandas as pd

# 读取 Parquet 文件
parquet_file_path = "/Users/sunhao/Downloads/part-00000-a4904c61-e55b-448d-a071-d6055185e558-c000.snappy.parquet"
df = pd.read_parquet(parquet_file_path)

# 将数据保存为 CSV 文件
csv_file_path = "hourdevicenoncontroller.csv"
df.to_csv(csv_file_path, index=False)

print(f"Parquet file converted to CSV at: {csv_file_path}")
