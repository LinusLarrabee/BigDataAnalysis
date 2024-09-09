import sys
from pyspark.sql import SparkSession

# 获取命令行参数
parquet_path = sys.argv[1]  # 从命令行获取 Parquet 文件路径

# 创建 SparkSession
spark = SparkSession.builder.appName("View Parquet Data from S3").getOrCreate()

# 读取 Parquet 文件
df = spark.read.parquet(parquet_path)

# 显示前几行数据
print("Showing first few rows:")
df.show(5, truncate=False)

# 显示表结构
print("Schema of the table:")
df.printSchema()

# 停止 SparkSession
spark.stop()
