from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder.appName("View Parquet Data from S3").getOrCreate()

# S3 文件路径
parquet_path = "s3a://aps1-tauc-data-analysis/ods/avg/backhaul_avg/hour/part-00000-e65d11a1-d118-4235-be6a-c17e81688830-c000.snappy.parquet"

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
