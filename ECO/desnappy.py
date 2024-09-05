from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Read Snappy Parquet") \
    .getOrCreate()

# 读取 Snappy 压缩的 Parquet 文件，文件路径替换为你的本地文件路径
parquet_file_path = "/Users/sunhao/s3/parquet/part-00000-54acad5f-e1ec-4304-974d-1efe3037c640-c000.snappy.parquet"

# 读取 Parquet 文件
df = spark.read.parquet(parquet_file_path)

# 展示表结构（Schema）
df.printSchema()

# 展示前3行数据
df.show(3, truncate=False)

# 停止 SparkSession
spark.stop()
