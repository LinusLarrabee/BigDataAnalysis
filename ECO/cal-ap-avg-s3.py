from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, avg, first, date_format

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("PySpark Aggregation on S3 with Snappy") \
    .getOrCreate()

# 读取S3上以Parquet格式存储的文件，带有Snappy压缩
input_path = "s3://aps1-tauc-data-analysis/extracted/part-00000-43d2a418-13a5-4c57-b654-e7fd8a428ef0-c000.snappy.parquet"
df = spark.read.parquet(input_path)

df.printSchema()
# 如果存在dt字段，移除该字段
if 'dt' in df.columns:
    df = df.drop('dt')

# 将collection_time从时间戳转换为日期时间格式，并创建小时和天字段
df = df.withColumn("collection_time_hour", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd HH:00:00"))
df = df.withColumn("collection_time_day", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd"))

# 聚合的唯一性判断字段
key_columns_hour = ['controller_id', 'device_id', 'band', 'collection_time_hour']
key_columns_day = ['controller_id', 'device_id', 'band', 'collection_time_day']

# 选择需要聚合的字段，排除key_columns
columns_to_aggregate = [col for col in df.columns if col not in key_columns_hour and col not in key_columns_day]

# 将字符串和数值字段区分开
string_columns = [field for field, dtype in df.dtypes if dtype == 'string' and field not in key_columns_hour and field not in key_columns_day]
numeric_columns = [field for field, dtype in df.dtypes if dtype in ['int', 'double', 'float'] and field not in key_columns_hour and field not in key_columns_day]

# 按小时聚合
agg_by_hour = df.groupBy("controller_id", "device_id", "band", "collection_time_hour").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 显示按小时聚合结果
print("按小时聚合结果：")
agg_by_hour.show()
agg_by_hour.printSchema()
# 按天聚合
agg_by_day = df.groupBy("controller_id", "device_id", "band", "collection_time_day").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 显示按天聚合结果
print("按天聚合结果：")
agg_by_day.show()
agg_by_day.printSchema()
# 停止SparkSession
spark.stop()
