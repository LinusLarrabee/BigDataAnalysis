from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, avg, first, date_format

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("PySpark Aggregation on S3 with Snappy") \
    .getOrCreate()

# 读取S3上以Parquet格式存储的文件，带有Snappy压缩
input_path = "s3://aps1-tauc-data-analysis/extracted/ap_data/dt=2024-07-30/part-00000-4becffb4-9899-49de-9a02-a3195629e102-c000.snappy.parquet"
df = spark.read.parquet(input_path)

# 将collection_time从时间戳转换为日期时间格式，并创建小时和天字段
df = df.withColumn("collection_time_hour", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd HH:00:00"))
df = df.withColumn("collection_time_day", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd"))

# 获取所有字段的列名
columns = df.columns

# 选择关键字段
key_columns = ['controller_id', 'device_id', 'band', 'collection_time_hour', 'collection_time_day']

# 根据字段类型分类，字符串字段取第一个值，数值字段取平均值
string_columns = [field for field, dtype in df.dtypes if dtype == 'string' and field not in key_columns]
numeric_columns = [field for field, dtype in df.dtypes if dtype in ['int', 'double', 'float'] and field not in key_columns]

# 按小时聚合
agg_by_hour = df.groupBy("controller_id", "device_id", "band", "collection_time_hour").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 按天聚合
agg_by_day = df.groupBy("controller_id", "device_id", "band", "collection_time_day").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 显示按小时聚合结果
agg_by_hour.show()

# 显示按天聚合结果
agg_by_day.show()

# 停止SparkSession
spark.stop()
