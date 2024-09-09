from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, date_format, avg, first

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("PySpark Aggregation by Hour and Day") \
    .getOrCreate()

# 读取CSV文件
df = spark.read.csv("/Users/sunhao/s3/ap_data.csv/part-00000-563593f8-91f9-46c2-88f8-49924c7c6148-c000.csv", header=True, inferSchema=True)

# 将collection_time从时间戳转换为日期时间格式，并分别创建小时和天字段
df = df.withColumn("collection_time_hour", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd HH:00:00"))
df = df.withColumn("collection_time_day", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd"))

# 获取所有字段的列名
columns = df.columns

# 定义哪些字段是数值型，哪些是字符型
string_columns = ['controller_id', 'device_id', 'band', 'backhaul_sta_mac_address', 'backhaul_sta_backhaul_link_type', 'ip_address']  # 举例说明字符字段
numeric_columns = [col for col in columns if col not in string_columns + ['collection_time_hour', 'collection_time_day', 'collection_time']]

# 按小时聚合
agg_by_hour = df.groupBy("controller_id", "device_id", "band", "collection_time_hour").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 按天聚合
agg_by_day = df.groupBy("controller_id", "device_id", "band", "collection_time_day").agg(
    *[first(col(c)).alias(c) for c in string_columns],  # 对字符字段取单一值
    *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
)

# 显示按小时聚合结果
agg_by_hour.show()

# 显示按天聚合结果
agg_by_day.show()

# 停止SparkSession
spark.stop()
