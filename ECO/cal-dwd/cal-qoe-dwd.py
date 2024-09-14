from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ODS to DWD - errors_rate Calculation") \
    .getOrCreate()

# 读取ODS层的ap表（从S3或HDFS读取Parquet文件）
input_path = "s3://your-bucket/ods/ap_table/"
df_ods_ap = spark.read.parquet(input_path)

# 计算errors_rate，确保避免除零错误
df_dwd_ap = df_ods_ap.withColumn(
    "errors_rate",
    when((col("packets_received") + col("packets_sent")) > 0,
         col("errors_pkt") / (col("packets_received") + col("packets_sent"))
         ).otherwise(0.0)
)

# 写入到DWD层的Parquet文件
output_path = "s3://your-bucket/dwd/ap_table/"
df_dwd_ap.write.mode("overwrite").parquet(output_path)

# 停止SparkSession
spark.stop()
