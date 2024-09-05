from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建 SparkSession
spark = SparkSession.builder.appName("Save AP Data with Date Partitioning").getOrCreate()

# 通用函数：按日期存储数据，bucket 和 output_prefix 作为参数输入
def save_by_date_partitioning(df, table_name, bucket, output_prefix):
    # 将 collection_time 转换为日期格式 'YYYY-MM-DD'
    df_with_date = df.withColumn("formatted_date", F.date_format(F.from_unixtime(F.col("collection_time")), 'yyyy-MM-dd'))

    # 获取所有不同的日期
    distinct_dates = df_with_date.select("formatted_date").distinct().collect()

    # 遍历每个日期进行分区存储
    for row in distinct_dates:
        date_str = row["formatted_date"]
        output_path = f's3a://{bucket}/{output_prefix}/{table_name}/dt={date_str}/'

        # 过滤出当前日期的数据并保存
        df_with_date.filter(df_with_date["formatted_date"] == date_str) \
            .coalesce(1) \
            .write.mode('overwrite').parquet(output_path, compression='snappy')

# 假设 bucket 和 output_prefix 是以参数形式传入
bucket = "aps1-tauc-data-analysis"
output_prefix = "extracted"

# 1. 读取 ap_data 的 Parquet 文件
ap_data_path = "s3a://aps1-tauc-data-analysis/ap_data.parquet"
df_ap = spark.read.parquet(ap_data_path)

# 处理 backhaul 数据并按日期保存
df_backhaul = df_ap.filter(df_ap['is_controller'] == 0).select(
    "controller_id",
    "device_id",
    "band",
    "collection_time",
    "backhaul_sta_mac_address",
    "backhaul_sta_backhaul_link_type",
    "backhaul_sta_link_rate",
    "backhaul_sta_signal_strength",
    "backhaul_sta_utilization"
)
save_by_date_partitioning(df_backhaul, "backhaul", bucket, output_prefix)

# 处理 detailedap 数据并按日期保存
df_detailedap = df_ap.select(
    "controller_id",
    "device_id",
    "band",
    "collection_time",
    "average_rx_rate",
    "average_tx_rate",
    "congestion_score",
    "wifi_coverage_score",
    "noise",
    "errors_pkt",
    "packets_received",
    "packets_sent",
    "wan_bandwidth"
)
save_by_date_partitioning(df_detailedap, "detailedap", bucket, output_prefix)

# 停止 SparkSession
spark.stop()
