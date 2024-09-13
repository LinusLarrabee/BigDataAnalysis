import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# 获取命令行参数
bucket = sys.argv[1]
input_prefix = sys.argv[2]
output_prefix = sys.argv[3]
start_date = sys.argv[4]  # 起始日期，格式：YYYY-MM-DD
end_date = sys.argv[5]    # 结束日期，格式：YYYY-MM-DD

# 创建 SparkSession
spark = SparkSession.builder.appName("Save AP Data with Date Partitioning").getOrCreate()

# 通用函数：按日期存储数据，分别存储到 backhaul 和 detailed_ap
def save_by_date_partitioning(df, table_name, bucket, input_prefix, output_prefix, start_date, end_date):
    # 将 collection_time 转换为日期格式 'YYYY-MM-DD'
    df_with_date = df.withColumn("formatted_date", F.date_format(F.from_unixtime(F.col("collection_time")), 'yyyy-MM-dd'))

    # 过滤出 start_date 和 end_date 范围内的数据
    df_filtered = df_with_date.filter((F.col("formatted_date") >= start_date) & (F.col("formatted_date") <= end_date))

    # 获取过滤后的日期
    distinct_dates = df_filtered.select("formatted_date").distinct().collect()

    # 遍历每个日期进行分区存储
    for row in distinct_dates:
        date_str = row["formatted_date"]
        input_path = f's3a://{bucket}/{input_prefix}/{table_name}/dt={date_str}/'
        output_path = f's3a://{bucket}/{output_prefix}/{table_name}/dt={date_str}/'

        print(f"Processing date {date_str}:")
        print(f"Input Path: {input_path}")
        print(f"Output Path: {output_path}")

        # 过滤出当前日期的数据并保存
        df_filtered.filter(df_filtered["formatted_date"] == date_str) \
            .coalesce(1) \
            .write.mode('overwrite').parquet(output_path, compression='snappy')

# 1. 读取 ap_data 的 Parquet 文件
ap_data_path = f"s3a://{bucket}/{input_prefix}/ap_data"
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
save_by_date_partitioning(df_backhaul, "backhaul", bucket, input_prefix, output_prefix, start_date, end_date)

# 处理 detailed_ap 数据并添加 error_rate 列
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
).withColumn(
    "error_rate",
    when((col("packets_received") + col("packets_sent")) > 0,
         col("errors_pkt") / (col("packets_received") + col("packets_sent"))
         ).otherwise(0.0)
)

# 保存数据并按日期分区
save_by_date_partitioning(df_detailedap, "detailed_ap", bucket, input_prefix, output_prefix, start_date, end_date)

# 停止 SparkSession
spark.stop()
