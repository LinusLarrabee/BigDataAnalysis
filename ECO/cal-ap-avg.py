import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# 获取命令行参数
bucket = sys.argv[1]
input_prefix = sys.argv[2]
output_prefix = sys.argv[3]
start_date = sys.argv[4]  # 起始日期，格式：YYYY-MM-DD
end_date = sys.argv[5]    # 结束日期，格式：YYYY-MM-DD

# 创建 SparkSession
spark = SparkSession.builder.appName("Save AP Avg Data with Aggregation").getOrCreate()

# 函数：生成从 start_date 到 end_date 的日期列表
def get_date_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    delta = timedelta(days=1)
    current = start
    date_list = []
    while current <= end:
        date_list.append(current.strftime('%Y-%m-%d'))
        current += delta
    return date_list

# 函数：按时间粒度聚合并存储
def aggregate_and_save(df, bucket, output_prefix, time_granularity, start_date, end_date):
    if time_granularity == 'hour':
        df_with_time = df.withColumn("time", F.date_format(F.from_unixtime(F.col("collection_time")), 'yyyy-MM-dd HH:00:00'))
    elif time_granularity == 'day':
        df_with_time = df.withColumn("time", F.date_format(F.from_unixtime(F.col("collection_time")), 'yyyy-MM-dd'))
    else:
        raise ValueError("Unknown time granularity!")

    # 过滤出 start_date 和 end_date 范围内的数据
    df_filtered = df_with_time.filter((F.col("time") >= start_date) & (F.col("time") <= end_date))

    # 按 controller_id, device_id, band, 和 time 进行分组，计算每个字段的平均值
    df_aggregated = df_filtered.groupBy("controller_id", "device_id", "band", "time").agg(
        *[F.avg(col).alias(col) for col in df.columns if col not in ['controller_id', 'device_id', 'band', 'collection_time', 'time']]
    )

    # 输出路径：统一存储为ap_avg
    output_path = f's3a://{bucket}/{output_prefix}/ap_avg/{time_granularity}/'

    # 保存聚合后的结果
    df_aggregated.write.mode('overwrite').parquet(output_path, compression='snappy')

# 1. 生成指定日期范围内的所有日期
date_list = get_date_range(start_date, end_date)

# 2. 遍历日期，逐个读取对应路径的 Parquet 文件
for date_str in date_list:
    input_path = f"s3a://{bucket}/{input_prefix}/ap_data/dt={date_str}/"

    # 读取当前日期的 Parquet 文件
    try:
        df_ap = spark.read.parquet(input_path)
        print(f"Processing data for date: {date_str}")

        # 处理 ap 数据，只选择需要的字段
        df_ap_avg = df_ap.select(
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

        # 聚合并保存 ap 数据，分别按小时和天聚合
        aggregate_and_save(df_ap_avg, bucket, output_prefix, 'hour', date_str, date_str)
        aggregate_and_save(df_ap_avg, bucket, output_prefix, 'day', date_str, date_str)

    except Exception as e:
        print(f"Error processing date {date_str}: {e}")

# 停止 SparkSession
spark.stop()
