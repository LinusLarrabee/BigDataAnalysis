import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta

def process_time_range(start_date, end_date):
    """
    生成从 start_date 到 end_date 的日期列表
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    :return: 日期字符串列表，格式为 'YYYY-MM-DD'
    """
    date_list = []
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    return date_list

def calculate_ads_from_dws(bucket, dws_prefix, ads_output_prefix, dws_agg_list, start_date, end_date):
    """
    从 DWS 层计算 ADS 层，以 controller 的 network 表为主表，添加 wireless 表中的 per_band_count、per_network_wireless_count 和 per_network_count 信息。
    :param bucket: S3 bucket 名称
    :param dws_prefix: DWS 表的前缀路径
    :param ads_output_prefix: ADS 表的输出前缀路径
    :param dws_agg_list: 包含 'daily', 'hourly' 的列表，用于动态生成路径
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("DWS to ADS Calculation with Controller and Wireless") \
        .getOrCreate()

    # 使用 process_time_range 生成日期列表
    date_list = process_time_range(start_date, end_date)

    # 遍历聚合列表
    for agg in dws_agg_list:
        # 遍历日期列表
        for date_str in date_list:
            controller_path = f"s3://{bucket}/{dws_prefix}/{agg}/controller_id/controller/dt={date_str}/*.parquet"
            wireless_path = f"s3://{bucket}/{dws_prefix}/{agg}/controller_id/wireless_data/dt={date_str}/*.parquet"

            # 读取 controller 的 network 表
            df_controller = spark.read.parquet(controller_path)

            # 读取 wireless 的 network 表
            df_wireless = spark.read.parquet(wireless_path)

            # 基于 collection_time_agg 和 controller_id 进行关联
            df_ads = df_controller.alias("c").join(
                df_wireless.alias("w"),
                (col("c.controller_id") == col("w.controller_id")) &
                (col("c.band") == col("w.band")) &
                (col("c.collection_time_agg") == col("w.collection_time_agg")),
                "left"
            ).select(
                col("c.controller_id"),  # 选择 controller_id
                col("c.band"),  # 选择 band
                col("c.collection_time"),  # 选择 collection_time_agg
                col("c.collection_time_agg"),  # 选择 collection_time_agg
                col("c.average_rx_rate"),  # 选择 average_rx_rate
                col("c.average_tx_rate"),  # 选择 average_tx_rate
                col("c.congestion_score"),  # 选择 congestion_score
                col("c.wifi_coverage_score"),  # 选择 wifi_coverage_score
                col("c.noise"),  # 选择 noise
                col("c.errors_rate"),  # 选择 errors_rate
                col("c.wan_bandwidth"),  # 选择 wan_bandwidth
                col("w.per_band_count"),  # 添加 wireless 的 per_band_count
                col("w.per_network_wireless_count"),  # 添加 wireless 的 per_network_wireless_count
                col("w.per_network_count")  # 添加 wireless 的 per_network_count
            )

            # 根据聚合维度写入到相应的 ADS 层路径
            df_ads.show(truncate=False)
            output_path = f"s3://{bucket}/{ads_output_prefix}/{agg}/network_ads/dt={date_str}/"
            df_ads.write.mode("overwrite").parquet(output_path, compression="snappy")

    # 停止SparkSession
    spark.stop()

if __name__ == "__main__":
    # 获取系统参数
    bucket = sys.argv[1]  # S3 bucket 名称
    dws_prefix = sys.argv[2]  # DWS 表的前缀路径
    ads_output_prefix = sys.argv[3]  # ADS 表的输出前缀路径
    dws_agg_list = sys.argv[4].split(',')  # 传入的聚合维度列表，逗号分隔
    start_date = sys.argv[5]  # 起始日期，格式为 'YYYY-MM-DD'
    end_date = sys.argv[6]  # 结束日期，格式为 'YYYY-MM-DD'

    # 调用计算函数
    calculate_ads_from_dws(bucket, dws_prefix, ads_output_prefix, dws_agg_list, start_date, end_date)
