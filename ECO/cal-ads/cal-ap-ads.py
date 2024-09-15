import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def calculate_ads_from_dws(bucket, dws_prefix, ads_output_prefix, start_date, end_date, dws_agg_list):
    """
    从 DWS 层计算 ADS 层，以 controller 的 network 表为主表，基于 controller_id 进行关联，添加 noncontroller 的 backhaul_sta_rssi 和 linkrate。
    :param bucket: S3 bucket 名称
    :param dws_prefix: DWS 表的前缀路径
    :param ads_output_prefix: ADS 表的输出前缀路径
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    :param dws_agg_list: 包含 'daily', 'hourly' 的列表，用于动态生成路径
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("DWS to ADS Calculation with Controller ID Join") \
        .getOrCreate()

    # 遍历聚合列表
    for agg in dws_agg_list:
        controller_path = f"s3://{bucket}/{dws_prefix}/{agg}/controller_id/dt={start_date}/*.parquet"
        noncontroller_path = f"s3://{bucket}/{dws_prefix}/{agg}/device_id/dt={start_date}/*.parquet"

        # 读取 controller 的 network 表
        df_controller = spark.read.parquet(controller_path)

        # 读取 noncontroller 的 network 表
        df_noncontroller = spark.read.parquet(noncontroller_path)

        # 基于 controller_id 进行关联
        df_ads = df_controller.alias("c").join(
            df_noncontroller.alias("n"),
            (col("c.controller_id") == col("n.controller_id")) &
            (col("c.band") == col("n.band")) &
            (col("c.collection_time") == col("n.collection_time")),
            "left"
        ).select(
            "c.*",  # 保留 controller 的所有字段
            "n.backhaul_sta_rssi",  # 添加 noncontroller 的 backhaul_sta_rssi
            "n.linkrate"  # 添加 noncontroller 的 linkrate
        )

        # 根据聚合维度写入到相应的 ADS 层路径
        output_path = f"s3://{bucket}/{ads_output_prefix}/{agg}/network_ads/dt={start_date}/"
        df_ads.write.mode("overwrite").parquet(output_path, compression="snappy")

    # 停止SparkSession
    spark.stop()

if __name__ == "__main__":
    # 获取系统参数
    bucket = sys.argv[1]  # S3 bucket 名称
    dws_prefix = sys.argv[2]  # DWS 表的前缀路径
    ads_output_prefix = sys.argv[3]  # ADS 表的输出前缀路径
    start_date = sys.argv[4]  # 起始日期，格式为 'YYYY-MM-DD'
    end_date = sys.argv[5]  # 结束日期，格式为 'YYYY-MM-DD'
    dws_agg_list = sys.argv[6].split(',')  # 传入的聚合维度列表，逗号分隔

    # 调用计算函数
    calculate_ads_from_dws(bucket, dws_prefix, ads_output_prefix, start_date, end_date, dws_agg_list)
