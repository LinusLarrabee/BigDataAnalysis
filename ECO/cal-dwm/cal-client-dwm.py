from pyspark.sql.functions import col, first, sum as _sum
import sys
from pyspark.sql import SparkSession


def calculate_dwm(bucket, input_prefix, output_prefix, start_date, end_date):
    """
    计算每个 device 和 network 的 DWM (sta_count 聚合结果) 并将其结果写回每一行。
    :param bucket: S3 bucket 名称
    :param input_prefix: 数据的前缀路径
    :param output_prefix: 输出的前缀路径
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("Client DWM Calculation") \
        .getOrCreate()

    # 读取 wireless 数据表
    input_path = f"s3://{bucket}/{input_prefix}/wireless_data/dt={start_date}/*.parquet"
    df = spark.read.parquet(input_path)

    # 按 band, collection_time, controller_id, device_id 进行聚合，获取每个 band 分类的第一条数据的 sta_count
    df_band_first = df.groupBy("band", "collection_time", "controller_id", "device_id").agg(
        first("sta_count").alias("sta_count")
    )

    # 按 device_id 和 collection_time 聚合，计算每个设备的 sta_count 总和 (per device)
    df_device_agg = df_band_first.groupBy("device_id", "collection_time").agg(
        _sum("sta_count").alias("per_device_sta_count")
    )

    # 按 controller_id 和 collection_time 聚合，计算每个网络的 sta_count 总和 (per network)
    df_network_agg = df_band_first.groupBy("controller_id", "collection_time").agg(
        _sum("sta_count").alias("per_network_sta_count")
    )

    # 将 device 的聚合结果写回到每一行数据中，使用 join 操作
    df_with_device_agg = df_band_first.join(df_device_agg, ["device_id", "collection_time"], "left")

    # 将 network 的聚合结果写回到每一行数据中
    df_with_all_agg = df_with_device_agg.join(df_network_agg, ["controller_id", "collection_time"], "left")

    # 最终的结果表包括每行的 per_device_sta_count 和 per_network_sta_count
    output_path = f"s3://{bucket}/{output_prefix}/dt={start_date}/"
    df_with_all_agg.write.mode("overwrite").parquet(output_path, compression="snappy")

    # 停止SparkSession
    spark.stop()

if __name__ == "__main__":
    bucket = sys.argv[1]  # S3 bucket 名称
    input_prefix = sys.argv[2]  # 输入的前缀路径
    output_prefix = sys.argv[3]  # 输出的前缀路径
    start_date = sys.argv[4]  # 起始日期
    end_date = sys.argv[5]  # 结束日期

    # 调用函数计算 DWM
    calculate_dwm(bucket, input_prefix, output_prefix, start_date, end_date)
