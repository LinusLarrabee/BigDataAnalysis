from pyspark.sql.functions import col, first, sum as _sum, coalesce
import sys
from pyspark.sql import SparkSession


def calculate_dwm_with_wire(bucket, input_prefix, output_prefix, start_date, end_date):
    """
    计算每个 device 和 network 的 DWM (sta_count 聚合结果)，
    对于 wireless 按原来的逻辑进行计算，同时加入 wire 的数据并进行合并计算。
    :param bucket: S3 bucket 名称
    :param input_prefix: 数据的前缀路径
    :param output_prefix: 输出的前缀路径
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("Client DWM Calculation with Wire") \
        .getOrCreate()

    # 读取 wireless 数据
    wireless_path = f"s3://{bucket}/{input_prefix}/wireless_data/dt={start_date}/*.parquet"
    df_wireless = spark.read.parquet(wireless_path)

    # 读取 wire 数据 (假设 wire 数据和 wireless 数据相同的 schema，除了 band 字段)
    wire_path = f"s3://{bucket}/{input_prefix}/wire_data/dt={start_date}/*.parquet"
    df_wire = spark.read.parquet(wire_path)

    # 对 wireless 数据按 band, collection_time, controller_id, device_id 聚合
    df_wireless_band_first = df_wireless.groupBy("band", "collection_time", "controller_id", "device_id").agg(
        first("sta_count").alias("sta_count")
    )

    # 对 wire 数据按 collection_time, controller_id, device_id 聚合
    df_wire_first = df_wire.groupBy("collection_time", "controller_id", "device_id").agg(
        first("sta_count").alias("sta_count")
    )

    # 对 wireless 的设备级别进行聚合
    df_wireless_device_agg = df_wireless_band_first.groupBy("device_id", "collection_time").agg(
        _sum("sta_count").alias("wireless_sta_count")
    )

    # 对 wire 的设备级别进行聚合
    df_wire_device_agg = df_wire_first.groupBy("device_id", "collection_time").agg(
        _sum("sta_count").alias("wire_sta_count")
    )

    # 合并 wire 和 wireless 的设备级别聚合结果
    df_device_agg = df_wireless_device_agg.join(
        df_wire_device_agg, ["device_id", "collection_time"], "left"
    ).select(
        "device_id", "collection_time",
        "wireless_sta_count",
        "wire_sta_count",
        (coalesce(col("wireless_sta_count"), col("wireless_sta_count"))) +
        (coalesce(col("wire_sta_count"), col("wire_sta_count"))).alias("per_device_sta_count")
    )

    # 对 wireless 的网络级别进行聚合
    df_wireless_network_agg = df_wireless_band_first.groupBy("controller_id", "collection_time").agg(
        _sum("sta_count").alias("wireless_per_network_sta_count")
    )

    # 对 wire 的网络级别进行聚合
    df_wire_network_agg = df_wire_first.groupBy("controller_id", "collection_time").agg(
        _sum("sta_count").alias("wire_per_network_sta_count")
    )

    # 合并 wire 和 wireless 的网络级别聚合结果
    df_network_agg = df_wireless_network_agg.join(
        df_wire_network_agg, ["controller_id", "collection_time"], "left"
    ).select(
        "controller_id", "collection_time",
        "wireless_per_network_sta_count",
        "wire_per_network_sta_count",
        (coalesce(col("wireless_per_network_sta_count"), col("wireless_per_network_sta_count"))) +
        (coalesce(col("wire_per_network_sta_count"), col("wire_per_network_sta_count"))).alias("per_network_sta_count")
    )

    # 将设备级别的聚合结果写回每一行
    df_with_device_agg = df_wireless_band_first.join(df_device_agg, ["device_id", "collection_time"], "left")

    # 将网络级别的聚合结果写回每一行
    df_with_all_agg = df_with_device_agg.join(df_network_agg, ["controller_id", "collection_time"], "left")

    # 将最终结果表写入 Parquet 文件
    output_path = f"s3://{bucket}/{output_prefix}/dwd_with_dwm/dt={start_date}/"
    df_with_all_agg.write.mode("overwrite").parquet(output_path, compression="snappy")

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    bucket = sys.argv[1]  # S3 bucket 名称
    input_prefix = sys.argv[2]  # 输入的前缀路径
    output_prefix = sys.argv[3]  # 输出的前缀路径
    start_date = sys.argv[4]  # 起始日期
    end_date = sys.argv[5]  # 结束日期

    # 调用函数计算 DWM 并写入到 DWD 表
    calculate_dwm_with_wire(bucket, input_prefix, output_prefix, start_date, end_date)
