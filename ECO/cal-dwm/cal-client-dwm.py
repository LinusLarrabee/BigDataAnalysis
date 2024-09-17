from pyspark.sql.functions import col, first, sum as _sum, coalesce
from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession


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


def calculate_dwm_with_wire(bucket, input_prefix, output_prefix, start_date, end_date):
    """
    计算每个 device 和 network 的 DWM (sta_count 聚合结果)，
    对于 wire 和 wireless 分别计算，并在需要时合并聚合计算写入 DWM 层。
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

    # 获取日期范围列表
    date_list = process_time_range(start_date, end_date)

    for dt in date_list:
        print(f"Processing date: {dt}")

        # 读取 wireless 数据
        wireless_path = f"s3://{bucket}/{input_prefix}/wireless_data/dt={dt}/*.parquet"
        try:
            df_wireless = spark.read.parquet(wireless_path)
            wireless_exists = True
        except Exception:
            print(f"Wireless data not found for date {dt}")
            wireless_exists = False

        # 读取 wire 数据
        wire_path = f"s3://{bucket}/{input_prefix}/wire_data/dt={dt}/*.parquet"
        try:
            df_wire = spark.read.parquet(wire_path)
            wire_exists = True
        except Exception:
            print(f"Wire data not found for date {dt}")
            wire_exists = False

        if wire_exists:
            # 计算 wire 表的 device 和 network 级别聚合
            df_wire_device_agg = df_wire.groupBy("device_id", "collection_time").agg(
                _sum("sta_count").alias("per_device_wire_count")
            )

            df_wire_network_agg = df_wire.groupBy("controller_id", "collection_time").agg(
                _sum("sta_count").alias("per_network_wire_count")
            )

            # 写入 wire 的 DWM 层
            wire_output_path = f"s3://{bucket}/{output_prefix}/wire_data/dt={dt}/"
            df_wire_final = df_wire.join(df_wire_device_agg, ["device_id", "collection_time"], "left") \
                .join(df_wire_network_agg, ["controller_id", "collection_time"], "left")
            df_wire_final.write.mode("overwrite").parquet(wire_output_path, compression="snappy")

        if wireless_exists:
            # 计算 wireless 表的 device 和 network 级别聚合
            df_wireless_band_first = df_wireless.groupBy("band", "collection_time", "controller_id", "device_id").agg(
                first("sta_count").alias("sta_count")
            )

            df_wireless_device_agg = df_wireless_band_first.groupBy("device_id", "collection_time").agg(
                _sum("sta_count").alias("per_device_wireless_count")
            )

            df_wireless_network_agg = df_wireless_band_first.groupBy("controller_id", "collection_time").agg(
                _sum("sta_count").alias("per_network_wireless_count")
            )

            if wire_exists:
                # 如果 wire 和 wireless 都存在，合并 device 和 network 聚合结果
                df_device_agg = df_wireless_device_agg.join(
                    df_wire_device_agg, ["device_id", "collection_time"], "left"
                ).select(
                    "device_id", "collection_time",
                    "per_device_wireless_count",
                    "per_device_wire_count",
                    (coalesce(col("per_device_wireless_count"), col("per_device_wireless_count")) +
                     coalesce(col("per_device_wire_count"), col("per_device_wire_count"))).alias("per_device_count")
                )

                df_network_agg = df_wireless_network_agg.join(
                    df_wire_network_agg, ["controller_id", "collection_time"], "left"
                ).select(
                    "controller_id", "collection_time",
                    "per_network_wireless_count",
                    "per_network_wire_count",
                    (coalesce(col("per_network_wireless_count"), col("per_network_wireless_count")) +
                     coalesce(col("per_network_wire_count"), col("per_network_wire_count"))).alias("per_network_count")
                )
            else:
                # 如果只有 wireless 数据
                df_device_agg = df_wireless_device_agg.withColumnRenamed("per_device_wireless_count", "per_device_count")
                df_network_agg = df_wireless_network_agg.withColumnRenamed("per_network_wireless_count", "per_network_count")

            # 更新写入逻辑，确保带上原始wireless数据
            df_wireless_final = df_wireless.join(df_device_agg, ["device_id", "collection_time"], "left") \
                .join(df_network_agg, ["controller_id", "collection_time"], "left") \
                .select(df_wireless["*"],
                        "per_device_wireless_count",
                        "per_device_wire_count",
                        "per_device_count",
                        "per_network_wireless_count",
                        "per_network_wire_count",
                        "per_network_count")

            # 写入聚合后的最终结果到无线的DWM层
            wireless_output_path = f"s3://{bucket}/{output_prefix}/wireless_data/dt={dt}/"
            df_wireless_final.show(truncate=False)
            df_wireless_final.write.mode("overwrite").parquet(wireless_output_path, compression="snappy")

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
