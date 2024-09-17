import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length
from datetime import datetime, timedelta
import logging

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

def calculate_errors_rate(bucket, input_prefix, output_prefix, start_date, end_date, mode='append'):
    """
    从 ODS 层计算 errors_rate 并保存到 DWD 层，跳过没有数据的日期。并处理 device_id 和计算 backhaul_sta_rssi。
    :param bucket: S3 bucket 名称
    :param input_prefix: ODS 表的输入前缀路径
    :param output_prefix: DWD 表的输出前缀路径
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("ODS to DWD - errors_rate Calculation") \
        .getOrCreate()

    # 获取时间范围内的所有日期
    date_list = process_time_range(start_date, end_date)

    for dt in date_list:
        input_path = f"s3://{bucket}/{input_prefix}/dt={dt}/*.parquet"

        # 检查是否有文件存在
        try:
            df_ods_ap = spark.read.parquet(input_path)
            logging.info(f"Processing date {dt} with input path {input_path}")
        except Exception as e:
            logging.warning(f"No data found for date {dt}, skipping. Error: {e}")
            continue
        df_ods_ap.show(truncate=False)

        # 过滤 device_id 长度为 17 的数据
        df_ods_ap = df_ods_ap.filter(
            (length(col("device_id")) == 17)
        # ).withColumn(
        #     "wifi_coverage_score",
        #     when(col("wifi_coverage_score") == '---', -1.0).otherwise(col("wifi_coverage_score"))
        )
        df_ods_ap.show(truncate=False)

        # 根据 is_controller 字段分为两张表
        df_controller = df_ods_ap.filter(col("is_controller") == 1)
        df_non_controller = df_ods_ap.filter(col("is_controller") == 0)

        # 删除 controller 中以 'backhaul_sta' 开头的列
        backhaul_sta_columns = [col_name for col_name in df_controller.columns if col_name.startswith("backhaul_sta")]
        df_controller_cleaned = df_controller.drop(*backhaul_sta_columns)

        # 对 non_controller 计算 backhaul_sta_rssi
        df_non_controller = df_non_controller.withColumn(
            "backhaul_sta_rssi",
            (col("backhaul_sta_signal_strength") / 2) - 110
        )

        # 计算 errors_rate，确保避免除零错误
        df_controller_dwd = df_controller_cleaned.withColumn(
            "errors_rate",
            when((col("packets_received") + col("packets_sent")) > 0,
                 col("errors_pkt") / (col("packets_received") + col("packets_sent"))
                 ).otherwise(0.0)
        )

        df_non_controller_dwd = df_non_controller.withColumn(
            "errors_rate",
            when((col("packets_received") + col("packets_sent")) > 0,
                 col("errors_pkt") / (col("packets_received") + col("packets_sent"))
                 ).otherwise(0.0)
        )

        # 分别写入到DWD层的Parquet文件（使用Snappy压缩）
        controller_output_path = f"s3://{bucket}/{output_prefix}/controller/dt={dt}/"
        non_controller_output_path = f"s3://{bucket}/{output_prefix}/non_controller/dt={dt}/"

        df_controller_dwd.write.mode(mode).parquet(controller_output_path, compression="snappy")
        df_non_controller_dwd.write.mode(mode).parquet(non_controller_output_path, compression="snappy")

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    # 获取命令行参数
    bucket = sys.argv[1]
    input_prefix = sys.argv[2]
    output_prefix = sys.argv[3]
    start_date = sys.argv[4]
    end_date = sys.argv[5]
    mode = sys.argv[6] if len(sys.argv) > 6 else 'append'  # 默认写入模式为 'append'

    # 调用计算函数
    calculate_errors_rate(bucket, input_prefix, output_prefix, start_date, end_date, mode)
