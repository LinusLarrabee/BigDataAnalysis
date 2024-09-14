import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from datetime import datetime, timedelta
import logging

def calculate_errors_rate(bucket, input_prefix, output_prefix, start_date, end_date):
    """
    从 ODS 层计算 errors_rate 并按日期分区保存到 DWD 层，跳过没有数据的日期。
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

    # 转换日期格式
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    while current_date <= end_date:
        dt = current_date.strftime('%Y-%m-%d')
        input_path = f"s3://{bucket}/{input_prefix}/dt={dt}/*.parquet"

        # 检查是否有文件存在
        try:
            df_ods_ap = spark.read.parquet(input_path)
            logging.info(f"Processing date {dt} with input path {input_path}")
        except Exception as e:
            logging.warning(f"No data found for date {dt}, skipping. Error: {e}")
            current_date += timedelta(days=1)
            continue

        # 计算errors_rate，确保避免除零错误
        df_dwd_ap = df_ods_ap.withColumn(
            "errors_rate",
            when((col("packets_received") + col("packets_sent")) > 0,
                 col("errors_pkt") / (col("packets_received") + col("packets_sent"))
                 ).otherwise(0.0)
        )

        # 写入到DWD层的Parquet文件，按日期分区保存（使用Snappy压缩）
        output_path = f"s3://{bucket}/{output_prefix}/"
        df_dwd_ap.write.mode("overwrite").parquet(output_path, compression="snappy")
        df_dwd_ap.printSchema()

        # 处理下一个日期
        current_date += timedelta(days=1)

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    # 获取命令行参数
    bucket = sys.argv[1]
    input_prefix = sys.argv[2]
    output_prefix = sys.argv[3]
    start_date = sys.argv[4]
    end_date = sys.argv[5]

    # 调用计算函数
    calculate_errors_rate(bucket, input_prefix, output_prefix, start_date, end_date)
