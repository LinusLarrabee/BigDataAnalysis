from pyspark.sql.functions import col, when, from_unixtime, avg, first, min, max, date_format
from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession

def replace_invalid_numeric_values(df, numeric_cols):
    """
    统一替换 DataFrame 中所有指定 numeric_cols 列中的非正常值为 -1.0
    :param df: 输入的 DataFrame
    :param numeric_cols: 需要处理的数值列列表
    :return: 返回处理后的 DataFrame
    """
    for col_name in numeric_cols:
        df = df.withColumn(
            col_name,
            when(col(col_name) == '---', -1.0).otherwise(col(col_name))
        )
    return df

def aggregate_data(bucket, input_prefix, output_prefix, input_list, start_date, end_date):
    """
    从 S3 读取数据，并同时进行按小时和按天的聚合，使用两种不同的聚合方式（基于 controller_id 和 device_id）。
    :param bucket: S3 bucket 名称
    :param input_prefix: 数据的前缀路径
    :param output_prefix: 数据的输出前缀路径
    :param input_list: 包含多个路径的列表
    :param start_date: 起始日期，格式为 'YYYY-MM-DD'
    :param end_date: 结束日期，格式为 'YYYY-MM-DD'
    """
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("PySpark Aggregation on S3 with Snappy") \
        .getOrCreate()

    # 解析日期范围
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # 遍历日期范围
    while current_date <= end_date:
        dt = current_date.strftime('%Y-%m-%d')

        for input_path_suffix in input_list:
            input_path = f"s3://{bucket}/{input_prefix}/{input_path_suffix}/dt={dt}/*.parquet"
            output_path_hour_controller = f"s3://{bucket}/{output_prefix}/hourly/controller_id/{input_path_suffix}/dt={dt}/"
            output_path_day_controller = f"s3://{bucket}/{output_prefix}/daily/controller_id/{input_path_suffix}/dt={dt}/"
            output_path_hour_device = f"s3://{bucket}/{output_prefix}/hourly/device_id/{input_path_suffix}/dt={dt}/"
            output_path_day_device = f"s3://{bucket}/{output_prefix}/daily/device_id/{input_path_suffix}/dt={dt}/"

            # 读取S3上以Parquet格式存储的文件，带有Snappy压缩
            try:
                df = spark.read.parquet(input_path)
                print(f"Processing data for date: {dt}, path: {input_path_suffix}")
            except Exception as e:
                print(f"No data found for date {dt} and path {input_path_suffix}, skipping. Error: {e}")
                continue

            # 如果存在dt字段，移除该字段
            if 'dt' in df.columns:
                df = df.drop('dt')

            # 将字符串和数值字段区分开
            string_columns = [field for field, dtype in df.dtypes if dtype == 'string']
            numeric_columns = [field for field, dtype in df.dtypes if dtype in ['int', 'double', 'float']]

            # 1. 基于 controller_id 和 band 进行聚合
            key_columns_controller = ['controller_id', 'band']

            # 按小时聚合
            df_hour = df.withColumn("collection_time_agg", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd HH:00:00"))
            agg_by_hour_controller = df_hour.groupBy(*key_columns_controller, "collection_time_agg").agg(
                *[first(col(c)).alias(c) for c in string_columns if c not in key_columns_controller],  # 对字符串字段取单一值
                *[avg(col(c)).alias(c) for c in numeric_columns if c not in key_columns_controller],    # 对数值字段取平均值
                *[min(col(c)).alias(f"min_{c}") for c in numeric_columns if c not in key_columns_controller],    # 对数值字段取最小值
                *[max(col(c)).alias(f"max_{c}") for c in numeric_columns if c not in key_columns_controller]     # 对数值字段取最大值
            )

            # 按天聚合
            df_day = df.withColumn("collection_time_agg", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd"))
            agg_by_day_controller = df_day.groupBy(*key_columns_controller, "collection_time_agg").agg(
                *[first(col(c)).alias(c) for c in string_columns if c not in key_columns_controller],  # 对字符串字段取单一值
                *[avg(col(c)).alias(c) for c in numeric_columns if c not in key_columns_controller],    # 对数值字段取平均值
                *[min(col(c)).alias(f"min_{c}") for c in numeric_columns if c not in key_columns_controller],    # 对数值字段取最小值
                *[max(col(c)).alias(f"max_{c}") for c in numeric_columns if c not in key_columns_controller]     # 对数值字段取最大值
            )

            # 2. 基于 device_id 和 band 进行聚合
            key_columns_device = ['device_id', 'band']

            # 按小时聚合
            agg_by_hour_device = df_hour.groupBy(*key_columns_device, "collection_time_agg").agg(
                *[first(col(c)).alias(c) for c in string_columns if c not in key_columns_device],  # 对字符串字段取单一值
                *[avg(col(c)).alias(c) for c in numeric_columns if c not in key_columns_device],    # 对数值字段取平均值
                *[min(col(c)).alias(f"min_{c}") for c in numeric_columns if c not in key_columns_device],    # 对数值字段取最小值
                *[max(col(c)).alias(f"max_{c}") for c in numeric_columns if c not in key_columns_device]     # 对数值字段取最大值
            )

            # 按天聚合
            agg_by_day_device = df_day.groupBy(*key_columns_device, "collection_time_agg").agg(
                *[first(col(c)).alias(c) for c in string_columns if c not in key_columns_device],  # 对字符串字段取单一值
                *[avg(col(c)).alias(c) for c in numeric_columns if c not in key_columns_device],    # 对数值字段取平均值
                *[min(col(c)).alias(f"min_{c}") for c in numeric_columns if c not in key_columns_device],    # 对数值字段取最小值
                *[max(col(c)).alias(f"max_{c}") for c in numeric_columns if c not in key_columns_device]     # 对数值字段取最大值
            )

            # 聚合后处理数值列中的异常值
            agg_by_hour_controller = replace_invalid_numeric_values(agg_by_hour_controller, numeric_columns)
            agg_by_day_controller = replace_invalid_numeric_values(agg_by_day_controller, numeric_columns)
            agg_by_hour_device = replace_invalid_numeric_values(agg_by_hour_device, numeric_columns)
            agg_by_day_device = replace_invalid_numeric_values(agg_by_day_device, numeric_columns)

            # 显示聚合结果
            print(f"按小时聚合结果 for {dt} (controller_id):")
            agg_by_hour_controller.show()
            print(f"按天聚合结果 for {dt} (controller_id):")
            agg_by_day_controller.show()

            print(f"按小时聚合结果 for {dt} (device_id):")
            agg_by_hour_device.show()
            print(f"按天聚合结果 for {dt} (device_id):")
            agg_by_day_device.show()

            # 写入到 DWD 层的 Parquet 文件，按小时和按天分别保存（使用 Snappy 压缩）
            agg_by_hour_controller.write.mode("overwrite").parquet(output_path_hour_controller, compression="snappy")
            agg_by_day_controller.write.mode("overwrite").parquet(output_path_day_controller, compression="snappy")

            agg_by_hour_device.write.mode("overwrite").parquet(output_path_hour_device, compression="snappy")
            agg_by_day_device.write.mode("overwrite").parquet(output_path_day_device, compression="snappy")

        # 处理下一个日期
        current_date += timedelta(days=1)

    # 停止SparkSession
    spark.stop()

if __name__ == "__main__":
    # 获取命令行参数
    bucket = sys.argv[1]  # S3 bucket
    input_prefix = sys.argv[2]  # 输入前缀路径
    output_prefix = sys.argv[3]  # 输出前缀路径
    input_list = sys.argv[4].split(',')  # 输入的多个子路径，逗号分隔
    start_date = sys.argv[5]  # 起始日期，格式为 'YYYY-MM-DD'
    end_date = sys.argv[6]  # 结束日期，格式为 'YYYY-MM-DD'

    # 调用聚合函数
    aggregate_data(bucket, input_prefix, output_prefix, input_list, start_date, end_date)
