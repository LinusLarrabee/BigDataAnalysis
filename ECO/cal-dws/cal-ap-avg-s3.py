import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, avg, first, date_format
from datetime import datetime, timedelta

def aggregate_data(bucket, input_prefix, output_prefix, start_date, end_date):
    """
    从 S3 读取数据，并同时进行按小时和按天的聚合。
    :param bucket: S3 bucket 名称
    :param input_prefix: 数据的前缀路径
    :param output_prefix: 数据的输出前缀路径
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
        input_path = f"s3://{bucket}/{input_prefix}/dt={dt}/*.parquet"

        # 读取S3上以Parquet格式存储的文件，带有Snappy压缩
        try:
            df = spark.read.parquet(input_path)
            print(f"Processing data for date: {dt}")
        except Exception as e:
            print(f"No data found for date {dt}, skipping. Error: {e}")
            current_date += timedelta(days=1)
            continue

        # 如果存在dt字段，移除该字段
        if 'dt' in df.columns:
            df = df.drop('dt')

        # 添加小时和天的时间列
        df = df.withColumn("collection_time_hour", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd HH:00:00"))
        df = df.withColumn("collection_time_day", date_format(from_unixtime(col("collection_time")), "yyyy-MM-dd"))

        # 聚合的唯一性判断字段
        key_columns_hour = ['controller_id', 'device_id', 'band', 'collection_time_hour']
        key_columns_day = ['controller_id', 'device_id', 'band', 'collection_time_day']


        # 将字符串和数值字段区分开
        string_columns = [field for field, dtype in df.dtypes if dtype == 'string' and field not in key_columns_hour and field not in key_columns_day]
        numeric_columns = [field for field, dtype in df.dtypes if dtype in ['int', 'double', 'float'] and field not in key_columns_hour and field not in key_columns_day]

        # 按小时聚合
        agg_by_hour = df.groupBy(*key_columns_hour).agg(
            *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
            *[avg(col(c)).alias(c) for c in numeric_columns]
            # *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
        )

        # 按天聚合
        agg_by_day = df.groupBy(*key_columns_day).agg(
            *[first(col(c)).alias(c) for c in string_columns],  # 对字符串字段取单一值
            *[avg(col(c)).alias(c) for c in numeric_columns]
            # *[avg(col(c)).alias(f"avg_{c}") for c in numeric_columns]  # 对数值字段取平均值
        )

        # 显示聚合结果
        print(f"按小时聚合结果 for {dt}:")
        agg_by_hour.show()
        agg_by_hour.printSchema()

        print(f"按天聚合结果 for {dt}:")
        agg_by_day.show()
        agg_by_day.printSchema()

        # 写入到DWD层的Parquet文件，按小时和按天分别保存（使用Snappy压缩）
        output_path_hour = f"s3://{bucket}/{output_prefix}/hourly/dt={dt}/"
        output_path_day = f"s3://{bucket}/{output_prefix}/daily/dt={dt}/"

        agg_by_hour.write.mode("overwrite").parquet(output_path_hour, compression="snappy")
        agg_by_day.write.mode("overwrite").parquet(output_path_day, compression="snappy")

        # 处理下一个日期
        current_date += timedelta(days=1)

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    # 获取命令行参数
    bucket = sys.argv[1]  # S3 bucket
    input_prefix = sys.argv[2]  # 输入前缀路径
    output_prefix = sys.argv[3]  # 输出前缀路径
    start_date = sys.argv[4]  # 起始日期，格式为 'YYYY-MM-DD'
    end_date = sys.argv[5]  # 结束日期，格式为 'YYYY-MM-DD'

    # 调用聚合函数
    aggregate_data(bucket, input_prefix, output_prefix, start_date, end_date)
