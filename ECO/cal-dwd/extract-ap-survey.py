from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from datetime import datetime, timedelta
import sys

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion with Column Selection") \
    .getOrCreate()

# 获取输入参数
bucket = sys.argv[1]
input_prefix = sys.argv[2]  # 应该是 "source/ap_survey"
output_prefix = sys.argv[3]
start_date = sys.argv[4]
end_date = sys.argv[5] if len(sys.argv) > 4 else None  # 如果没有提供 end_date，则设为 None

# 定义 schema，仅包含需要的字段
schema = StructType([
    StructField("isp_admin_id", StringType(), nullable=False),
    StructField("device_id", StringType(), nullable=False),
    StructField("band", StringType(), nullable=False),
    StructField("bssid", StringType(), nullable=False),
    StructField("signal_strength", IntegerType(), nullable=False),
    StructField("channel", IntegerType(), nullable=False),
    StructField("bandwidth", StringType(), nullable=False),
    StructField("message_time", LongType(), nullable=False)
])

# 定义日期范围生成函数
def generate_date_range(start, end=None):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else start_dt
    date_range = []
    while start_dt <= end_dt:
        date_range.append(start_dt.strftime("%Y-%m-%d"))
        start_dt += timedelta(days=1)
    return date_range

def read_and_write_data(input_path, output_path, schema, date_str):
    """读取 CSV 文件并写入为 Parquet 格式，仅包含指定列。"""
    try:
        # 读取 CSV 文件并筛选所需列
        df = spark.read.option("header", "true") \
            .option("compression", "gzip") \
            .csv(input_path) \
            .select("isp_admin_id", "device_id", "band", "bssid", "signal_strength", "channel", "bandwidth", "message_time")

        # 写入 Parquet 文件，使用 Snappy 压缩
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)

        print(f"Successfully processed data for date: {date_str}")

    except Exception as e:
        print(f"Error processing data for date {date_str}: {e}")

def process_data_for_date(date_str, bucket, input_prefix, output_prefix, schema):
    input_path = f's3a://{bucket}/{input_prefix}/dt={date_str}/ap_survey_*.csv.gz'
    output_path = f's3a://{bucket}/{output_prefix}/dt={date_str}/'
    read_and_write_data(input_path, output_path, schema, date_str)

def process_data_from_local(date_str, schema):
    """从本地读取 CSV 数据并写入为 Parquet 格式，仅包含指定列。"""
    input_path = f'/Users/sunhao/s3/ap_survey/dt={date_str}/ap_survey_*.csv.gz'
    output_path = f'/Users/sunhao/s3/output/dt={date_str}/'  # 本地输出路径
    read_and_write_data(input_path, output_path, schema, date_str)

# 循环处理每一天的数据
for date_str in generate_date_range(start_date, end_date):
    # process_data_from_local(date_str, schema)
    process_data_for_date(date_str, bucket, input_prefix, output_prefix, schema)

# 停止 SparkSession
spark.stop()
