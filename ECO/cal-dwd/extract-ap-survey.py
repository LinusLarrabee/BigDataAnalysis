from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from datetime import datetime, timedelta
import sys

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()

# 获取输入参数
bucket = sys.argv[1]
input_prefix = sys.argv[2]  # 应该是 "source/ap_survey"
output_prefix = sys.argv[3]
start_date = sys.argv[4]
end_date = sys.argv[5]

# 定义 schema
schema = StructType([
    StructField("isp_admin_id", StringType(), nullable=False),
    StructField("device_id", StringType(), nullable=False),
    StructField("band", StringType(), nullable=False),
    StructField("bssid", StringType(), nullable=False),
    StructField("signal_strength", IntegerType(), nullable=False),
    StructField("channel", IntegerType(), nullable=False),
    StructField("bandwidth", StringType(), nullable=False),
    StructField("message_time", LongType(), nullable=False),
    StructField("version", IntegerType(), nullable=True)
])

# 定义日期范围生成函数
def generate_date_range(start, end):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    date_range = []
    while start_dt <= end_dt:
        date_range.append(start_dt.strftime("%Y-%m-%d"))
        start_dt += timedelta(days=1)
    return date_range

# 循环处理每一天的数据
for date_str in generate_date_range(start_date, end_date):
    input_path = f's3a://{bucket}/{input_prefix}/dt={date_str}/ap_survey_*.csv.gz'
    output_path = f's3a://{bucket}/{output_prefix}/dt={date_str}/'

    try:
        # 读取 CSV 文件，Spark 会自动读取多个匹配的文件
        df = spark.read.option("header", "true") \
            .option("compression", "gzip") \
            .schema(schema) \
            .csv(input_path)

        # 写入 Parquet 文件，使用 Snappy 压缩
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)

        print(f"Successfully processed data for date: {date_str}")

    except Exception as e:
        print(f"Error processing data for date {date_str}: {e}")

# 停止 SparkSession
spark.stop()
