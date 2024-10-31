from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import sys

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()

# 获取输入参数
bucket = sys.argv[1]
input_prefix = sys.argv[2]
output_prefix = sys.argv[3]
start_date = sys.argv[4]
end_date = sys.argv[5]


# 生成日期范围函数
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
    input_path = f's3a://{bucket}/{input_prefix}/{date_str}/messages-*.txt.gz'
    output_path = f's3a://{bucket}/{output_prefix}/{date_str}/'

    try:
        # 读取当前日期的数据
        df = spark.read.option("header", "true") \
            .option("compression", "gzip") \
            .csv(input_path)

        # 选择需要的列
        selected_columns = ['column1', 'column2', 'column3']  # 替换成你的实际列名
        df_selected = df.select(*selected_columns)

        # 写入 Parquet 文件，使用 Snappy 压缩
        df_selected.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)

        print(f"Successfully processed {date_str}")

    except Exception as e:
        print(f"Error processing {date_str}: {e}")

# 停止 SparkSession
spark.stop()
