# hello_world.py

from pyspark.sql import SparkSession
import argparse

# 创建参数解析器
parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="Date parameter in YYYY-MM-DD format")
args = parser.parse_args()

# 初始化 Spark 会话
spark = SparkSession.builder.appName("HelloWorldApp").getOrCreate()

# 打印 Hello, World! 和日期
print(f"Hello, World! The provided date is: {args.date}")

# 停止 Spark 会话
spark.stop()
