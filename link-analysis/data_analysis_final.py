import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StringType, StructType, StructField

# 创建SparkSession
spark = SparkSession.builder.appName("ExtractMessage").getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("INFO")

# 定义数据的Schema
schema = StructType([
    StructField("messageId", StringType(), True),
    StructField("totalSize", StringType(), True),
    StructField("size", StringType(), True),
    StructField("totalIndex", StringType(), True),
    StructField("index", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("compressionType", StringType(), True),
    StructField("compressionLevel", StringType(), True),
    StructField("enableSlice", StringType(), True)
])

# 读取文件
file_path = "/Users/sunhao/message1.txt"
try:
    df = spark.read.text(file_path)
    print("文件读取成功。")
except Exception as e:
    print(f"文件读取失败: {e}", file=sys.stderr)
    sys.exit(1)

# 显示读取的前几行以进行检查
df.show(5, truncate=False)

# 检查文件内容
print("文件内容示例：")
for row in df.head(5):
    print(row.value)

# 解析JSON数据
try:
    df_json = df.select(from_json(col("value"), schema).alias("data"))
    print("JSON解析成功。")
except Exception as e:
    print(f"JSON解析失败: {e}", file=sys.stderr)
    sys.exit(1)

# 显示解析后的数据结构
df_json.show(5, truncate=False)

# 提取payload部分
try:
    df_payload = df_json.select(col("data.payload").alias("payload"))
    print("payload部分提取成功。")
    df_payload.show(5, truncate=False)
except Exception as e:
    print(f"提取payload部分失败: {e}", file=sys.stderr)
    sys.exit(1)

# 提取message部分
try:
    df_message = df_payload.select(get_json_object(col("payload"), "$.message").alias("message"))
    print("message部分提取成功。")
    df_message.show(truncate=False)
except Exception as e:
    print(f"提取message部分失败: {e}", file=sys.stderr)
    sys.exit(1)
