from pyspark.sql import SparkSession
import json

# 初始化 Spark 会话
spark = SparkSession.builder.appName("ProcessMessages").getOrCreate()

# 读取文本文件到 RDD
rdd = spark.sparkContext.textFile("/Users/sunhao/message.txt")

# 只读取第一行
first_line = rdd.first()

# 处理第一行数据，替换转义字符
processed_line = first_line.replace(r'\"', '"').replace(r'\\', '')

# 提取 JSON 数据中的 payload 和 message
json_data = json.loads(processed_line)
payload = json.loads(json_data["payload"])
message = json.loads(payload["message"])

# 创建 DataFrame
df = spark.createDataFrame([message], schema=None)

# 显示处理后的 DataFrame
df.show(truncate=False)
