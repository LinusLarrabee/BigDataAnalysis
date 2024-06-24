from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json

# 创建 SparkSession
spark = SparkSession.builder.appName("ExtractMessage").getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("INFO")

# 定义文件路径
file_path = "/Users/sunhao/message1.txt"

# 定义处理函数
def extract_data_collector(json_str):
    try:
        # 替换转义字符
        replaced_str = json_str.replace('\\\\', '\\').replace('\\"', '"')

        # 查找 dataCollectorDTO 和 timeStamp 的索引
        index_start = replaced_str.find('{"dataCollectorDTO')
        index_end = replaced_str.find('"timeStamp')

        # 检查索引是否合法
        if index_start == -1 or index_end == -1:
            return "string format error! str= " + replaced_str

        # 提取子字符串
        substring = replaced_str[index_start:index_end]

        # 返回提取的 JSON 子字符串
        return substring
    except Exception as e:
        return str(e)

# 注册 UDF
extract_data_collector_udf = udf(extract_data_collector, StringType())

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 dataCollectorDTO 部分
df_with_data_collector = df.withColumn("dataCollectorDTO", extract_data_collector_udf(col("value")))

# 显示结果
df_with_data_collector.select("dataCollectorDTO").show(truncate=False)
