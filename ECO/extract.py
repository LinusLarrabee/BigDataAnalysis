from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType


# 定义处理函数
def extract_qoe(json_str):
    try:
        # 去除转义
        replaced_str = json_str.replace('\\', '')

        # 查找kafka消息层
        type_start = replaced_str.find("{\"filterKey\":\"")
        type_end = replaced_str.find("\",\"message\":\"{")

        if type_start == -1 or type_end == -1:
            return "string format error! str= " + replaced_str


        qoe_type = replaced_str[type_start + 14:type_end]  # Ensure the substring is complete


        # 查找Qoe内容
        data_start = replaced_str.find('{"collectionRecords')
        data_end = replaced_str.find('"collectionTime')
        if type_start == -1 or type_end == -1:
            return "string format error! str= " + replaced_str
        qoe_data = replaced_str[data_start + 14:data_end]
        return {
            "QoeType": qoe_type,
            "QoeData": qoe_data
        }
    except Exception as e:
        return str(e)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadSingleFileJSON") \
    .getOrCreate()

# 指定文件路径
file_path = "messages-1721813944482.txt"

# 定义 UDF 返回的 schema
schema = StructType([
    StructField("QoeType", StringType(), True),
    StructField("QoeData", StringType(), True)
])

# 注册 UDF
extract_udf = udf(extract_qoe, schema)

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
df_qoe_kind = df.withColumn("Qoe", extract_udf(col("value"))).select(col("Qoe.*"))

# 显示结果
df_qoe_kind.show(truncate=False)

# 停止SparkSession
spark.stop()
