from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 定义处理函数
def extract_qoe(json_str):
    try:
        replaced_str = json_str.replace('\\', '')
        index_start = replaced_str.find("{\"filterKey")
        index_end = replaced_str.find("\",\"timeStamp")

        if index_start == -1 or index_end == -1:
            return "string format error! str= " + replaced_str

        substring = replaced_str[index_start:index_end]
        return substring
    except Exception as e:
        return str(e)


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadSingleFileJSON") \
    .getOrCreate()

# 指定文件路径
file_path = "messages-1721813944482.txt"

extract_udf = udf(extract_qoe, StringType())

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 dataCollectorDTO 部分
df_qoe = df.withColumn("QoeData", extract_udf(col("value")))

df_qoe.show()

# 停止SparkSession
spark.stop()
