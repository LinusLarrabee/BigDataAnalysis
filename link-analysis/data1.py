import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from io import StringIO

# 获取输入和输出路径
input_path = sys.argv[1]
output_path = sys.argv[2]

# 创建SparkContext和SparkSession
sc = SparkContext(appName="PathTransformation")
spark = SparkSession(sc)

# 从S3读取文件内容
file_content = sc.textFile(input_path).collect()
file_content = "\n".join(file_content)

# 读取文件内容，每行一个 JSON 对象
json_list = [json.loads(line) for line in StringIO(file_content)]

# 定义数据模式
schema = StructType([
    StructField("uvi", StringType(), True),
    StructField("el", ArrayType(
        StructType([
            StructField("path", StringType(), True),
            StructField("ct", StringType(), True)  # 使用StringType读取时间戳
        ])
    ), True)
])

# 创建 DataFrame
df = spark.createDataFrame(json_list, schema)

# 转换时间戳字段为 TimestampType
df = df.withColumn("el", F.explode("el")) \
    .withColumn("path", F.col("el.path")) \
    .withColumn("ct", F.col("el.ct")) \
    .drop("el") \
    .withColumn("ct", F.to_timestamp("ct"))

# 重新聚合数据
df = df.groupBy("uvi").agg(F.collect_list(F.struct("path", "ct")).alias("el"))

# 处理路径转换
def transform_path(el):
    paths = [e["path"] for e in el]
    transformed_path = " -> ".join(paths)
    return transformed_path

transform_path_udf = F.udf(transform_path, StringType())

df = df.withColumn("transformed_path", transform_path_udf(F.col("el")))

# 展示结果
df.select("uvi", "transformed_path").show(truncate=False)

# 保存结果到 S3
df.select("uvi", "transformed_path").write.csv(output_path, header=True)

# 关闭 SparkSession
spark.stop()
