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

print(f"Reading data from {input_path}")

# 从S3读取文件内容
file_content = sc.textFile(input_path).collect()
file_content = "\n".join(file_content)

print("File content read successfully.")

# 读取文件内容，每行一个 JSON 对象
try:
    json_list = [json.loads(line) for line in StringIO(file_content)]
    print("JSON parsing successful.")
except Exception as e:
    print(f"Error parsing JSON: {e}")
    spark.stop()
    sys.exit(1)

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
try:
    df = spark.createDataFrame(json_list, schema)
    print("DataFrame creation successful.")
except Exception as e:
    print(f"Error creating DataFrame: {e}")
    spark.stop()
    sys.exit(1)

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

# 将处理后的数据保存到指定的S3路径，不分区
temp_output_path = output_path + "_temp"
df.select("uvi", "transformed_path").coalesce(1).write.csv(temp_output_path, header=True, mode='overwrite')

print("Data saved successfully. Renaming the file...")

# 获取Hadoop文件系统
hadoop = sc._gateway.jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

# 找到临时输出目录中的文件
path = hadoop.fs.Path(temp_output_path)
files = fs.listStatus(path)
for file in files:
    if file.getPath().getName().startswith('part-'):
        temp_file_path = file.getPath()
        break

# 目标文件路径
final_output_path = hadoop.fs.Path(output_path)

# 重命名文件
fs.rename(temp_file_path, final_output_path)

# 删除临时目录
fs.delete(path, True)

# 删除.crc文件
crc_path = hadoop.fs.Path(output_path + '.crc')
if fs.exists(crc_path):
    fs.delete(crc_path, True)

print("File renamed and .crc file deleted successfully.")

# 关闭 SparkSession
spark.stop()
