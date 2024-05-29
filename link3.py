import json
import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, TimestampType
from io import StringIO

# MinIO 配置
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'mybucket'
FILE_KEY = 'path_chain_counts.txt'  # 请根据实际文件路径进行修改

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("PathTransformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.874") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# 初始化 MinIO 客户端
s3_client = boto3.client('s3',
                         endpoint_url=MINIO_ENDPOINT,
                         aws_access_key_id=MINIO_ACCESS_KEY,
                         aws_secret_access_key=MINIO_SECRET_KEY)

# 从 MinIO 读取文件内容
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
file_content = response['Body'].read().decode('utf-8')

# 读取文件内容，每行一个 JSON 对象
json_list = []
for line in StringIO(file_content):
    json_list.append(json.loads(line))

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

# 保存结果到 MinIO
df.select("uvi", "transformed_path").write.csv("s3a://mybucket/output/transformed_paths.csv", header=True)

# 关闭 SparkSession
spark.stop()
