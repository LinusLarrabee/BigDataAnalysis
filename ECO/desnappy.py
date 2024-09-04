from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
import gzip
import sys
from pyspark import SparkContext


# 读取 Snappy 压缩的 Parquet 文件
parquet_df = spark.read.parquet("s3://your-bucket/your-path-to-parquet-file")

# 显示 DataFrame 的前几行
parquet_df.show(truncate=False)

# 打印 DataFrame 的 schema
parquet_df.printSchema()
