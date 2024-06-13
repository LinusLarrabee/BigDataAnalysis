from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

# 初始化 SparkContext 和 SparkSession
sc = SparkContext(appName="PathTransformation")
spark = SparkSession.builder \
    .appName(sc.appName) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.warehouse.dir", "s3://linkanalysis/warehouse/") \
    .enableHiveSupport() \
    .getOrCreate()

# 示例 DataFrame
data = [(1, "path1"), (2, "path2"), (3, "path3")]
columns = ["uvi", "transformed_path"]
df = spark.createDataFrame(data, columns)

# 打印DataFrame，用于验证数据是否正确加载
df.show()

# Presto JDBC配置
presto_url = "jdbc:presto://your-presto-host:8080/your_catalog/your_schema"
properties = {
    "user": "your_presto_user",
    "password": "your_presto_password",
    "driver": "io.prestosql.jdbc.PrestoDriver"
}

# 使用JDBC连接将DataFrame写入Presto表
try:
    df.write \
        .jdbc(url=presto_url, table="your_presto_table", mode="overwrite", properties=properties)
    print("Data written to Presto table successfully")
except Exception as e:
    print(f"An error occurred: {e}")
