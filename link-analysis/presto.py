from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

# 检查AWS凭证文件位置
aws_credentials_path = os.path.expanduser('~/.aws/credentials')
if not os.path.exists(aws_credentials_path):
    print(f"Error: AWS credentials file not found at {aws_credentials_path}")

# 初始化 SparkContext
sc = SparkContext(appName="PathTransformation")

# 使用已有的 SparkContext 初始化 SparkSession，并启用 Hive 支持和 Glue Data Catalog
spark = SparkSession.builder \
    .appName(sc.appName) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.warehouse.dir", "s3://linkanalysis/warehouse/") \
    .enableHiveSupport() \
    .getOrCreate()

# 打印Spark和Hive的配置信息，用于调试
print("Spark configuration:")
for item in spark.sparkContext.getConf().getAll():
    print(item)

# 示例 DataFrame
data = [(1, "path1"), (2, "path2"), (3, "path3")]
columns = ["uvi", "transformed_path"]
df = spark.createDataFrame(data, columns)

# 打印DataFrame，用于验证数据是否正确加载
df.show()

try:
    # 将 DataFrame 写入 Hive 表
    print("Writing DataFrame to Hive table...")
    df.select("uvi", "transformed_path").write.mode("overwrite").saveAsTable("default.transformed_paths")

    # 使用 Spark SQL 查询验证表
    print("Querying the Hive table...")
    spark.sql("SELECT * FROM default.transformed_paths").show()
except Exception as e:
    print(f"An error occurred: {e}")
