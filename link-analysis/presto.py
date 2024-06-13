from pyspark import SparkContext
from pyspark.sql import SparkSession

# 初始化 SparkContext 和 SparkSession
sc = SparkContext(appName="PathTransformation")
spark = SparkSession.builder \
    .appName(sc.appName) \
    .config("spark.sql.catalogImplementation", "hive") \
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

# 删除现有表（如果存在）
spark.sql("DROP TABLE IF EXISTS default.transformed_paths")

# 将 DataFrame 写入 Hive 表
df.select("uvi", "transformed_path").write.mode("overwrite").saveAsTable("default.transformed_paths")

# 使用 Spark SQL 查询验证表
spark.sql("SELECT * FROM default.transformed_paths").show()
