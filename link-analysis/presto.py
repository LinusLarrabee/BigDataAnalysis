from pyspark.sql import SparkSession

# 初始化 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("PathTransformation") \
    .config("spark.sql.warehouse.dir", "file:///mnt/var/lib/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# 示例 DataFrame
data = [(1, "path1"), (2, "path2"), (3, "path3")]
columns = ["uvi", "transformed_path"]
df = spark.createDataFrame(data, columns)

# 将 DataFrame 写入 Hive 表
df.write.mode("overwrite").saveAsTable("default.transformed_paths")

# 验证数据是否成功写入
spark.sql("SELECT * FROM default.transformed_paths").show()
