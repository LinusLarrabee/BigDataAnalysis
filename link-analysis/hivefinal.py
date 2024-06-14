from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("PathTransformation") \
    .enableHiveSupport() \
    .getOrCreate()

# 示例 DataFrame
data = [(1, "path1"), (2, "path2"), (3, "path3")]
columns = ["uvi", "transformed_path"]
df = spark.createDataFrame(data, columns)

# 打印DataFrame，用于验证数据是否正确加载
df.show()

# 定义Hive表名称
new_table_name = "default.transformed_paths_new1"

# 删除已有的Hive表
spark.sql(f"DROP TABLE IF EXISTS {new_table_name}")

# 将 DataFrame 写入 Hive 表
df.write.mode("overwrite").format("parquet").saveAsTable(new_table_name)

# 验证写入的表
spark.sql(f"SELECT * FROM {new_table_name}").show()

# 停止SparkSession
spark.stop()
