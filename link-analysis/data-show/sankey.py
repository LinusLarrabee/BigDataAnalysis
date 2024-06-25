from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_list, explode, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# 创建SparkSession
spark = SparkSession.builder \
    .appName("PathTransformation") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.log4jHotPatch.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 从 Hive 表读取数据
df_aggregated_paths = spark.table("default.aggregated_paths")

# 打印读取的数据帧
print("Aggregated Paths DataFrame:")
df_aggregated_paths.show(truncate=False)

processed_table_name = "default.sankey_edges"

# 处理链路数据
def process_paths(path, count):
    paths = []
    for i in range(len(path) - 1):
        source_label = f"{path[i]} ({i+1})"
        target_label = f"{path[i+1]} ({i+2})"
        paths.append((source_label, target_label, count, str(path)))
    return paths

# 使用flatMap进行路径处理
processed_paths_rdd = df_aggregated_paths.rdd.flatMap(lambda row: process_paths(row['PathList'], row['weight']))

# 定义处理后的Schema
processed_schema = StructType([
    StructField("source", StringType(), True),
    StructField("target", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("full_path", StringType(), True)
])

# 创建处理后的DataFrame
processed_df = spark.createDataFrame(processed_paths_rdd, processed_schema)

spark.sql(f"DROP TABLE IF EXISTS {processed_table_name}")

# 将处理后的 DataFrame 写入 Hive 表
processed_df.write.mode("overwrite").format("parquet").saveAsTable(processed_table_name)

# 验证写入的表
spark.sql(f"SELECT * FROM {processed_table_name}").show()
