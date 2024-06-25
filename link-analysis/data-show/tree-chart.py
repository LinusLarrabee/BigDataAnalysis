from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, col, monotonically_increasing_id
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

# 过滤路径，保留第一个元素以 '/' 开头的路径
df_aggregated_paths = df_aggregated_paths.filter(col("PathList")[0].startswith("/"))

# 打印读取的数据帧
print("Filtered Aggregated Paths DataFrame:")
df_aggregated_paths.show(truncate=False)

# 定义处理链路数据的函数，用于生成树图数据
def process_paths_for_tree(path, weight):
    nodes = []
    # 添加起始source节点
    nodes.append((path[0], "#", weight))
    for i in range(1, len(path)):
        node_label = path[i]
        parent_label = path[i-1]
        nodes.append((node_label, parent_label, weight))
    return nodes

# 使用flatMap进行路径处理以生成树图数据
tree_nodes_rdd = df_aggregated_paths.rdd.flatMap(lambda row: process_paths_for_tree(row['PathList'], row['weight']))

# 定义树图数据的Schema
tree_schema = StructType([
    StructField("node", StringType(), True),
    StructField("parent", StringType(), True),
    StructField("weight", IntegerType(), True)
])

# 创建树图数据的DataFrame
tree_df = spark.createDataFrame(tree_nodes_rdd, tree_schema)

# 计算每个节点的总权重
aggregated_df = tree_df.groupBy("node", "parent").agg(
    sum_("weight").alias("weight")
)

# 为每个节点添加一个唯一的id，并生成name列
final_df = aggregated_df.withColumn("id", monotonically_increasing_id())
final_df = final_df.withColumn("name", col("node"))

# 打印树图数据
print("Tree Chart DataFrame:")
final_df.show(truncate=False)

# 保存树图数据到 Hive 表
final_df.write.mode("overwrite").format("parquet").saveAsTable("default.tree_chart")

# 验证写入的表
spark.sql("SELECT * FROM default.tree_chart").show()
