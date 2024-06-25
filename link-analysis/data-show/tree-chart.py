from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, col, lit, round as round_, expr
from pyspark.sql.types import StringType, StructType, StructField, FloatType

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

# 从 Hive 表读取数据，并仅选取前3条记录进行测试
df_aggregated_paths = spark.table("default.aggregated_paths")

# 过滤以 "/" 开头的路径
df_aggregated_paths = df_aggregated_paths.filter(col("PathList")[0].startswith("/"))

# 打印读取的数据帧
print("Filtered Aggregated Paths DataFrame:")
df_aggregated_paths.show(truncate=False)

# 定义处理链路数据的函数，用于生成树图数据
def process_paths_for_tree(path, weight):
    nodes = []
    # 初始source节点
    current_path = "root"
    for node in path:
        new_path = f"{current_path}/{node}"
        nodes.append((new_path, current_path, node, float(weight)))  # 确保 weight 为 float
        current_path = new_path
    return nodes

# 使用flatMap进行路径处理以生成树图数据
tree_nodes_rdd = df_aggregated_paths.rdd.flatMap(lambda row: process_paths_for_tree(row['PathList'], row['weight']))

# 定义树图数据的Schema
tree_schema = StructType([
    StructField("node", StringType(), True),
    StructField("parent", StringType(), True),
    StructField("name", StringType(), True),
    StructField("weight", FloatType(), True)  # 将 weight 改为 FloatType
])

# 创建树图数据的DataFrame
tree_df = spark.createDataFrame(tree_nodes_rdd, tree_schema)

# 计算每个节点的总权重
aggregated_df = tree_df.groupBy("node", "parent", "name").agg(
    sum_("weight").alias("weight")
)

# 计算总权重值
total_weight = aggregated_df.agg(sum_("weight")).collect()[0][0]

# 将每个节点的权重转换为百分比形式
aggregated_df = aggregated_df.withColumn("weight", round_(col("weight") / lit(total_weight) * 100, 2))

# 添加根节点
root_node = spark.createDataFrame([("root", "", "root", 100.0)], tree_schema)

# 合并根节点和计算结果
final_df = root_node.union(aggregated_df)

# 打印树图数据
print("Tree Chart DataFrame:")
final_df.show(truncate=False)

# 保存树图数据到 Hive 表
final_df.write.mode("overwrite").format("parquet").saveAsTable("default.tree_chart")

# 验证写入的表
spark.sql("SELECT * FROM default.tree_chart").show()
