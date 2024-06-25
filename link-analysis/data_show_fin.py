from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode, lit

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Path Processing") \
    .enableHiveSupport() \
    .getOrCreate()

# 示例数据
data = [
    ("0190482cd7fd801c", ["/auth/login?redirect=/isp/dashboard/basic", "/isp/dashboard/basic"]),
    ("01904976b32b2d7b", ["/auth/login", "/isp/dashboard/basic"]),
    ("07190483e55ff053", ["/auth/login", "/isp/assets-management/asset-list"])
]
columns = ["uvi", "PathList"]

# 创建 DataFrame
df_path_list = spark.createDataFrame(data, columns)

# 打印中间数据帧
print("PathList DataFrame:")
df_path_list.show(truncate=False)

# 拆分路径为单个节点并保留位置
df_exploded = df_path_list.withColumn("pos_and_node", posexplode(col("PathList")))

# 创建起点和终点的边
df_edges = df_exploded.withColumnRenamed("pos", "source_pos") \
    .withColumnRenamed("col", "source_node") \
    .withColumn("target_pos", col("source_pos") + lit(1)) \
    .join(df_exploded.withColumnRenamed("pos", "target_pos").withColumnRenamed("col", "target_node"),
          on=["uvi", "target_pos"],
          how="inner") \
    .select("source_node", "target_node", "uvi")

# 计算权重
df_edges = df_edges.groupBy("source_node", "target_node").count().withColumnRenamed("count", "weight")

# 写入 Hive 表
df_edges.write.mode("overwrite").saveAsTable("sankey_edges")

# 查看处理后的数据
print("Edges DataFrame:")
df_edges.show(truncate=False)
