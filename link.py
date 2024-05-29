import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, collect_list, concat_ws

# 初始化Spark会话
spark = SparkSession.builder.appName("ProcessJSON").getOrCreate()

# 读取txt文件内容，每行一个JSON对象
file_path = 'input.txt'  # 请根据实际文件路径进行修改

json_list = []
with open(file_path, 'r') as f:
    for line in f:
        json_list.append(json.loads(line))

# 将多行JSON对象转换为DataFrame
rdd = spark.sparkContext.parallelize(json_list)
df = spark.read.json(rdd)

# 展开el字段
df_exploded = df.select(col("uvi"), explode(col("el")).alias("el_item"))

# 提取path和ct字段
df_path_ct = df_exploded.select(col("uvi"), col("el_item.path").alias("path"), col("el_item.ct").alias("ct"))

# 按照用户分组并收集所有路径到一个列表中
user_paths = df_path_ct.groupBy("uvi").agg(collect_list("path").alias("paths"))

# 将路径列表转换为字符串链路
user_paths = user_paths.withColumn("path_chain", concat_ws(" -> ", col("paths")))

# 统计每个链路的出现频率
path_chain_counts = user_paths.groupBy("path_chain").count().orderBy("count", ascending=False)

# 将DataFrame转换为Pandas DataFrame以便使用Matplotlib
path_chain_counts_pd = path_chain_counts.toPandas()

# 保存路径链路出现频率到csv文件
path_chain_counts_pd.to_csv('path_chain_counts1.csv', index=False)
