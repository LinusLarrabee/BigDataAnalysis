from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, collect_list, sort_array, from_json, regexp_replace, count
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
from datetime import datetime, timedelta
import json

# 生成日期范围
def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current = start
    while current <= end:
        yield current.strftime("%Y/%m/%d")
        current += delta

# 定义处理函数
def extract_data_collector(json_str):
    try:
        replaced_str = json_str.replace('\\', '')
        index_start = replaced_str.find("{\"dataCollectorDTO")
        index_end = replaced_str.find("\",\"timeStamp")

        if index_start == -1 or index_end == -1:
            return "string format error! str= " + replaced_str

        substring = replaced_str[index_start:index_end]
        return substring
    except Exception as e:
        return str(e)

# 定义解析 uvi 的函数
def extract_uvi(data_collector_str):
    try:
        data_collector = json.loads(data_collector_str)
        uvi = data_collector['dataCollectorDTO']['uvi']
        return uvi
    except Exception as e:
        return str(e)

# 定义解析 el 的函数
def extract_el(data_collector_str):
    try:
        data_collector = json.loads(data_collector_str)
        el = data_collector['dataCollectorDTO']['el']
        return json.dumps(el)
    except Exception as e:
        return str(e)

# 定义解析 el 列表中字段的函数
def extract_eid_ct_ep(el_str):
    try:
        el_list = json.loads(el_str)
        results = []
        for item in el_list:
            if item.get('eid') == 'pageView':
                results.append({
                    "eid": item.get('eid'),
                    "ct": item.get('ct'),
                    "ep_L": item.get('ep', {}).get('L')
                })
        return json.dumps(results)
    except Exception as e:
        return str(e)

# 定义 schema
schema = ArrayType(StructType([
    StructField("eid", StringType(), True),
    StructField("ct", StringType(), True),
    StructField("ep_L", StringType(), True)
]))

# 创建SparkContext和SparkSession
sc = SparkContext(appName="PathTransformation")
spark = SparkSession.builder \
    .appName(sc.appName) \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.log4jHotPatch.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 注册 UDF
extract_data_collector_udf = udf(extract_data_collector, StringType())
extract_uvi_udf = udf(extract_uvi, StringType())
extract_el_udf = udf(extract_el, StringType())
extract_eid_ct_ep_udf = udf(extract_eid_ct_ep, StringType())

# 生成日期范围
start_date = "2024-06-24"  # 起始日期
end_date = "2024-06-24"    # 结束日期

bucket = 'beta-tauc-data-analysis'
all_json_list = []

for date_str in generate_date_range(start_date, end_date):
    input_path = f's3://{bucket}/local/uat/use1/{date_str}/messages-*.txt'
    file_rdd = sc.textFile(input_path)
    json_list = file_rdd.map(lambda x: json.loads(x)).collect()
    all_json_list.extend(json_list)

# 将 JSON 列表转换为 DataFrame
df = spark.createDataFrame(all_json_list, StringType()).toDF("value")

# 应用 UDF 提取 dataCollectorDTO 部分
df_with_data_collector = df.withColumn("dataCollectorDTO", extract_data_collector_udf(col("value")))

# 应用 UDF 提取 uvi 和 el
df_with_uvi = df_with_data_collector.withColumn("uvi", extract_uvi_udf(col("dataCollectorDTO")))
df_with_el = df_with_uvi.withColumn("el", extract_el_udf(col("dataCollectorDTO")))

# 应用 UDF 提取 el 列表中的字段
df_with_eid_ct_ep = df_with_el.withColumn("eid_ct_ep_str", extract_eid_ct_ep_udf(col("el")))
df_with_eid_ct_ep = df_with_eid_ct_ep.withColumn("eid_ct_ep", from_json(col("eid_ct_ep_str"), schema))

# 解析 eid_ct_ep 列表为多个行
df_exploded = df_with_eid_ct_ep.withColumn("eid_ct_ep", explode(col("eid_ct_ep")))

# 过滤掉没有 pageView 的记录
df_filtered = df_exploded.filter(col("eid_ct_ep.eid") == "pageView")

# 去掉字段 L 中 # 之前的部分
df_filtered = df_filtered.withColumn("eid_ct_ep", col("eid_ct_ep").withField("ep_L", regexp_replace(col("eid_ct_ep.ep_L"), ".*#", "")))

# 按 uvi 聚合路径
df_path_list = df_filtered.groupBy("uvi").agg(
    sort_array(collect_list("eid_ct_ep.ep_L")).alias("PathList")
)

# 打印中间数据帧
print("PathList DataFrame:")
df_path_list.show(truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, array, sort_array, collect_list, size, split

# 拆分路径为单个节点
df_exploded = df_path_list.withColumn("nodes", explode(col("PathList")))

# 将路径拆分为多个节点，并生成层级信息
df_exploded = df_exploded.withColumn("nodes_split", split(col("nodes"), "/"))
max_length = df_exploded.select(size(col("nodes_split")).alias("length")).agg({"length": "max"}).collect()[0][0]

for i in range(1, max_length + 1):
    df_exploded = df_exploded.withColumn(f"level_{i}", col("nodes_split").getItem(i - 1))

# 生成边的起点和终点，同时去除循环
edges = []
for i in range(1, max_length):
    df_edges = df_exploded.filter(col(f"level_{i}").isNotNull() & col(f"level_{i+1}").isNotNull())
    df_edges = df_edges.select(
        col(f"level_{i}").alias("source"),
        col(f"level_{i+1}").alias("target")
    ).distinct()  # 去除重复边
    edges.append(df_edges)

# 合并所有边
df_edges = edges[0]
for df_edge in edges[1:]:
    df_edges = df_edges.union(df_edge)

# 计算权重
df_edges = df_edges.groupBy("source", "target").count().withColumnRenamed("count", "weight")

# 写入Hive表
df_edges.write.mode("overwrite").saveAsTable("sankey_edges")

# 查看处理后的数据
print("Edges DataFrame:")
df_edges.show(truncate=False)