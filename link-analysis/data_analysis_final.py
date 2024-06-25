from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, collect_list, sort_array, from_json, regexp_replace, count
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
from datetime import datetime, timedelta
import sys
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

# 获取输入参数
start_date = sys.argv[1]  # 起始日期
end_date = sys.argv[2]    # 结束日期

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

# 初始化一个空的 DataFrame
df_empty_schema = StructType([
    StructField("value", StringType(), True)
])
df = spark.createDataFrame([], df_empty_schema)

# 生成日期范围并读取数据
bucket = 'beta-tauc-data-analysis'

for date_str in generate_date_range(start_date, end_date):
    input_path = f's3://{bucket}/local/uat/use1/{date_str}/messages-*.txt'
    try:
        file_rdd = sc.textFile(input_path)
        json_list = file_rdd.map(lambda x: json.loads(x)).collect()
        df_day = spark.createDataFrame(json_list, StringType()).toDF("value")
        df = df.union(df_day)
    except Exception as e:
        print(f"Path not found: {input_path}, skipping.")

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
    collect_list("eid_ct_ep.ep_L").alias("PathList")
)

# 打印中间数据帧
print("PathList DataFrame:")
df_path_list.show(truncate=False)

# 聚合相同的 PathList，计算权重
df_aggregated_paths = df_path_list.groupBy("PathList").agg(
    count("PathList").cast(IntegerType()).alias("weight")
)

# 打印中间数据帧
print("Aggregated Paths DataFrame:")
df_aggregated_paths.show(truncate=False)

# 保存 df_aggregated_paths 到 Hive 表
df_aggregated_paths.write.mode("overwrite").format("parquet").saveAsTable("default.aggregated_paths")


