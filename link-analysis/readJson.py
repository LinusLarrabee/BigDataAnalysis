from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, MapType, ArrayType, StructType, StructField

import json

# 创建 SparkSession
spark = SparkSession.builder.appName("ExtractMessage").getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("INFO")

# 定义文件路径
file_path = "/Users/sunhao/message.txt"

# 定义处理函数
def extract_data_collector(json_str):
    try:
        # 替换转义字符
        replaced_str = json_str.replace('\\', '')

        # 查找 dataCollectorDTO 和 timeStamp 的索引
        index_start = replaced_str.find("{\"dataCollectorDTO")
        index_end = replaced_str.find("\",\"timeStamp")

        # 检查索引是否合法
        if index_start == -1 or index_end == -1:
            return "string format error! str= " + replaced_str

        # 提取子字符串
        substring = replaced_str[index_start:index_end]

        # 返回提取的 JSON 子字符串
        return substring
    except Exception as e:
        return str(e)

# 注册 UDF
extract_data_collector_udf = udf(extract_data_collector, StringType())

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 dataCollectorDTO 部分
df_with_data_collector = df.withColumn("dataCollectorDTO", extract_data_collector_udf(col("value")))

# 定义解析 uvi 的函数
def extract_uvi(data_collector_str):
    try:
        data_collector = json.loads(data_collector_str)
        uvi = data_collector['dataCollectorDTO']['uvi']
        return uvi
    except Exception as e:
        return str(e)

# 注册 UDF
extract_uvi_udf = udf(extract_uvi, StringType())

# 应用 UDF 提取 uvi
df_with_uvi = df_with_data_collector.withColumn("uvi", extract_uvi_udf(col("dataCollectorDTO")))

# 定义解析 el 的函数
def extract_el(data_collector_str):
    try:
        data_collector = json.loads(data_collector_str)
        el = data_collector['dataCollectorDTO']['el']
        return json.dumps(el)
    except Exception as e:
        return str(e)

# 注册 UDF
extract_el_udf = udf(extract_el, StringType())

# 应用 UDF 提取 el
df_with_el = df_with_uvi.withColumn("el", extract_el_udf(col("dataCollectorDTO")))

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

# 注册 UDF
extract_eid_ct_ep_udf = udf(extract_eid_ct_ep, StringType())

# 应用 UDF 提取 el 列表中的字段
df_with_eid_ct_ep = df_with_el.withColumn("eid_ct_ep", extract_eid_ct_ep_udf(col("el")))

# 显示结果
df_with_eid_ct_ep.select("uvi", "eid_ct_ep").show(truncate=False)
