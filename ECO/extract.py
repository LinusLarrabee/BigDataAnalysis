from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType

import json

# 定义处理函数
def extract_qoe(json_str):
    try:
        # 去除转义和修剪空格
        replaced_str = json_str.replace('\\', '').strip()

        # 查找kafka消息层
        type_start = replaced_str.find("{\"filterKey\":\"")
        type_end = replaced_str.find("\",\"message\":\"{")

        if type_start == -1 or type_end == -1:
            return {
                "QoeType": "string format error!",
                "collectionTime": "",
                "controllerId": "",
                "deviceDataList": []
            }

        qoe_type = replaced_str[type_start + 14:type_end]  # 确保子字符串完整

        # 查找Qoe内容
        data_start = replaced_str.find('{"collectionRecords')
        data_end = replaced_str.find('"timeStamp')

        if data_start == -1 or data_end == -1:
            return {
                "qoeType": qoe_type,
                "collectionTime": "",
                "controllerId": "",
                "deviceDataList": []
            }

        qoe_data = replaced_str[data_start + 22:data_end-4]
        qoe_json = json.loads(qoe_data)
        control_id = qoe_json['controllerId']
        collection_time = qoe_json['collectionTime']
        device_data_list = qoe_json['deviceDataList']



        # # 解析QoeData
        # qoe_data_json = json.loads(qoe_data)
        # collection_record = qoe_data_json['collectionRecords'][0]  # 假设collectionRecords是一个列表，取第一个元素
        #
        # collection_time = collection_record.get('collectionTime', '')
        # device_data_list = collection_record.get('deviceDataList', [])

        return {
            "qoeType": qoe_type,
            "controllerId": control_id,
            "collectionTime": str(collection_time),
            "deviceDataList": json.dumps(device_data_list)  # 转换为字符串
        }
    except Exception as e:
        return {
            "qoeType": "exception",
            "collectionTime": "",
            "controllerId": "",
            "deviceDataList": []
        }

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadSingleFileJSON") \
    .getOrCreate()

# 指定读取文件路径
file_path = "messages-1721813944482.txt"

# 定义 UDF 返回的 schema
schema = StructType([
    StructField("qoeType", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("controllerId", StringType(), True),
    StructField("deviceDataList", StringType(), True)  # 存储为字符串
])

# 注册 UDF
extract_udf = udf(extract_qoe, schema)

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
df_qoe_kind = df.withColumn("Qoe", extract_udf(col("value"))).select(col("Qoe.*"))

# 解析 deviceDataList 列为数组
# device_data_schema = ArrayType(MapType(StringType(), StringType()))
# df_qoe_kind = df_qoe_kind.withColumn("deviceDataList", from_json(col("deviceDataList"), device_data_schema))

# 显示结果
df_qoe_kind.show(truncate=False)

# 按 controllerId 列进行分区保存
# output_base_path = "/euw1/your-isp-name/apData/2024/06/24"
# df_qoe_kind.write.partitionBy("controllerId").mode("overwrite").parquet(output_base_path)

# 停止SparkSession
spark.stop()
