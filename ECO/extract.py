from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, explode
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType, IntegerType
import json

# 定义解析 JSON 数据的函数
def extract_device_data_list(device_data_list, control_id):
    controller_list = []
    agent_list = []
    for device in device_data_list:
        if device.get('id', '') == control_id:
            controller_list.append(device)
        else:
            agent_list.append(device)
    return json.dumps(controller_list), json.dumps(agent_list), len(agent_list)

# 定义处理函数
def extract_qoe(json_str):
    try:
        # 去除转义和修剪空格
        replaced_str = json_str.replace('\\', '').strip()

        # 查找kafka消息层
        type_start = replaced_str.find("{\"filterKey\":\"")
        type_end = replaced_str.find("\",\"message\":\"{")

        if type_start == -1 or type_end == -1:
            raise ValueError("string format error!")

        qoe_type = replaced_str[type_start + 14:type_end]  # 确保子字符串完整

        # 查找Qoe内容
        data_start = replaced_str.find('{"collectionRecords')
        data_end = replaced_str.find('"timeStamp')

        if data_start == -1 or data_end == -1:
            raise ValueError("string format error!")

        qoe_data = replaced_str[data_start + 22:data_end-4]
        qoe_json = json.loads(qoe_data)
        control_id = qoe_json['controllerId']
        collection_time = qoe_json['collectionTime']
        device_data_list = qoe_json['deviceDataList']

        # 使用函数解析 deviceDataList
        controller_data, agent_data_list, agent_data_list_size = extract_device_data_list(device_data_list, control_id)

        return {
            "qoeType": qoe_type,
            "controllerId": control_id,
            "collectionTime": str(collection_time),
            "controllerData": controller_data,
            "agentDataList": agent_data_list,
            "agentDataListSize": agent_data_list_size
        }
    except Exception as e:
        raise ValueError(f"Error parsing JSON: {e}")

# 定义解析controllerData的函数
def parse_controller_data(controller_data_str, collection_time):
    controller_data = json.loads(controller_data_str)[0]  # 转换为JSON对象并取第一个元素
    result = []
    device_data_list = controller_data["collectionData"]
    factor = controller_data["factor"]
    for device_data in device_data_list:
        band = device_data["band"]
        wifi_coverage_score = (
            factor.get("wifiCoverage2GScore") if "2.4" in band else
            factor.get("wifiCoverage5GScore") if "5" in band else
            factor.get("wifiCoverage6GScore") if "6" in band else None
        )
        result.append({
            "id": controller_data["id"],
            "band": band,
            "collectionTime": collection_time,
            "wifiCoverageScore": wifi_coverage_score,
            "utilization": device_data["utilization"],
            "averageRxRate": device_data["averageRxRate"],
            "averageTxRate": device_data["averageTxRate"],
            "bandWidth": device_data["bandWidth"],
            "errorsPkt": device_data["errorsPkt"],
            "ipAddress": device_data["ipAddress"],
            "signalStrength": device_data["signalStrength"],
            "packetsSent": device_data["packetsSent"],
            "packetsReceived": device_data["packetsReceived"],
            "errorsSent": device_data["errorsSent"],
            "errorsReceived": device_data["errorsReceived"],
            "bytesSent": device_data["bytesSent"],
            "bytesReceived": device_data["bytesReceived"],
            "noise": device_data["noise"],
            "associatedDeviceNumberOfEntries": device_data["associatedDeviceNumberOfEntries"],
            "congestionRate": device_data["congestionRate"]
        })
    return result

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
    StructField("controllerData", StringType(), True),  # 存储为字符串
    StructField("agentDataList", StringType(), True),  # 存储为字符串
    StructField("agentDataListSize", IntegerType(), True)  # 存储列表的大小
])

# 注册 UDF
extract_udf = udf(extract_qoe, schema)

# 读取文件
df = spark.read.text(file_path)

# 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
df_qoe_kind = df.withColumn("Qoe", extract_udf(col("value"))).select(col("Qoe.*"))

# 显示结果
df_qoe_kind.show(truncate=False)

# 按 qoeType 判断是否进一步解析
df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "AP_DATA")

# 定义controllerData的解析模式
controller_data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("factor", MapType(StringType(), StringType()), True),
    StructField("metrics", MapType(StringType(), StringType()), True),
    StructField("collectionData", ArrayType(MapType(StringType(), StringType())), True)
])

# 注册解析controllerData的UDF
parse_controller_data_udf = udf(lambda controller_data_str, collection_time: parse_controller_data(controller_data_str, collection_time), ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("wifiCoverageScore", StringType(), True),
    StructField("utilization", StringType(), True),
    StructField("averageRxRate", StringType(), True),
    StructField("averageTxRate", StringType(), True),
    StructField("bandWidth", StringType(), True),
    StructField("errorsPkt", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("signalStrength", StringType(), True),
    StructField("packetsSent", StringType(), True),
    StructField("packetsReceived", StringType(), True),
    StructField("errorsSent", StringType(), True),
    StructField("errorsReceived", StringType(), True),
    StructField("bytesSent", StringType(), True),
    StructField("bytesReceived", StringType(), True),
    StructField("noise", StringType(), True),
    StructField("associatedDeviceNumberOfEntries", StringType(), True),
    StructField("congestionRate", StringType(), True)
])))

# 应用UDF并展平结果
df_split = df_ap_data.withColumn(
    "parsed_data",
    explode(parse_controller_data_udf(col("controllerData"), col("collectionTime")))
).select("parsed_data.*")

# 显示结果
df_split.show(truncate=False)

# 停止SparkSession
spark.stop()
