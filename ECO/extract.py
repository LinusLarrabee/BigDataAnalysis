from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import json
import os

# 定义解析 JSON 数据的函数
def extract_qoe(json_str):
    try:
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

        return {
            "qoeType": qoe_type,
            "controllerId": control_id,
            "collectionTime": str(collection_time),
            "deviceDataList": json.dumps(device_data_list)
        }
    except Exception as e:
        raise ValueError(f"Error parsing JSON: {e}")

# 定义解析 AP_DATA 数据的函数
def parse_ap_data(device_data_str, collection_time, control_id):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device in device_data_list:
            band_count = len(device["collectionData"])
            for device_data in device["collectionData"]:
                band = device_data.get("band", None)
                factor = device.get("factor", {})
                wifi_coverage_score = (
                    factor.get("wifiCoverage2GScore") if "2.4" in band else
                    factor.get("wifiCoverage5GScore") if "5" in band else
                    factor.get("wifiCoverage6GScore") if "6" in band else None
                ) if band else None
                is_controller = factor.get("isController") == "1" if factor else False
                result.append({
                    "mainDeviceId": control_id,
                    "id": device.get("id"),
                    "band": band,
                    "collectionTime": collection_time,
                    "wifiCoverageScore": wifi_coverage_score,
                    "isController": is_controller,
                    "bandType": band_count,
                    "utilization": device_data.get("utilization"),
                    "averageRxRate": device_data.get("averageRxRate"),
                    "averageTxRate": device_data.get("averageTxRate"),
                    "bandWidth": device_data.get("bandWidth"),
                    "errorsPkt": device_data.get("errorsPkt"),
                    "ipAddress": device_data.get("ipAddress"),
                    "signalStrength": device_data.get("signalStrength"),
                    "packetsSent": device_data.get("packetsSent"),
                    "packetsReceived": device_data.get("packetsReceived"),
                    "errorsSent": device_data.get("errorsSent"),
                    "errorsReceived": device_data.get("errorsReceived"),
                    "bytesSent": device_data.get("bytesSent"),
                    "bytesReceived": device_data.get("bytesReceived"),
                    "noise": device_data.get("noise"),
                    "associatedDeviceNumberOfEntries": device_data.get("associatedDeviceNumberOfEntries"),
                    "congestionRate": device_data.get("congestionRate"),
                    "backhaulSta_macAddress": device_data["backhaulSta"]["macAddress"] if "backhaulSta" in device_data else None,
                    "backhaulSta_backhaulLinkType": device_data["backhaulSta"]["backhaulLinkType"] if "backhaulSta" in device_data else None,
                    "backhaulSta_linkRate": device_data["backhaulSta"]["linkRate"] if "backhaulSta" in device_data else None,
                    "backhaulSta_signalStrength": device_data["backhaulSta"]["signalStrength"] if "backhaulSta" in device_data else None,
                    "backhaulSta_utilization": device_data["backhaulSta"]["utilization"] if "backhaulSta" in device_data else None,
                    "backhaulSta_snr": device_data["backhaulSta"]["snr"] if "backhaulSta" in device_data else None
                })
        return result
    except Exception as e:
        print(f"Error parsing AP_DATA: {e}, data: {device_data_str}")
        raise

# 定义解析 CLIENT_DATA 数据的函数
def parse_client_data(device_data_str, collection_time, control_id):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device in device_data_list:
            for client_data in device["collectionData"]:
                result.append({
                    "mainDeviceId": control_id,
                    "id": device.get("id"),
                    "band": client_data.get("band"),
                    "collectionTime": collection_time,
                    "macAddress": client_data.get("macAddress"),
                    "ipAddress": client_data.get("ipAddress"),
                    "hostName": client_data.get("hostName"),
                    "signalStrength": client_data.get("signalStrength"),
                    "bytesSent": client_data.get("bytesSent"),
                    "bytesReceived": client_data.get("bytesReceived"),
                    "txRate": client_data.get("txRate"),
                    "rxRate": client_data.get("rxRate"),
                    "lastDataDownlinkRate": client_data.get("lastDataDownlinkRate"),
                    "lastDataUplinkRate": client_data.get("lastDataUplinkRate"),
                    "estMACDataRateDownlink": client_data.get("estMACDataRateDownlink"),
                    "estMACDataRateUplink": client_data.get("estMACDataRateUplink"),
                    "packetsSent": client_data.get("packetsSent"),
                    "packetsReceived": client_data.get("packetsReceived"),
                    "operatingStandard": client_data.get("operatingStandard"),
                    "lastConnectTime": client_data.get("lastConnectTime"),
                    "noise": client_data.get("noise"),
                    "errorsSent": client_data.get("errorsSent"),
                    "errorsReceived": client_data.get("errorsReceived"),
                    "retransCount": client_data.get("retransCount"),
                    "numOfAlerts": client_data["factor"]["numOfAlerts"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "availableWifiServiceQualityScore": client_data["factor"]["availableWifiServiceQualityScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "networkReadyTimeScore": client_data["factor"]["networkReadyTimeScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "signalStrengthScore": client_data["factor"]["signalStrengthScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "wifiConnectivityScore": client_data["factor"]["wifiConnectivityScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "wifiProtocolScore": client_data["factor"]["wifiProtocolScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "clientHealthScore": client_data["factor"]["clientHealthScore"] if "factor" in client_data and client_data["factor"] is not None else None,
                    "wanBandwidth": client_data["metrics"]["wanBandwidth"] if "metrics" in client_data and client_data["metrics"] is not None else None,
                    "wanThroughput": client_data["metrics"]["wanThroughput"] if "metrics" in client_data and client_data["metrics"] is not None else None,
                    "networkReadyTime": client_data["metrics"]["networkReadyTime"] if "metrics" in client_data and client_data["metrics"] is not None else None,
                    "wifiConnectivity": client_data["metrics"]["wifiConnectivity"] if "metrics" in client_data and client_data["metrics"] is not None else None
                })
        return result
    except Exception as e:
        print(f"Error parsing CLIENT_DATA: {e}, data: {device_data_str}")
        raise

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadLocalJSONFiles") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .getOrCreate()

# 指定读取文件路径
input_path = '/Users/sunhao/Library/Mobile Documents/com~apple~CloudDocs/Downloads/Downloads'  # 输入路径

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
file_paths = []
for file_name in os.listdir(input_path):
    if file_name.startswith("messages-") and file_name.endswith(".txt"):
        file_paths.append(os.path.join(input_path, file_name))

if not file_paths:
    raise FileNotFoundError(f"No files found in the directory: {input_path}")

# 创建一个空的 DataFrame
df = spark.read.text(file_paths)

# 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
df_qoe_kind = df.withColumn("Qoe", extract_udf(col("value"))).select(col("Qoe.*"))

# 显示结果
df_qoe_kind.show(truncate=False)

# 解析 AP_DATA 数据
df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "AP_DATA")
parse_ap_data_udf = udf(lambda device_data_str, collection_time, control_id: parse_ap_data(device_data_str, collection_time, control_id), ArrayType(StructType([
    StructField("mainDeviceId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("wifiCoverageScore", StringType(), True),
    StructField("isController", StringType(), True),
    StructField("bandType", IntegerType(), True),
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
    StructField("congestionRate", StringType(), True),
    StructField("backhaulSta_macAddress", StringType(), True),
    StructField("backhaulSta_backhaulLinkType", StringType(), True),
    StructField("backhaulSta_linkRate", StringType(), True),
    StructField("backhaulSta_signalStrength", StringType(), True),
    StructField("backhaulSta_utilization", StringType(), True),
    StructField("backhaulSta_snr", StringType(), True)
])))

df_ap_split = df_ap_data.withColumn(
    "parsed_data",
    explode(parse_ap_data_udf(col("deviceDataList"), col("collectionTime"), col("controllerId")))
).select("parsed_data.*")

# 解析 CLIENT_DATA 数据
df_client_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "CLIENT_DATA")
parse_client_data_udf = udf(lambda device_data_str, collection_time, control_id: parse_client_data(device_data_str, collection_time, control_id), ArrayType(StructType([
    StructField("mainDeviceId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("macAddress", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("hostName", StringType(), True),
    StructField("signalStrength", StringType(), True),
    StructField("bytesSent", StringType(), True),
    StructField("bytesReceived", StringType(), True),
    StructField("txRate", StringType(), True),
    StructField("rxRate", StringType(), True),
    StructField("lastDataDownlinkRate", StringType(), True),
    StructField("lastDataUplinkRate", StringType(), True),
    StructField("estMACDataRateDownlink", StringType(), True),
    StructField("estMACDataRateUplink", StringType(), True),
    StructField("packetsSent", StringType(), True),
    StructField("packetsReceived", StringType(), True),
    StructField("operatingStandard", StringType(), True),
    StructField("lastConnectTime", StringType(), True),
    StructField("noise", StringType(), True),
    StructField("errorsSent", StringType(), True),
    StructField("errorsReceived", StringType(), True),
    StructField("retransCount", StringType(), True),
    StructField("numOfAlerts", StringType(), True),
    StructField("availableWifiServiceQualityScore", StringType(), True),
    StructField("networkReadyTimeScore", StringType(), True),
    StructField("signalStrengthScore", StringType(), True),
    StructField("wifiConnectivityScore", StringType(), True),
    StructField("wifiProtocolScore", StringType(), True),
    StructField("clientHealthScore", StringType(), True),
    StructField("wanBandwidth", StringType(), True),
    StructField("wanThroughput", StringType(), True),
    StructField("networkReadyTime", StringType(), True),
    StructField("wifiConnectivity", StringType(), True)
])))

df_client_split = df_client_data.withColumn(
    "parsed_data",
    explode(parse_client_data_udf(col("deviceDataList"), col("collectionTime"), col("controllerId")))
).select("parsed_data.*")

# 存储 AP_DATA 处理后的数据到 ap_data.csv
output_path_ap = os.path.join(input_path, 'ap_data.csv')  # 输出文件路径
df_ap_split.coalesce(1).write.csv(output_path_ap, mode='overwrite', header=True)

# 存储 CLIENT_DATA 处理后的数据到 client_data.csv
output_path_client = os.path.join(input_path, 'client_data.csv')  # 输出文件路径
df_client_split.coalesce(1).write.csv(output_path_client, mode='overwrite', header=True)

# 停止SparkSession
spark.stop()
