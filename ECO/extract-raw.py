from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import json
import os

# 定义解析 JSON 数据的函数
def extract_qoe(json_str):
    try:
        # 去除转义
        replaced_str = json_str.replace('\\', '')

        # 查找kafka消息层
        type_start = replaced_str.find("{\"filterKey\":\"")
        type_end = replaced_str.find("\",\"message\":\"{")

        if type_start == -1 or type_end == -1:
            return "string format error! str= " + replaced_str


        qoe_type = replaced_str[type_start + 14:type_end]  # Ensure the substring is complete


        # 查找Qoe内容
        data_start = type_end
        data_end = replaced_str.find('","timeStamp')
        if type_start == -1 or type_end == -1:
            return "string format error! str= " + replaced_str
        qoe_data = replaced_str[data_start + 13:data_end]


        qoe_json = json.loads(qoe_data)
        collection_time = qoe_json['Report'][0]['CollectionTime']
        device_data_list = qoe_json['Report'][0]['Device']['WiFi']['DataElements']['Network']['Device']

        return {
            "qoeType": qoe_type,
            "collectionTime": str(collection_time),
            "deviceDataList": json.dumps(device_data_list),
            "rawData": replaced_str
        }
    except Exception as e:
        raise ValueError(f"Error parsing JSON: {e}")

# 定义解析 AP_DATA 数据的函数
def parse_ap_data(device_data_str, collection_time):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device_id, device in device_data_list.items():
            for radio_id, radio in device.get('Radio', {}).items():
                result.append({
                    "mainDeviceId": device_id,
                    "radioId": radio_id,
                    "band": radio.get("X_TP_Band"),
                    "collectionTime": collection_time,
                    "noise": radio.get("Noise"),
                    "utilization": radio.get("Utilization"),
                    "averageRxRate": radio.get("X_TP_AverageRxRate"),
                    "averageTxRate": radio.get("X_TP_AverageTxRate"),
                    "bandWidth": radio.get("X_TP_Bandwidth"),
                    "errorsPkt": radio.get("X_TP_ErrorsPkt"),
                    "ipAddress": radio.get("X_TP_IPAddress"),
                    "signalStrength": radio.get("X_TP_SignalStrength"),
                    "packetsSent": radio.get("X_TP_PacketsSent"),
                    "packetsReceived": radio.get("X_TP_PacketsReceived"),
                    "errorsSent": radio.get("ErrorsSent"),
                    "errorsReceived": radio.get("ErrorsReceived"),
                    "bytesSent": radio.get("X_TP_BytesSent"),
                    "bytesReceived": radio.get("BytesReceived"),
                    "congestionRate": radio.get("X_TP_Congestion_Rate"),
                    "associatedDeviceNumberOfEntries": radio.get("X_TP_AssociatedDeviceNumberOfEntries"),
                    "backhaulSta_macAddress": radio["BackhaulSta"]["MACAddress"] if "BackhaulSta" in radio else None,
                    "backhaulSta_backhaulLinkType": radio["BackhaulSta"]["X_TP_BackhaulLinkType"] if "BackhaulSta" in radio else None,
                    "backhaulSta_linkRate": radio["BackhaulSta"]["X_TP_LinkRate"] if "BackhaulSta" in radio else None,
                    "backhaulSta_signalStrength": radio["BackhaulSta"]["X_TP_SignalStrength"] if "BackhaulSta" in radio else None,
                    "backhaulSta_utilization": radio["BackhaulSta"]["X_TP_Utilization"] if "BackhaulSta" in radio else None,
                    "backhaulSta_snr": radio["BackhaulSta"]["X_TP_SNR"] if "BackhaulSta" in radio else None
                })
        return result
    except Exception as e:
        print(f"Error parsing AP_DATA: {e}, data: {device_data_str}")
        raise

# 定义解析 CLIENT_DATA 数据的函数
def parse_client_data(device_data_str, collection_time):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device_id, device in device_data_list.items():
            for radio_id, radio in device.get('Radio', {}).items():
                for bss_id, bss in radio.get('BSS', {}).items():
                    for sta_id, sta in bss.get('STA', {}).items():
                        factor = sta.get("X_TP_QoE", {}).get("Factor", {})
                        result.append({
                            "mainDeviceId": device_id,
                            "radioId": radio_id,
                            "bssId": bss_id,
                            "staId": sta_id,
                            "band": radio.get("X_TP_Band"),
                            "collectionTime": collection_time,
                            "lastDataDownlinkRate": sta.get("LastDataDownlinkRate"),
                            "lastDataUplinkRate": sta.get("LastDataUplinkRate"),
                            "macAddress": sta.get("MACAddress"),
                            "signalStrength": sta.get("SignalStrength"),
                            "hostName": sta.get("X_TP_HostName"),
                            "ipAddress": sta.get("X_TP_IPAddress"),
                            "networkReadyTime": sta.get("X_TP_QoE", {}).get("NetworkReadyTime"),
                            "wifiConnectivity": sta.get("X_TP_QoE", {}).get("WiFiConnectivity"),
                            "numberOfAlerts": factor.get("NumberOfAlerts"),
                            "availableWifiServiceQualityScore": factor.get("AvailableWiFiServiceQualityScore"),
                            "networkReadyTimeScore": factor.get("NetworkReadyTimeScore"),
                            "signalStrengthScore": factor.get("SignalStrengthScore"),
                            "wifiConnectivityScore": factor.get("WiFiConnectivityScore"),
                            "wifiProtocolScore": factor.get("WiFiProtocolScore"),
                            "clientHealthScore": factor.get("ClientHealthScore"),
                            "rxRate": sta.get("X_TP_RxRate"),
                            "txRate": sta.get("X_TP_TxRate"),
                            "retransCount": sta.get("RetransCount"),
                            "estMACDataRateDownlink": sta.get("EstMACDataRateDownlink"),
                            "estMACDataRateUplink": sta.get("EstMACDataRateUplink"),
                            "failNum": sta.get("X_TP_FailNum")
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
input_path = '/Users/sunhao/s3'  # 输入路径

# 定义 UDF 返回的 schema
schema = StructType([
    StructField("qoeType", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("deviceDataList", StringType(), True),
    StructField("rawData", StringType(), True)  # 存储原始数据
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

# 存储原始报告数据到 report.csv
output_path_report = os.path.join(input_path, 'report.csv')
df_qoe_kind.select("rawData").coalesce(1).write.csv(output_path_report, mode='overwrite', header=True)

# 解析 AP_DATA 数据
df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "AP_DATA")
parse_ap_data_udf = udf(lambda device_data_str, collection_time: parse_ap_data(device_data_str, collection_time), ArrayType(StructType([
    StructField("mainDeviceId", StringType(), True),
    StructField("radioId", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("noise", StringType(), True),
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
    StructField("congestionRate", StringType(), True),
    StructField("associatedDeviceNumberOfEntries", StringType(), True),
    StructField("backhaulSta_macAddress", StringType(), True),
    StructField("backhaulSta_backhaulLinkType", StringType(), True),
    StructField("backhaulSta_linkRate", StringType(), True),
    StructField("backhaulSta_signalStrength", StringType(), True),
    StructField("backhaulSta_utilization", StringType(), True),
    StructField("backhaulSta_snr", StringType(), True)
])))

df_ap_split = df_ap_data.withColumn(
    "parsed_data",
    explode(parse_ap_data_udf(col("deviceDataList"), col("collectionTime")))
).select("parsed_data.*")

# 解析 CLIENT_DATA 数据
df_client_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "CLIENT_DATA")
parse_client_data_udf = udf(lambda device_data_str, collection_time: parse_client_data(device_data_str, collection_time), ArrayType(StructType([
    StructField("mainDeviceId", StringType(), True),
    StructField("radioId", StringType(), True),
    StructField("bssId", StringType(), True),
    StructField("staId", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collectionTime", StringType(), True),
    StructField("lastDataDownlinkRate", StringType(), True),
    StructField("lastDataUplinkRate", StringType(), True),
    StructField("macAddress", StringType(), True),
    StructField("signalStrength", StringType(), True),
    StructField("hostName", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("networkReadyTime", StringType(), True),
    StructField("wifiConnectivity", StringType(), True),
    StructField("numberOfAlerts", StringType(), True),
    StructField("availableWifiServiceQualityScore", StringType(), True),
    StructField("networkReadyTimeScore", StringType(), True),
    StructField("signalStrengthScore", StringType(), True),
    StructField("wifiConnectivityScore", StringType(), True),
    StructField("wifiProtocolScore", StringType(), True),
    StructField("clientHealthScore", StringType(), True),
    StructField("rxRate", StringType(), True),
    StructField("txRate", StringType(), True),
    StructField("retransCount", StringType(), True),
    StructField("estMACDataRateDownlink", StringType(), True),
    StructField("estMACDataRateUplink", StringType(), True),
    StructField("failNum", StringType(), True)
])))

df_client_split = df_client_data.withColumn(
    "parsed_data",
    explode(parse_client_data_udf(col("deviceDataList"), col("collectionTime")))
).select("parsed_data.*")

# 存储 AP_DATA 处理后的数据到 ap_data.csv
output_path_ap = os.path.join(input_path, 'ap_data.csv')  # 输出文件路径
df_ap_split.coalesce(1).write.csv(output_path_ap, mode='overwrite', header=True)

# 存储 CLIENT_DATA 处理后的数据到 client_data.csv
output_path_client = os.path.join(input_path, 'client_data.csv')  # 输出文件路径
df_client_split.coalesce(1).write.csv(output_path_client, mode='overwrite', header=True)

# 停止SparkSession
spark.stop()
