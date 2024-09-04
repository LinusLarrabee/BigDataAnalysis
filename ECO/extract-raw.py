from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
import os
import gzip

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

        qoe_type = replaced_str[type_start + 14:type_end]  # 确保子字符串完整

        # 查找Qoe内容
        data_start = type_end
        data_end = replaced_str.find('","timeStamp')
        if data_start == -1 or data_end == -1:
            return "string format error! str= " + replaced_str
        qoe_data = replaced_str[data_start + 13:data_end]

        qoe_json = json.loads(qoe_data)
        reports = qoe_json['Report']

        results = []
        for report in reports:
            collection_time = report['CollectionTime']
            controller_id = report['Device']['WiFi']['DataElements']['Network']['ControllerID']
            device_data_list = report['Device']['WiFi']['DataElements']['Network']['Device']
            multiap_data_list = report.get('Device', {}).get('WiFi', {}).get('MultiAP', {}).get('APDevice', {})
            results.append({
                "qoe_type": qoe_type,
                "collection_time": str(collection_time),
                "controller_id": controller_id,
                "device_data_list": json.dumps(device_data_list),
                "multiap_data_list": json.dumps(multiap_data_list)
            })

        return results
    except Exception as e:
        raise ValueError(f"Error parsing JSON: {e}")

# 定义解析 AP_DATA 数据的函数
def parse_ap_data(device_data_str, collection_time, controller_id):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device_id, device in device_data_list.items():
            device_id = device.get("ID", device_id)  # 确保使用正确的12位ID
            qoe = device.get("X_TP_QoE", {})
            factor = qoe.get("Factor", {})
            common_data = {
                "controller_id": controller_id,
                "device_id": device_id,
                "collection_time": collection_time,
                "jitter": qoe.get("Jitter"),
                "latency": qoe.get("Latency"),
                "wan_bandwidth": qoe.get("WANBandwidth"),
                "upload_bandwidth": qoe.get("UploadBandwidth"),
                "wan_connectivity": qoe.get("WANConnectivity"),
                "wan_throughput": qoe.get("WANThroughput"),
                "memory_free": qoe.get("MemoryFree"),
                "memory_total": qoe.get("MemoryTotal"),
                "cpu_usage": qoe.get("CPUUsage"),
                "number_of_alerts": factor.get("NumberOfAlerts"),
                "is_controller": factor.get("IsController"),
                "connectivity_score": factor.get("ConnectivityScore"),
                "available_bandwidth_score": factor.get("AvailableBandwidthScore"),
                "internet_delay_score": factor.get("InternetDelayScore"),
                "internet_jitter_score": factor.get("InternetJitterScore"),
                "system_health_score": factor.get("SystemHealthScore"),
                "congestion_score": factor.get("CongestionScore")
            }

            for radio_id, radio in device.get('Radio', {}).items():
                band = radio.get("X_TP_Band")
                radio_data = common_data.copy()
                radio_data.update({
                    "band": band,
                    "noise": radio.get("Noise"),
                    "utilization": radio.get("Utilization"),
                    "transmit": radio.get("Transmit"),
                    "receive_self": radio.get("ReceiveSelf"),
                    "receive_other": radio.get("ReceiveOther"),
                    "congestion_rate": radio.get("X_TP_Congestion_Rate"),
                    "associated_device_number_of_entries": radio.get("X_TP_AssociatedDeviceNumberOfEntries"),
                    "average_rx_rate": radio.get("X_TP_AverageRxRate"),
                    "average_tx_rate": radio.get("X_TP_AverageTxRate"),
                    "bandwidth": radio.get("X_TP_Bandwidth"),
                    "bytes_sent": radio.get("X_TP_BytesSent"),
                    "errors_pkt": radio.get("X_TP_ErrorsPkt"),
                    "ip_address": radio.get("X_TP_IPAddress"),
                    "packets_received": radio.get("X_TP_PacketsReceived"),
                    "packets_sent": radio.get("X_TP_PacketsSent"),
                    "errors_sent": radio.get("ErrorsSent"),
                    "errors_received": radio.get("ErrorsReceived"),
                    "bytes_received": radio.get("BytesReceived"),
                    "backhaul_sta_mac_address": radio["BackhaulSta"]["MACAddress"] if "BackhaulSta" in radio else None,
                    "backhaul_sta_backhaul_link_type": radio["BackhaulSta"]["X_TP_BackhaulLinkType"] if "BackhaulSta" in radio else None,
                    "backhaul_sta_link_rate": radio["BackhaulSta"]["X_TP_LinkRate"] if "BackhaulSta" in radio else None,
                    "backhaul_sta_signal_strength": radio["BackhaulSta"]["X_TP_SignalStrength"] if "BackhaulSta" in radio else None,
                    "backhaul_sta_utilization": radio["BackhaulSta"]["X_TP_Utilization"] if "BackhaulSta" in radio else None
                })
                if band == "2.4GHz":
                    radio_data.update({
                        "wifi_coverage_score": factor.get("WiFiCoverage2GScore"),
                        "wifi_availability_score": factor.get("WiFiAvailability2GScore")
                    })
                elif band == "5GHz":
                    radio_data.update({
                        "wifi_coverage_score": factor.get("WiFiCoverage5GScore"),
                        "wifi_availability_score": factor.get("WiFiAvailability5GScore")
                    })
                elif band == "6GHz":
                    radio_data.update({
                        "wifi_coverage_score": factor.get("WiFiCoverage6GScore"),
                        "wifi_availability_score": factor.get("WiFiAvailability6GScore")
                    })
                result.append(radio_data)
        return result
    except Exception as e:
        print(f"Error parsing AP_DATA: {e}, data: {device_data_str}")
        raise

# 定义解析 CLIENT_DATA 数据的函数
def parse_client_data(device_data_str, collection_time, controller_id):
    try:
        device_data_list = json.loads(device_data_str)
        result = []
        for device_id, device in device_data_list.items():
            device_id = device.get("ID", device_id)  # 确保使用正确的12位ID
            for radio_id, radio in device.get('Radio', {}).items():
                for bss_id, bss in radio.get('BSS', {}).items():
                    for sta_id, sta in bss.get('STA', {}).items():
                        factor = sta.get("X_TP_QoE", {}).get("Factor", {})
                        result.append({
                            "controller_id": controller_id,
                            "device_id": device_id,
                            "radio_id": radio_id,
                            "bss_id": bss_id,
                            "sta_id": sta_id,
                            "band": radio.get("X_TP_Band"),
                            "collection_time": collection_time,
                            "last_data_downlink_rate": sta.get("LastDataDownlinkRate"),
                            "last_data_uplink_rate": sta.get("LastDataUplinkRate"),
                            "mac_address": sta.get("MACAddress"),
                            "signal_strength": sta.get("SignalStrength"),
                            "host_name": sta.get("X_TP_HostName"),
                            "ip_address": sta.get("X_TP_IPAddress"),
                            "network_ready_time": sta.get("X_TP_QoE", {}).get("NetworkReadyTime"),
                            "wifi_connectivity": sta.get("X_TP_QoE", {}).get("WiFiConnectivity"),
                            "number_of_alerts": factor.get("NumberOfAlerts"),
                            "available_wifi_service_quality_score": factor.get("AvailableWiFiServiceQualityScore"),
                            "network_ready_time_score": factor.get("NetworkReadyTimeScore"),
                            "signal_strength_score": factor.get("SignalStrengthScore"),
                            "wifi_connectivity_score": factor.get("WiFiConnectivityScore"),
                            "wifi_protocol_score": factor.get("WiFiProtocolScore"),
                            "client_health_score": factor.get("ClientHealthScore"),
                            "rx_rate": sta.get("X_TP_RxRate"),
                            "tx_rate": sta.get("X_TP_TxRate"),
                            "retrans_count": sta.get("RetransCount"),
                            "est_mac_data_rate_downlink": sta.get("EstMACDataRateDownlink"),
                            "est_mac_data_rate_uplink": sta.get("EstMACDataRateUplink"),
                            "fail_num": sta.get("X_TP_FailNum")
                        })
        return result
    except Exception as e:
        print(f"Error parsing CLIENT_DATA: {e}, data: {device_data_str}")
        raise

# 定义解析 MULTIAP 数据的函数
def parse_multiap_data(multiap_data_str, collection_time, controller_id):
    try:
        multiap_data_list = json.loads(multiap_data_str)
        result = []
        for device_id, device in multiap_data_list.items():
            for assoc_device_id, assoc_device in device.get("X_TP_Ethernet", {}).get("AssociatedDevice", {}).items():
                result.append({
                    "controller_id": controller_id,
                    "collection_time": collection_time,
                    "ap_device_id": assoc_device.get("APDeviceID"),
                    "mac_address": assoc_device.get("MACAddress"),
                    "ip_address": assoc_device.get("IPAddress"),
                    "host_name": assoc_device.get("X_TP_HostName"),
                    "up_speed": assoc_device.get("UpSpeed"),
                    "down_speed": assoc_device.get("DownSpeed"),
                    "link_speed": assoc_device.get("LinkSpeed"),
                    "duplex_mode": assoc_device.get("DuplexMode"),
                    "active": assoc_device.get("Active"),
                    "packets_sent": assoc_device.get("PacketsSent"),
                    "packets_received": assoc_device.get("PacketReceived"),
                    "errors_sent": assoc_device.get("ErrorsSent"),
                    "errors_received": assoc_device.get("ErrorsReceived"),
                    "interface_type": assoc_device.get("InterfaceType")
                })
        return result
    except Exception as e:
        print(f"Error parsing MULTIAP_DATA: {e}, data: {multiap_data_str}")
        raise

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadLocalJSONFiles") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .getOrCreate()

# 指定读取文件路径
input_path = '/Users/sunhao/s3/qoe_rawLocal/uat/aps1/2024/07/26'  # 输入路径

# 定义 UDF 返回的 schema
schema = ArrayType(StructType([
    StructField("qoe_type", StringType(), True),
    StructField("collection_time", StringType(), True),
    StructField("controller_id", StringType(), True),
    StructField("device_data_list", StringType(), True),
    StructField("multiap_data_list", StringType(), True)
]))

# 注册 UDF
extract_udf = udf(extract_qoe, schema)

file_paths = []
for file_name in os.listdir(input_path):
    if file_name.startswith("messages-") and file_name.endswith(".txt.gz"):
        file_paths.append(os.path.join(input_path, file_name))

if not file_paths:
    raise FileNotFoundError(f"No files found in the directory: {input_path}")

# 读取并解压缩文件内容，只保留偶数行，并打印前十个偶数行
even_lines = []
for file_path in file_paths:
    with gzip.open(file_path, 'rt') as f:  # 'rt' 模式表示以文本形式读取
        for i, line in enumerate(f, 1):  # enumerate 从 1 开始计数
            if i % 2 == 0:  # 偶数行
                even_lines.append(line.strip())
                if len(even_lines) == 10:  # 只取前十个偶数行
                    break
    if len(even_lines) == 10:
        break

# 打印前十个偶数行
for line in even_lines:
    print(line)


# 将偶数行转换为 Row 对象列表
rows = [Row(value=line) for line in even_lines]

# 创建 DataFrame
df = spark.createDataFrame(rows)

# 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
df_qoe_kind = df.withColumn("Qoe", explode(extract_udf(col("value")))).select(col("Qoe.*"))

# 显示结果
df_qoe_kind.show(truncate=False)

# 解析 AP_DATA 数据
df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoe_type == "AP_DATA")
df_ap_data.show(truncate=False)
parse_ap_data_udf = udf(lambda device_data_str, collection_time, controller_id: parse_ap_data(device_data_str, collection_time, controller_id), ArrayType(StructType([
    StructField("controller_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collection_time", StringType(), True),
    StructField("jitter", StringType(), True),
    StructField("latency", StringType(), True),
    StructField("wan_bandwidth", StringType(), True),
    StructField("upload_bandwidth", StringType(), True),
    StructField("wan_connectivity", StringType(), True),
    StructField("wan_throughput", StringType(), True),
    StructField("memory_free", StringType(), True),
    StructField("memory_total", StringType(), True),
    StructField("cpu_usage", StringType(), True),
    StructField("number_of_alerts", StringType(), True),
    StructField("is_controller", StringType(), True),
    StructField("connectivity_score", StringType(), True),
    StructField("available_bandwidth_score", StringType(), True),
    StructField("internet_delay_score", StringType(), True),
    StructField("internet_jitter_score", StringType(), True),
    StructField("system_health_score", StringType(), True),
    StructField("congestion_score", StringType(), True),
    StructField("wifi_coverage_score", StringType(), True),
    StructField("wifi_availability_score", StringType(), True),
    StructField("noise", StringType(), True),
    StructField("utilization", StringType(), True),
    StructField("transmit", StringType(), True),
    StructField("receive_self", StringType(), True),
    StructField("receive_other", StringType(), True),
    StructField("congestion_rate", StringType(), True),
    StructField("associated_device_number_of_entries", StringType(), True),
    StructField("average_rx_rate", StringType(), True),
    StructField("average_tx_rate", StringType(), True),
    StructField("bandwidth", StringType(), True),
    StructField("bytes_sent", StringType(), True),
    StructField("errors_pkt", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("packets_received", StringType(), True),
    StructField("packets_sent", StringType(), True),
    StructField("errors_sent", StringType(), True),
    StructField("errors_received", StringType(), True),
    StructField("bytes_received", StringType(), True),
    StructField("backhaul_sta_mac_address", StringType(), True),
    StructField("backhaul_sta_backhaul_link_type", StringType(), True),
    StructField("backhaul_sta_link_rate", StringType(), True),
    StructField("backhaul_sta_signal_strength", StringType(), True),
    StructField("backhaul_sta_utilization", StringType(), True)
])))

df_ap_split = df_ap_data.withColumn(
    "parsed_data",
    explode(parse_ap_data_udf(col("device_data_list"), col("collection_time"), col("controller_id")))
).select("parsed_data.*")

# 解析 CLIENT_DATA 数据
df_client_data = df_qoe_kind.filter(df_qoe_kind.qoe_type == "CLIENT_DATA")
df_client_data.show(truncate=False)
parse_client_data_udf = udf(lambda device_data_str, collection_time, controller_id: parse_client_data(device_data_str, collection_time, controller_id), ArrayType(StructType([
    StructField("controller_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("radio_id", StringType(), True),
    StructField("bss_id", StringType(), True),
    StructField("sta_id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("collection_time", StringType(), True),
    StructField("last_data_downlink_rate", StringType(), True),
    StructField("last_data_uplink_rate", StringType(), True),
    StructField("mac_address", StringType(), True),
    StructField("signal_strength", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("network_ready_time", StringType(), True),
    StructField("wifi_connectivity", StringType(), True),
    StructField("number_of_alerts", StringType(), True),
    StructField("available_wifi_service_quality_score", StringType(), True),
    StructField("network_ready_time_score", StringType(), True),
    StructField("signal_strength_score", StringType(), True),
    StructField("wifi_connectivity_score", StringType(), True),
    StructField("wifi_protocol_score", StringType(), True),
    StructField("client_health_score", StringType(), True),
    StructField("rx_rate", StringType(), True),
    StructField("tx_rate", StringType(), True),
    StructField("retrans_count", StringType(), True),
    StructField("est_mac_data_rate_downlink", StringType(), True),
    StructField("est_mac_data_rate_uplink", StringType(), True),
    StructField("fail_num", StringType(), True)
])))

df_client_split = df_client_data.withColumn(
    "parsed_data",
    explode(parse_client_data_udf(col("device_data_list"), col("collection_time"), col("controller_id")))
).select("parsed_data.*")

# 解析 MULTIAP 数据
df_multiap_data = df_qoe_kind.filter(df_qoe_kind.qoe_type == "CLIENT_DATA")
parse_multiap_data_udf = udf(lambda multiap_data_str, collection_time, controller_id: parse_multiap_data(multiap_data_str, collection_time, controller_id), ArrayType(StructType([
    StructField("controller_id", StringType(), True),
    StructField("collection_time", StringType(), True),
    StructField("ap_device_id", StringType(), True),
    StructField("mac_address", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("up_speed", StringType(), True),
    StructField("down_speed", StringType(), True),
    StructField("link_speed", StringType(), True),
    StructField("duplex_mode", StringType(), True),
    StructField("active", StringType(), True),
    StructField("packets_sent", StringType(), True),
    StructField("packets_received", StringType(), True),
    StructField("errors_sent", StringType(), True),
    StructField("errors_received", StringType(), True),
    StructField("interface_type", StringType(), True)
])))

df_multiap_split = df_multiap_data.withColumn(
    "parsed_data",
    explode(parse_multiap_data_udf(col("multiap_data_list"), col("collection_time"), col("controller_id")))
).select("parsed_data.*")


# 存储 AP_DATA 处理后的数据到 Parquet 格式
output_path_ap = os.path.join(input_path, 'ap_data.parquet')  # 输出文件路径
df_ap_split.coalesce(1).write.mode('overwrite').parquet(output_path_ap, compression='gzip')

# 存储 CLIENT_DATA 处理后的数据到 Parquet 格式
output_path_client = os.path.join(input_path, 'client_data.parquet')  # 输出文件路径
df_client_split.coalesce(1).write.mode('overwrite').parquet(output_path_client, compression='gzip')

# 存储 MULTIAP 数据到 Parquet 格式
output_path_multiap = os.path.join(input_path, 'multiap_data.parquet')  # 输出文件路径
df_multiap_split.coalesce(1).write.mode('overwrite').parquet(output_path_multiap, compression='gzip')

# 停止SparkSession
spark.stop()
