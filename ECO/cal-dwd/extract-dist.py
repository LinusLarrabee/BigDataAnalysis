from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, when
from pyspark.sql.types import StructType, StructField,IntegerType, StringType, ArrayType, DoubleType
import json
import sys
from pyspark import SparkContext
from pyspark.sql import functions as F

# 安全转换为整数的函数
def f_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

# 安全转换为浮点数的函数
def f_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# 定义解析 JSON 数据的函数
def extract_qoe(json_str):
    try:
        # 去除转义
        replaced_str = json_str.replace('\\', '')

        # 查找 Kafka 消息层
        type_start = replaced_str.find("{\"filterKey\":\"")
        type_end = replaced_str.find("\",\"message\":\"{")

        if type_start == -1 or type_end == -1:
            return f"string format error! str= {replaced_str}"

        qoe_type = replaced_str[type_start + 14:type_end]  # 确保子字符串完整

        # 查找 QoE 内容
        data_start = type_end
        data_end = replaced_str.find('","timeStamp')
        if data_start == -1 or data_end == -1:
            return f"string format error! str= {replaced_str}"

        qoe_data = replaced_str[data_start + 13:data_end]

        # 将 QoE 内容解析为 JSON
        qoe_json = json.loads(qoe_data)
        reports = qoe_json['Report']

        results = []
        for report in reports:
            try:
                # 解析每个 report 中的字段
                collection_time = report['CollectionTime']
                controller_id = report['Device']['WiFi']['DataElements']['Network']['ControllerID']
                device_data_list = report.get('Device',{}).get('WiFi', {}).get('DataElements',{}).get('Network',{}).get('Device',{})
                multiap_data_list = report.get('Device', {}).get('WiFi', {}).get('MultiAP', {}).get('APDevice', {})

                # 将解析后的数据添加到结果集中
                results.append({
                    "qoe_type": qoe_type,
                    "collection_time": str(collection_time),
                    "controller_id": controller_id,
                    "device_data_list": json.dumps(device_data_list),
                    "multiap_data_list": json.dumps(multiap_data_list)
                })

            except KeyError as e:
                print(f"Missing key error: {e}, in report: {report}")
                continue  # 跳过当前 report，但不会影响其他 report 的处理
            except Exception as e:
                print(f"Error parsing report: {e}, report: {report}")
                continue  # 跳过当前 report

        return results

    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}, data: {json_str}")
        return []  # 返回空结果，跳过该 JSON
    except Exception as e:
        print(f"Unknown error while parsing JSON: {e}, data: {json_str}")
        return []  # 返回空结果

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
                "jitter": f_int(qoe.get("Jitter")),
                "latency": f_int(qoe.get("Latency")),
                "wan_bandwidth": f_int(qoe.get("WANBandwidth")),
                "upload_bandwidth": f_int(qoe.get("UploadBandwidth")),
                "wan_connectivity": f_int(qoe.get("WANConnectivity")),
                "wan_throughput": f_int(qoe.get("WANThroughput")),
                "memory_free": f_int(qoe.get("MemoryFree")),
                "memory_total": f_int(qoe.get("MemoryTotal")),
                "cpu_usage": f_int(qoe.get("CPUUsage")),
                "number_of_alerts": f_int(factor.get("NumberOfAlerts")),
                "is_controller": f_int(factor.get("IsController")),
                "connectivity_score": f_float(factor.get("ConnectivityScore")),
                "available_bandwidth_score": f_float(factor.get("AvailableBandwidthScore")),
                "internet_delay_score": f_float(factor.get("InternetDelayScore")),
                "internet_jitter_score": f_float(factor.get("InternetJitterScore")),
                "system_health_score": f_float(factor.get("SystemHealthScore")),
                "congestion_score": f_float(factor.get("CongestionScore"))
            }

            for radio_id, radio in device.get('Radio', {}).items():
                band = radio.get("X_TP_Band")
                radio_data = common_data.copy()
                radio_data.update({
                    "band": band,
                    "noise": f_int(radio.get("Noise")),
                    "utilization": f_int(radio.get("Utilization")),
                    "transmit": f_int(radio.get("Transmit")),
                    "receive_self": f_int(radio.get("ReceiveSelf")),
                    "receive_other": f_int(radio.get("ReceiveOther")),
                    "congestion_rate": f_int(radio.get("X_TP_Congestion_Rate")),
                    "associated_device_number_of_entries": f_int(radio.get("X_TP_AssociatedDeviceNumberOfEntries")),
                    "average_rx_rate": f_int(radio.get("X_TP_AverageRxRate")),
                    "average_tx_rate": f_int(radio.get("X_TP_AverageTxRate")),
                    "bandwidth": radio.get("X_TP_Bandwidth"),
                    "bytes_sent": f_int(radio.get("X_TP_BytesSent")),
                    "errors_pkt": f_int(radio.get("X_TP_ErrorsPkt")),
                    "ip_address": radio.get("X_TP_IPAddress"),
                    "packets_received": f_int(radio.get("X_TP_PacketsReceived")),
                    "packets_sent": f_int(radio.get("X_TP_PacketsSent")),
                    "errors_sent": f_int(radio.get("ErrorsSent", 0)),
                    "errors_received": f_int(radio.get("ErrorsReceived", 0)),
                    "bytes_received": f_int(radio.get("BytesReceived", 0)),
                    "backhaul_sta_mac_address": radio.get("BackhaulSta", {}).get("MACAddress"),
                    "backhaul_sta_backhaul_link_type": radio.get("BackhaulSta", {}).get("X_TP_BackhaulLinkType"),
                    "backhaul_sta_link_rate": f_int(radio.get("BackhaulSta", {}).get("X_TP_LinkRate", 0)),
                    "backhaul_sta_signal_strength": f_int(radio.get("BackhaulSta", {}).get("X_TP_SignalStrength", 0)),
                    "backhaul_sta_utilization": f_int(radio.get("BackhaulSta", {}).get("X_TP_Utilization", 0))
                })
                # 根据 band 来更新 radio_data
                if band == "2.4GHz":
                    radio_data.update({
                        "wifi_coverage_score": f_float(factor.get("WiFiCoverage2GScore")),
                        "wifi_availability_score": f_float(factor.get("WiFiAvailability2GScore"))
                    })
                elif band == "5GHz":
                    radio_data.update({
                        "wifi_coverage_score": f_float(factor.get("WiFiCoverage5GScore")),
                        "wifi_availability_score": f_float(factor.get("WiFiAvailability5GScore"))
                    })
                elif band == "6GHz":
                    radio_data.update({
                        "wifi_coverage_score": f_float(factor.get("WiFiCoverage6GScore")),
                        "wifi_availability_score": f_float(factor.get("WiFiAvailability6GScore"))
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
                # 计算当前 radio 下的 STA 数量
                sta_count = sum([len(bss.get('STA', {})) for bss in radio.get('BSS', {}).values()])

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
                            "sta_count": sta_count,  # 添加当前 radio 的 STA 数量
                            "last_data_downlink_rate": f_int(sta.get("LastDataDownlinkRate")),
                            "last_data_uplink_rate": f_int(sta.get("LastDataUplinkRate")),
                            "mac_address": sta.get("MACAddress"),
                            "signal_strength": f_int(sta.get("SignalStrength")),
                            "host_name": sta.get("X_TP_HostName"),
                            "ip_address": sta.get("X_TP_IPAddress"),
                            "network_ready_time": f_int(sta.get("X_TP_QoE", {}).get("NetworkReadyTime")),
                            "wifi_connectivity": f_int(sta.get("X_TP_QoE", {}).get("WiFiConnectivity")),
                            "number_of_alerts": f_int(factor.get("NumberOfAlerts")),
                            "available_wifi_service_quality_score": f_float(factor.get("AvailableWiFiServiceQualityScore")),
                            "network_ready_time_score": f_float(factor.get("NetworkReadyTimeScore")),
                            "signal_strength_score": f_float(factor.get("SignalStrengthScore")),
                            "wifi_connectivity_score": f_float(factor.get("WiFiConnectivityScore")),
                            "wifi_protocol_score": f_float(factor.get("WiFiProtocolScore")),
                            "client_health_score": f_float(factor.get("ClientHealthScore")),
                            "rx_rate": f_int(sta.get("X_TP_RxRate")),
                            "tx_rate": f_int(sta.get("X_TP_TxRate")),
                            "retrans_count": f_int(sta.get("RetransCount")),
                            "est_mac_data_rate_downlink": f_int(sta.get("EstMACDataRateDownlink")),
                            "est_mac_data_rate_uplink": f_int(sta.get("EstMACDataRateUplink")),
                            "fail_num": f_int(sta.get("X_TP_FailNum"))
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
            # 计算 AssociatedDevice 的数量
            sta_count = len(device.get("X_TP_Ethernet", {}).get("AssociatedDevice", {}))
            for assoc_device_id, assoc_device in device.get("X_TP_Ethernet", {}).get("AssociatedDevice", {}).items():
                result.append({
                    "controller_id": controller_id,
                    "collection_time": collection_time,
                    "device_id": assoc_device.get("APDeviceID"),
                    "mac_address": assoc_device.get("MACAddress"),
                    "ip_address": assoc_device.get("IPAddress"),
                    "host_name": assoc_device.get("X_TP_HostName"),
                    "up_speed": f_int(assoc_device.get("UpSpeed")),
                    "down_speed": f_int(assoc_device.get("DownSpeed")),
                    "link_speed": f_int(assoc_device.get("LinkSpeed")),
                    "duplex_mode": assoc_device.get("DuplexMode"),
                    "active": f_int(assoc_device.get("Active")),
                    "packets_sent": f_int(assoc_device.get("PacketsSent")),
                    "packets_received": f_int(assoc_device.get("PacketReceived")),
                    "errors_sent": f_int(assoc_device.get("ErrorsSent")),
                    "errors_received": f_int(assoc_device.get("ErrorsReceived")),
                    "interface_type": assoc_device.get("InterfaceType"),
                    "sta_count": sta_count  # 新增字段：关联设备数量
                })
        return result
    except Exception as e:
        print(f"Error parsing MULTIAP_DATA: {e}, data: {multiap_data_str}")
        raise

def process_file(file_path):
    try:
        file_rdd = sc.textFile(file_path)
        json_list = file_rdd.zipWithIndex().filter(lambda x: (x[1] + 1) % 2 == 0).map(lambda x: json.loads(x[0])).collect()
        df_day = spark.createDataFrame(json_list, StringType()).toDF("value")

        # 在此处直接应用后续操作逻辑（例如解析或存储）
        df_qoe_kind = df_day.withColumn("Qoe", explode(extract_udf(col("value")))).select(col("Qoe.*"))

        df_ap_split = df_qoe_kind.filter(df_qoe_kind.qoe_type == "AP_DATA") \
            .withColumn("parsed_data", explode(parse_ap_data_udf(col("device_data_list"), col("collection_time"), col("controller_id")))) \
            .select("parsed_data.*")

        df_client_split = df_qoe_kind.filter(df_qoe_kind.qoe_type == "CLIENT_DATA") \
            .withColumn("parsed_data", explode(parse_client_data_udf(col("device_data_list"), col("collection_time"), col("controller_id")))) \
            .select("parsed_data.*")

        df_multiap_split = df_qoe_kind.filter(df_qoe_kind.qoe_type == "MULTIAP_DATA") \
            .withColumn("multiap_data_valid", when(col("multiap_data_list") != '{}', col("multiap_data_list")).otherwise(None)) \
            .filter(col("multiap_data_valid").isNotNull()) \
            .withColumn("parsed_data", explode(parse_multiap_data_udf(col("multiap_data_valid"), col("collection_time"), col("controller_id")))) \
            .select("parsed_data.*")

        # 存储数据
        save_by_date_partitioning(df_ap_split, "ap_data", bucket, output_prefix)
        save_by_date_partitioning(df_client_split, "wireless_data", bucket, output_prefix)
        save_by_date_partitioning(df_multiap_split, "wire_data", bucket, output_prefix)

    except Exception as file_error:
        print(f"Error processing file: {file_path}, skipping. Error: {file_error}")


for date_str in generate_date_range(start_date, end_date):
    input_path = f's3://{bucket}/{input_prefix}/{date_str}/messages-*.txt.gz'
    try:
        file_list = sc._jsc.hadoopConfiguration().globStatus(input_path)
        if not file_list:
            print(f"No files found for path: {input_path}, skipping.")
            continue

        for file_status in file_list:
            file_path = file_status.getPath().toString()
            print(f"Processing file: {file_path}")
            process_file(file_path)

    except Exception as e:
        print(f"Path not found or error processing: {input_path}, skipping. Error: {e}")


