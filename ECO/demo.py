from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, FloatType

# 创建SparkSession
spark = SparkSession.builder \
    .appName("解析并拆分AP数据") \
    .getOrCreate()

# 定义输入数据的模式
schema = StructType([
    StructField("id", StringType(), True),
    StructField("factor", MapType(StringType(), StringType()), True),
    StructField("metrics", MapType(StringType(), StringType()), True),
    StructField("collectionData", ArrayType(MapType(StringType(), StringType())), True)
])

# 创建示例数据
data = [
    (
        "5C:E9:31:47:9C:48",
        {
            "numOfAlerts": "2", "isController": "1", "connectivityScore": "5.0", "availableBandwidthScore": "1.0",
            "wifiCoverage2GScore": "0.0", "wifiCoverage5GScore": "3.6", "wifiCoverage6GScore": None,
            "wifiAvailability2GScore": "1.0", "wifiAvailability5GScore": "4.5", "wifiAvailability6GScore": None,
            "internetDelayScore": "4.5", "internetJitterScore": "5.0", "systemHealthScore": "5.0", "congestionScore": None
        },
        {
            "wanBandwidth": "0", "uploadBandwidth": None, "wanConnectivity": "0", "wanThroughput": "0",
            "uploadThroughput": None, "latency": "80", "jitter": "8", "memoryFree": None, "memoryTotal": None,
            "cpuUsage": None
        },
        [
            {
                "utilization": "239", "averageRxRate": "9", "averageTxRate": "3", "band": "2.4GHz", "bandWidth": "40MHz",
                "errorsPkt": "0", "ipAddress": "192.168.128.1", "signalStrength": None, "packetsSent": "3052",
                "packetsReceived": "4781", "errorsSent": None, "errorsReceived": None, "bytesSent": "817247",
                "bytesReceived": None, "noise": "54", "associatedDeviceNumberOfEntries": "0", "congestionRate": None,
                "backhaulSta": {"macAddress": "0", "backhaulLinkType": "NULL", "linkRate": "0", "signalStrength": "0", "utilization": "0", "snr": None}
            },
            {
                "utilization": "63", "averageRxRate": "0", "averageTxRate": "0", "band": "5GHz", "bandWidth": "160MHz",
                "errorsPkt": "2385", "ipAddress": "192.168.128.1", "signalStrength": None, "packetsSent": "965170",
                "packetsReceived": "426996", "errorsSent": None, "errorsReceived": None, "bytesSent": "1094121752",
                "bytesReceived": None, "noise": "6", "associatedDeviceNumberOfEntries": "1", "congestionRate": None,
                "backhaulSta": {"macAddress": "0", "backhaulLinkType": "NULL", "linkRate": "0", "signalStrength": "0", "utilization": "0", "snr": None}
            }
        ]
    )
]

# 创建DataFrame
df = spark.createDataFrame(data, schema)

# 定义UDF来解析并拆分数据
def parse_ap_data(device_data_list, control_id, factor):
    result = []
    for device_data in device_data_list:
        band = device_data["band"]
        if "2.4" in band:
            wifi_coverage_score = factor.get("wifiCoverage2GScore")
        elif "5" in band:
            wifi_coverage_score = factor.get("wifiCoverage5GScore")
        elif "6" in band:
            wifi_coverage_score = factor.get("wifiCoverage6GScore")
        else:
            wifi_coverage_score = None

        result.append({
            "id": control_id,
            "band": band,
            "wifiCoverageScore": wifi_coverage_score,
            "utilization": device_data["utilization"],
            "averageRxRate": device_data["averageRxRate"],
            "averageTxRate": device_data["averageTxRate"],
            "channel": device_data["bandWidth"],
            "errorsPkt": device_data["errorsPkt"],
            "ipAddress": device_data["ipAddress"],
            "packetsSent": device_data["packetsSent"],
            "packetsReceived": device_data["packetsReceived"],
            "bytesSent": device_data["bytesSent"],
            "bytesReceived": device_data["bytesReceived"],
            "noise": device_data["noise"]
        })
    return result

parse_ap_data_udf = udf(lambda device_data_list, control_id, factor: parse_ap_data(device_data_list, control_id, factor), ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("wifiCoverageScore", StringType(), True),
    StructField("utilization", StringType(), True),
    StructField("averageRxRate", StringType(), True),
    StructField("averageTxRate", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("errorsPkt", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("packetsSent", StringType(), True),
    StructField("packetsReceived", StringType(), True),
    StructField("bytesSent", StringType(), True),
    StructField("bytesReceived", StringType(), True),
    StructField("noise", StringType(), True)
])))

# 应用UDF并展平结果
df_split = df.withColumn("parsed_data", explode(parse_ap_data_udf(col("collectionData"), col("id"), col("factor")))).select("parsed_data.*")

# 展示结果
df_split.show(truncate=False)

# 停止SparkSession
spark.stop()
