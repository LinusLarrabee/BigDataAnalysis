from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, explode
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType, IntegerType
import json

def process_data(input_df):
    # 定义解析 JSON 数据的函数
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

            return {
                "qoeType": qoe_type,
                "controllerId": control_id,
                "collectionTime": str(collection_time),
                "deviceDataList": json.dumps(device_data_list)
            }
        except Exception as e:
            raise ValueError(f"Error parsing JSON: {e}")

    # 定义解析所有数据的函数
    def parse_data(device_data_str, collection_time, control_id):
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
                is_controller = factor.get("isController") == "1"
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

    # 定义 UDF 返回的 schema
    schema = StructType([
        StructField("qoeType", StringType(), True),
        StructField("collectionTime", StringType(), True),
        StructField("controllerId", StringType(), True),
        StructField("deviceDataList", StringType(), True)  # 存储为字符串
    ])

    # 注册 UDF
    extract_udf = udf(extract_qoe, schema)

    # 应用 UDF 提取 QoeType 和 QoeData 部分，并展开为单独的列
    df_qoe_kind = input_df.withColumn("Qoe", extract_udf(col("value"))).select(col("Qoe.*"))

    # 按 qoeType 判断是否进一步解析
    df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "AP_DATA")

    # 注册解析所有数据的UDF
    parse_data_udf = udf(lambda device_data_str, collection_time, control_id: parse_data(device_data_str, collection_time, control_id), ArrayType(StructType([
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

    # 应用UDF并展平结果
    df_split = df_ap_data.withColumn(
        "parsed_data",
        explode(parse_data_udf(col("deviceDataList"), col("collectionTime"), col("controllerId")))
    ).select("parsed_data.*")

    return df_split


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json
from datetime import datetime, timedelta

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("S3 Data Processing and Saving") \
    .getOrCreate()

# 定义生成日期范围的函数
def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current = start
    while current <= end:
        yield current.strftime("%Y/%m/%d")
        current += delta

# 定义读取数据的函数
def read_data_from_s3(path):
    try:
        file_rdd = spark.sparkContext.textFile(path)
        json_list = file_rdd.map(lambda x: json.loads(x)).collect()
        return json_list
    except Exception as e:
        print(f"Error reading data from {path}: {e}")
        return []

# 定义process_data函数
def process_data(input_df):
    # 示例处理函数，可以根据实际需要进行修改
    processed_df = input_df.withColumn("value_length", input_df["value"].length())
    return processed_df

# 参数
bucket = 'beta-tauc-data-analysis'
input_prefix = 'qoe/uat/aps1'  # 输入路径的前缀部分
start_date = '2024-07-01'  # 起始日期
end_date = '2024-07-10'    # 结束日期
output_path = 's3://beta-tauc-data-analysis/ap-data/uat/aps1/a.csv'

# 初始化一个空的DataFrame
schema = StructType([
    StructField("mainDeviceId", StringType(), True),
    StructField("value", StringType(), True)
])
df = spark.createDataFrame([], schema)

# 生成日期范围并读取数据
for date_str in generate_date_range(start_date, end_date):
    input_path = f's3://{bucket}/{input_prefix}/{date_str}/message-*.txt'
    data = read_data_from_s3(input_path)
    if data:
        df_day = spark.createDataFrame(data, schema)
        df = df.union(df_day)

# 调用process_data函数进行数据处理
output_df = process_data(df)

# 存储处理后的数据到a.csv
output_df.coalesce(1).write.csv(output_path, mode='overwrite', header=True)

# 停止SparkSession
spark.stop()
