from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType, IntegerType
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

        # 将device_data_list转换为字符串
        device_data_list = json.dumps(device_data_list)

        return {
            "qoeType": qoe_type,
            "controllerId": control_id,
            "collectionTime": str(collection_time),
            "deviceDataList": device_data_list
        }
    except Exception as e:
        raise ValueError(f"Error parsing JSON: {e}")

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

# 显示结果（不显示最后一列 deviceDataList）
# df_qoe_kind.drop("deviceDataList").show(truncate=False)
df_qoe_kind.show(truncate=False)

# 按 qoeType 判断是否进一步解析
df_ap_data = df_qoe_kind.filter(df_qoe_kind.qoeType == "AP_DATA")

# 定义进一步解析函数
def parse_ap_data(device_data_list, control_id):
    try:
        control_id = control_id.strip().replace(":", "")
        device_data_list = json.loads(device_data_list)
        controller_radio = []
        wifi_coverage_2g_score = None
        for device in device_data_list:
            device_id = device.get('id', '').strip().replace(":", "")
            print(f"Comparing device_id: {device_id} with control_id: {control_id}")
            if device_id == control_id:

                wifi_coverage_2g_score = device['factor']['wifiCoverage2GScore']
                wifi_coverage_5g_score = device['factor']['wifiCoverage5GScore']
                wifi_coverage_6g_score = device['factor']['wifiCoverage6GScore']
                radio_detail = device['collectionData']
                for radio in radio_detail:
                    controller_radio.append({
                        "band": radio["band"],
                        "channel": radio['channel'],
                        "noise": radio["noise"],
                        "congestion_rate": radio["congestion_rate"]
                    })



        return wifi_coverage_2g_score,wifi_coverage_5g_score,wifi_coverage_6g_score
    except Exception as e:
        return 0,None,None,None


# 注册 UDF 进行进一步解析
parse_ap_data_udf = udf(lambda device_data_list, control_id: parse_ap_data(device_data_list, control_id), ArrayType(StructType([
    # StructField("device_id", StringType(), True),
    StructField("band", StringType(), True),
    StructField("channel", IntegerType(), True),
    StructField("noise", StringType(), True),
    StructField("congestion_rate", StringType(), True),
    # StructField("packet_error_rate", StringType(), True),
    # StructField("wan_bandwidth", StringType(), True),
    # StructField("wifi_coverage", StringType(), True),
    # StructField("tx_rate", StringType(), True),
    # StructField("rx_rate", IntegerType(), True)
])))

# 解析 deviceDataList 列为数组并进行进一步解析
df_ap_data = df_ap_data.withColumn("parsedData", parse_ap_data_udf(col("deviceDataList"), col("controllerId"))) \
    .select(col("*"), col("parsedData.*")).drop("deviceDataList")

# 显示进一步解析的结果，打印解析出来的JSON数据
df_ap_data.show(truncate=False)

# 按 controllerId 列进行分区保存
# output_base_path = "/euw1/your-isp-name/apData/2024/06/24"
# df_ap_data.write.partitionBy("controllerId").mode("overwrite").parquet(output_base_path)

# 停止SparkSession
spark.stop()
