from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Extract nested message") \
    .getOrCreate()

# 示例字符串
data = [
    ("{\"messageId\":\"89f722a4-b18f-43a7-bd99-5cebd781150f\",\"totalSize\":444,\"size\":444,\"totalIndex\":1,\"index\":1,\"payload\":\"{\\\"filterKey\\\":\\\"data_collector\\\",\\\"message\\\":\\\"{\\\\\\\"dataCollectorDTO\\\\\\\":{\\\\\\\"tz\\\\\\\":8,\\\\\\\"lg\\\\\\\":\\\\\\\"zh-CN\\\\\\\",\\\\\\\"uvi\\\\\\\":\\\\\\\"b1902f4c3ab5bf73\\\\\\\",\\\\\\\"sr\\\\\\\":\\\\\\\"tauc\\\\\\\",\\\\\\\"srp\\\\\\\":{\\\\\\\"apv\\\\\\\":null,\\\\\\\"plv\\\\\\\":\\\\\\\"windows 10-Edge 126.0.0.0\\\\\\\",\\\\\\\"scr\\\\\\\":\\\\\\\"1920*1080+1\\\\\\\",\\\\\\\"scl\\\\\\\":\\\\\\\"1872*966\\\\\\\"},\\\\\\\"el\\\\\\\":[{\\\\\\\"eid\\\\\\\":\\\\\\\"ReportCenter\\\\\\\",\\\\\\\"ep\\\\\\\":{\\\\\\\"be\\\\\\\":\\\\\\\"Ins\\\\\\\"},\\\\\\\"ct\\\\\\\":1718780443335,\\\\\\\"path\\\\\\\":null,\\\\\\\"usi\\\\\\\":null,\\\\\\\"pvi\\\\\\\":null}],\\\\\\\"ex\\\\\\\":null,\\\\\\\"accountId\\\\\\\":\\\\\\\"\\\\\\\"},\\\\\\\"id\\\\\\\":\\\\\\\"1718780445\\\\\\\"}\\\",\\\"timeStamp\\\":1718780445988}\",\"compressionType\":null,\"compressionLevel\":-1,\"enableSlice\":false}",)
]

# 创建 DataFrame
df = spark.createDataFrame(data, ["message"])

# 提取 payload 字段
df_payload = df.withColumn("payload", regexp_extract("message", "\"payload\":\"(.*?)\",\"compressionType\"", 1))

# 提取 payload 中的 message 字段
df_message = df_payload.withColumn("nested_message", regexp_extract("payload", "\\\\\"message\\\\\":\\\\\"(.*?)\\\\\"", 1))

# 显示结果
df_message.select("nested_message").show(truncate=False)

# 停止 SparkSession
spark.stop()
