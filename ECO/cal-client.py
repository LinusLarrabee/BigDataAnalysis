from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder.appName("Extract Client Data").getOrCreate()

# 1. 读取 client_data 的 Parquet 文件
client_data_path = "/Users/sunhao/s3/client_data.parquet"
df_client = spark.read.parquet(client_data_path)

# 2. 提取指定的字段
df_client_selected = df_client.select(
    "controller_id",
    "device_id",
    "band",
    "collection_time",
    "est_mac_data_rate_downlink",
    "est_mac_data_rate_uplink"
)

# 3. 将提取后的数据保存到目标目录
client_output_path = "/Users/sunhao/s3/target/wireless"
df_client_selected.write.mode('overwrite').parquet(client_output_path, compression='snappy')

# 停止 SparkSession
spark.stop()
