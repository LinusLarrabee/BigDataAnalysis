from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建 SparkSession
spark = SparkSession.builder.appName("Extract AP Data and Save").getOrCreate()

# 原始数据路径
input_data_path = "/Users/sunhao/s3"

# 1. 读取 ap_data 的 Parquet 文件
ap_data_path = f"{input_data_path}/ap_data.parquet"
df_ap = spark.read.parquet(ap_data_path)

# 2. 提取 backhaul 所需的字段，并保存到 /target/backhaul
df_backhaul = df_ap.filter(df_ap['is_controller'] == 0).select(
    "controller_id",
    "device_id",
    "band",
    "collection_time",
    "backhaul_sta_mac_address",
    "backhaul_sta_backhaul_link_type",
    "backhaul_sta_link_rate",
    "backhaul_sta_signal_strength",
    "backhaul_sta_utilization"
)

# 输出路径：backhaul
backhaul_output_path = "/Users/sunhao/s3/target/backhaul"
df_backhaul.write.mode('overwrite').parquet(backhaul_output_path, compression='snappy')

# 3. 提取 detailedap 所需的字段，并保存到 /target/detailedap
df_detailedap = df_ap.select(
    "controller_id",
    "device_id",
    "band",
    "collection_time",
    "average_rx_rate",
    "average_tx_rate",
    "congestion_score",
    "wifi_coverage_score",
    "noise",
    "errors_pkt",
    "packets_received",
    "packets_sent",
    "wan_bandwidth"
)

# 输出路径：detailedap
detailedap_output_path = "/Users/sunhao/s3/target/detailedap"
df_detailedap.write.mode('overwrite').parquet(detailedap_output_path, compression='snappy')

# 停止 SparkSession
spark.stop()
