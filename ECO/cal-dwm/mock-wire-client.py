from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import random

# Initialize Spark session
spark = SparkSession.builder.appName("MockMultiAPData").getOrCreate()

# Define schema
schema = StructType([
    StructField("controller_id", StringType(), True),
    StructField("collection_time", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("mac_address", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("up_speed", IntegerType(), True),
    StructField("down_speed", IntegerType(), True),
    StructField("link_speed", IntegerType(), True),
    StructField("duplex_mode", StringType(), True),
    StructField("active", IntegerType(), True),
    StructField("packets_sent", IntegerType(), True),
    StructField("packets_received", IntegerType(), True),
    StructField("errors_sent", IntegerType(), True),
    StructField("errors_received", IntegerType(), True),
    StructField("interface_type", StringType(), True),
    StructField("sta_count", IntegerType(), True)
])

# Example values from wireless data (replace with your own sample data)
sample_controllers = ['00:0A:EB:13:B1:CC', '00:FF:00:40:06:26', '00:FF:EB:46:E4:C9']
sample_times = ['1725895400', '1725870020', '1725882260']
sample_devices = ['00:0A:EB:13:B1:CC', '7C:F1:7E:4C:A5:2C', '74:FE:CE:B7:D1:68']

# Generate mock data
mock_data = [
    Row(
        controller_id=random.choice(sample_controllers),
        collection_time=random.choice(sample_times),
        device_id=random.choice(sample_devices),
        mac_address=f"{random.randint(0, 255):02X}:{random.randint(0, 255):02X}:{random.randint(0, 255):02X}:{random.randint(0, 255):02X}:{random.randint(0, 255):02X}:{random.randint(0, 255):02X}",
        ip_address=f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",
        host_name=f"host-{random.randint(1, 100)}",
        up_speed=random.randint(100, 1000),
        down_speed=random.randint(100, 1000),
        link_speed=random.randint(100, 1000),
        duplex_mode=random.choice(["full", "half"]),
        active=random.randint(0, 1),
        packets_sent=random.randint(0, 10000),
        packets_received=random.randint(0, 10000),
        errors_sent=random.randint(0, 100),
        errors_received=random.randint(0, 100),
        interface_type=random.choice(["wifi", "ethernet"]),
        sta_count=random.randint(1, 10)
    )
    for _ in range(1000)  # Create 1000 mock records
]

# Convert the mock data into a DataFrame
df_mock = spark.createDataFrame(mock_data, schema)

# Write the mock data to a Parquet file with Snappy compression
output_path = 'mock_multiap_data.snappy.parquet'
df_mock.write.mode("overwrite").parquet(output_path, compression="snappy")

# Stop Spark session
spark.stop()
