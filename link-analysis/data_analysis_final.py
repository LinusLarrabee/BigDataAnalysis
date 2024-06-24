from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessMessages").getOrCreate()

# Read the text file into an RDD
rdd = spark.sparkContext.textFile("/Users/sunhao/message.txt")

# Function to process each row
def process_row(row):
    row = row.replace(r'\"', '"').replace(r'\\', '')
    json_data = json.loads(row)
    payload = json.loads(json_data['payload'])
    message = json.loads(payload['message'])
    return json.dumps(message)

# Register the function as a UDF
process_row_udf = udf(process_row, StringType())

# Convert RDD to DataFrame
df = rdd.toDF(["raw_message"])

# Apply the UDF to process the raw message
processed_df = df.withColumn("processed_message", process_row_udf(col("raw_message")))

# Extract payload and message fields
processed_df = processed_df.select(
    col("processed_message"),
    col("raw_message")
)

# Show the processed DataFrame
processed_df.show(truncate=False)
