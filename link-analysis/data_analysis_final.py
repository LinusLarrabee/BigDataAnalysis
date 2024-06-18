import json
import sys
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from io import StringIO

# 设置日志级别
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PathTransformation')

def log(message):
    logger.info(message)
    sys.stdout.flush()

def main(input_path, output_path, new_table_name, processed_table_name):
    try:
        log("Starting Spark job...")

        # 创建SparkContext和SparkSession
        sc = SparkContext(appName="PathTransformation")
        spark = SparkSession.builder \
            .appName(sc.appName) \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.instances", "4") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "2") \
            .config("spark.dynamicAllocation.maxExecutors", "10") \
            .config("spark.log4jHotPatch.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()

        log("SparkSession created.")

        # 从S3读取文件内容
        file_content = sc.textFile(input_path).collect()
        file_content = "\n".join(file_content)
        log(f"File content read from {input_path}")

        # 读取文件内容，每行一个 JSON 对象
        json_list = [json.loads(line) for line in StringIO(file_content)]

        # 定义数据模式
        schema = StructType([
            StructField("uvi", StringType(), True),
            StructField("el", ArrayType(
                StructType([
                    StructField("path", StringType(), True),
                    StructField("ct", StringType(), True)  # 使用StringType读取时间戳
                ])
            ), True)
        ])

        # 创建 DataFrame
        df = spark.createDataFrame(json_list, schema)
        log("DataFrame created.")
        log(f"DataFrame count: {df.count()}")

        # 转换时间戳字段为 TimestampType
        df = df.withColumn("el", F.explode("el")) \
            .withColumn("path", F.col("el.path")) \
            .withColumn("ct", F.col("el.ct")) \
            .drop("el") \
            .withColumn("ct", F.to_timestamp("ct"))
        log("Timestamp fields converted.")
        log(f"DataFrame count after timestamp conversion: {df.count()}")

        # 重新聚合数据
        df = df.groupBy("uvi").agg(F.collect_list(F.struct("path", "ct")).alias("el"))
        log("Data aggregated.")
        log(f"DataFrame count after aggregation: {df.count()}")

        # 处理路径转换
        def transform_path(el):
            paths = [e["path"] for e in el]
            transformed_path = " -> ".join(paths)
            return transformed_path

        transform_path_udf = F.udf(transform_path, StringType())

        df = df.withColumn("transformed_path", transform_path_udf(F.col("el")))
        log("Path transformation applied.")
        log(f"DataFrame count after path transformation: {df.count()}")

        # 展示结果
        log("Transformed DataFrame:")
        df.select("uvi", "transformed_path").show(truncate=False)

        # 删除已有的Hive表
        log(f"Dropping table if it exists: {new_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {new_table_name}")

        # 将 DataFrame 写入 Hive 表
        log(f"Writing DataFrame to Hive table: {new_table_name}")
        df.write.mode("overwrite").format("parquet").saveAsTable(new_table_name)

        # 验证写入的表
        log(f"Reading from Hive table: {new_table_name}")
        spark.sql(f"SELECT * FROM {new_table_name}").show()

        # 保存结果到 S3
        log(f"Saving results to S3: {output_path}")
        df.select("uvi", "transformed_path").write.csv(output_path, header=True)

        # 新增逻辑：统计链路数量并存储到Hive
        log("Starting path chain count processing...")

        # 统计链路数量
        path_counts = df.groupBy("transformed_path").count()
        log("Path chain counts calculated.")
        log(f"Path chain counts DataFrame count: {path_counts.count()}")

        # 处理链路数据
        def process_paths(path, count):
            pages = path.split(" -> ")
            paths = []
            weights = []
            full_paths = []
            for i in range(len(pages) - 1):
                source_label = f"{pages[i]} ({i+1})"
                target_label = f"{pages[i+1]} ({i+2})"
                paths.append((source_label, target_label, count, path))
            return paths

        # 使用flatMap进行路径处理
        processed_paths = path_counts.rdd.flatMap(lambda row: process_paths(row['transformed_path'], row['count']))

        # 定义处理后的Schema
        processed_schema = StructType([
            StructField("source", StringType(), True),
            StructField("target", StringType(), True),
            StructField("weight", StringType(), True),
            StructField("full_path", StringType(), True)
        ])

        # 创建处理后的DataFrame
        processed_df = spark.createDataFrame(processed_paths, processed_schema)
        log("Processed DataFrame created.")
        log(f"Processed DataFrame count: {processed_df.count()}")

        # 删除已有的Hive表
        log(f"Dropping table if it exists: {processed_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {processed_table_name}")

        # 将处理后的 DataFrame 写入 Hive 表
        log(f"Writing processed DataFrame to Hive table: {processed_table_name}")
        processed_df.write.mode("overwrite").format("parquet").saveAsTable(processed_table_name)

        # 验证写入的表
        log(f"Reading from Hive table: {processed_table_name}")
        spark.sql(f"SELECT * FROM {processed_table_name}").show()

    except Exception as e:
        log(f"An error occurred: {str(e)}")

    finally:
        # 关闭 SparkSession
        spark.stop()
        log("SparkSession stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        log("Usage: script <input_path> <output_path> <new_table_name> <processed_table_name>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    new_table_name = sys.argv[3]
    processed_table_name = sys.argv[4]

    main(input_path, output_path, new_table_name, processed_table_name)
