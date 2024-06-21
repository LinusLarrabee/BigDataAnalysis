import json
import sys
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, MapType, LongType
from datetime import datetime, timedelta

# 设置日志级别
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PathTransformation')

def log(message):
    logger.info(message)
    sys.stdout.flush()

def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    while start <= end:
        yield start.strftime("%Y/%m/%d")
        start += delta

def main(start_date, end_date, new_table_name_prefix, processed_table_name_prefix):
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

        all_json_list = []
        bucket = 'beta-tauc-data-analysis'

        for date_str in generate_date_range(start_date, end_date):
            input_path = f's3://{bucket}/local/uat/aps1/{date_str}/messages-*.json'
            log(f"Reading data from {input_path}")

            # 从S3读取文件内容
            file_rdd = sc.textFile(input_path)
            log(f"File content read from {input_path}")

            # 读取文件内容，每行一个 JSON 对象
            json_list = file_rdd.map(lambda x: json.loads(x)).collect()
            log(f"Number of JSON objects read: {len(json_list)}")
            all_json_list.extend(json_list)

        # 检查是否读取到任何数据
        if not all_json_list:
            log("No data found for the specified date range.")
            return

        # 添加调试日志以查看所有 JSON 对象的结构
        for i, json_obj in enumerate(all_json_list[:5]):  # 只打印前5个对象以避免日志过长
            log(f"JSON object {i}: {json.dumps(json_obj, indent=2)}")

        # 解析JSON对象中的"payload"字段并加载其内容
        parsed_json_list = []
        for json_obj in all_json_list:
            try:
                if isinstance(json_obj, list):
                    continue  # 跳过列表对象
                payload_str = json_obj.get('payload')
                payload = json.loads(payload_str)
                message = json.loads(payload.get('message'))
                data_collector = message.get('dataCollectorDTO')
                parsed_json_list.append({
                    'uvi': data_collector.get('uvi'),
                    'el': data_collector.get('el')
                })
            except Exception as e:
                log(f"Error parsing JSON object: {e}")
                continue

        # 定义数据模式
        schema = StructType([
            StructField("uvi", StringType(), True),
            StructField("el", ArrayType(
                StructType([
                    StructField("eid", StringType(), True),
                    StructField("ep", MapType(StringType(), StringType()), True),
                    StructField("ct", LongType(), True),
                    StructField("path", StringType(), True),
                    StructField("usi", StringType(), True),
                    StructField("pvi", StringType(), True)
                ])
            ), True)
        ])

        # 创建 DataFrame
        df = spark.createDataFrame(parsed_json_list, schema)
        log("DataFrame created.")
        log(f"DataFrame count: {df.count()}")

        # 暂时跳过 `eid` 为 `pageView` 的过滤操作，只展开 `el` 列
        df = df.withColumn("el", F.explode("el")) \
            .withColumn("eid", F.col("el.eid")) \
            .withColumn("ep", F.col("el.ep")) \
            .withColumn("ct", F.col("el.ct")) \
            .withColumn("path", F.col("el.path")) \
            .withColumn("usi", F.col("el.usi")) \
            .withColumn("pvi", F.col("el.pvi")) \
            .drop("el") \
            .withColumn("ct", F.to_timestamp(F.col("ct") / 1000))
        log("Transformed DataFrame.")
        log(f"DataFrame count after transformation: {df.count()}")

        # 重新聚合数据，按 `ct` 排序
        df = df.groupBy("uvi").agg(F.collect_list(F.struct("eid", "ep", "ct", "path", "usi", "pvi")).alias("el"))
        df = df.withColumn("el", F.expr("array_sort(el, (left, right) -> case when left.ct < right.ct then -1 when left.ct > right.ct then 1 else 0 end)"))
        log("Aggregated and sorted DataFrame.")
        log(f"DataFrame count after aggregation: {df.count()}")

        # 展示结果
        log("Transformed DataFrame:")
        df.select("uvi", "el").show(truncate=False)

        # 删除已有的Hive表
        new_table_name = f"{new_table_name_prefix}_{start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}"
        processed_table_name = f"{processed_table_name_prefix}_{start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}"

        log(f"Dropping table if it exists: {new_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {new_table_name}")

        # 将 DataFrame 写入 Hive 表
        log(f"Writing DataFrame to Hive table: {new_table_name}")
        df.write.mode("overwrite").format("parquet").saveAsTable(new_table_name)

        # 验证写入的表
        log(f"Reading from Hive table: {new_table_name}")
        spark.sql(f"SELECT * FROM {new_table_name}").show()

        # 确定输出路径
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        result_path = f"s3://{bucket}/result/link-analysis/{start_date}_to_{end_date}/{timestamp}/"

        # 保存结果到 S3
        log(f"Saving results to S3: {result_path}")
        df.select("uvi", "el").write.json(result_path)

        # 新增逻辑：统计链路数量并存储到Hive
        log("Starting path chain count processing...")

        # 统计链路数量
        path_counts = df.withColumn("transformed_path", F.expr("transform(el, x -> x.path)")) \
            .withColumn("transformed_path", F.expr("concat_ws(' -> ', transformed_path)")) \
            .groupBy("transformed_path").count()
        log("Path chain counts calculated.")
        log(f"Path chain counts DataFrame count: {path_counts.count()}")

        # 处理链路数据
        def process_paths(path, count):
            pages = path.split(" -> ")
            paths = []
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
        log("Usage: script <start_date> <end_date> <new_table_name_prefix> <processed_table_name_prefix>")
        sys.exit(-1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    new_table_name_prefix = sys.argv[3]
    processed_table_name_prefix = sys.argv[4]

    main(start_date, end_date, new_table_name_prefix, processed_table_name_prefix)
