import json
import sys
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
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

def extract_data_from_json(json_obj):
    extracted_data = []
    try:
        if isinstance(json_obj, dict):
            payload_str = json_obj.get('payload')
            if payload_str:
                payload = json.loads(payload_str)
                message_str = payload.get('message')
                if message_str:
                    message = json.loads(message_str)
                    data_collector = message.get('dataCollectorDTO')
                    if data_collector:
                        uvi = data_collector.get('uvi')
                        events = data_collector.get('el')
                        if isinstance(events, list):
                            for event in events:
                                eid = event.get('eid')
                                ct = event.get('ct')
                                extracted_data.append((uvi, eid, ct))
    except Exception as e:
        log(f"Error parsing JSON object: {e}")
    return extracted_data

def main(start_date, end_date):
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

        log(f"Total JSON objects collected: {len(all_json_list)}")

        # 解析JSON对象并提取所需的字段
        parsed_json_list = []
        for i, json_obj in enumerate(all_json_list):
            extracted_data = extract_data_from_json(json_obj)
            parsed_json_list.extend(extracted_data)

        # 打印前十条记录的 uvi, eid, ct
        if parsed_json_list:
            for i, record in enumerate(parsed_json_list[:10]):
                log(f"Record {i}: uvi={record[0]}, eid={record[1]}, ct={record[2]}")
        else:
            log("No records parsed.")

    except Exception as e:
        log(f"An error occurred: {str(e)}")

    finally:
        # 关闭 SparkSession
        spark.stop()
        log("SparkSession stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        log("Usage: script <start_date> <end_date>")
        sys.exit(-1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    main(start_date, end_date)
