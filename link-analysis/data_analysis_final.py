import json
import sys
import logging
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession

# 设置日志级别
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PathTransformation')

def log(message):
    logger.info(message)
    sys.stdout.flush()

def extract_data_from_json(json_str):
    try:
        json_obj = json.loads(json_str)
    except Exception as e:
        log(f"Error parsing JSON object: {e}")
        return []

    try:
        payload_str = json_obj.get('payload')
    except Exception as e:
        log(f"Error getting 'payload' from JSON object: {e}")
        return []

    try:
        payload = json.loads(payload_str)
    except Exception as e:
        log(f"Error parsing 'payload' JSON string: {e}")
        return []

    try:
        message_str = payload.get('message')
    except Exception as e:
        log(f"Error getting 'message' from payload: {e}")
        return []

    try:
        message = json.loads(message_str)
    except Exception as e:
        log(f"Error parsing 'message' JSON string: {e}")
        return []

    try:
        data_collector = message.get('dataCollectorDTO')
    except Exception as e:
        log(f"Error getting 'dataCollectorDTO' from message: {e}")
        return []

    try:
        uvi = data_collector.get('uvi')
        events = data_collector.get('el')
        extracted_data = []
        for event in events:
            eid = event.get('eid')
            ct = event.get('ct')
            extracted_data.append((uvi, eid, ct))
        return extracted_data
    except Exception as e:
        log(f"Error parsing events: {e}")
        return []

def main():
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

        # 本地文件路径
        local_path = '/Users/sunhao/message.txt'
        log(f"Reading data from {local_path}")

        # 从本地文件读取数据
        file_rdd = sc.textFile(local_path)
        log(f"File content read from {local_path}")

        # 读取文件内容，每行一个 JSON 对象
        json_list = file_rdd.collect()
        log(f"Number of JSON objects read: {len(json_list)}")

        # 解析JSON对象并提取所需的字段
        parsed_json_list = []
        for json_str in json_list:
            extracted_data = extract_data_from_json(json_str)
            parsed_json_list.extend(extracted_data)

        # 确保输出路径存在
        output_dir = "/mnt/data/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, "extracted_data.txt")

        # 保存提取的数据到 txt 文件
        with open(output_path, 'w') as f:
            for record in parsed_json_list:
                f.write(f"{record[0]},{record[1]},{record[2]}\n")

        log(f"Data successfully extracted and saved to {output_path}")

    except Exception as e:
        log(f"An error occurred: {str(e)}")

    finally:
        # 关闭 SparkSession
        spark.stop()
        log("SparkSession stopped.")

if __name__ == "__main__":
    main()
