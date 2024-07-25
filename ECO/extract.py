from pyspark.sql import SparkSession


# 定义处理函数
def extract_data_collector(json_str):
    try:
        replaced_str = json_str.replace('\\', '')
        index_start = replaced_str.find("{\"dataCollectorDTO")
        index_end = replaced_str.find("\",\"timeStamp")

        if index_start == -1 or index_end == -1:
            return "string format error! str= " + replaced_str

        substring = replaced_str[index_start:index_end]
        return substring
    except Exception as e:
        return str(e)


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadSingleFileJSON") \
    .getOrCreate()

# 指定文件路径
file_path = "messages-1721813944482.txt"

# 读取每一行作为一个JSON对象
df = spark.read.json(file_path)

# 显示数据
df.show()

# 停止SparkSession
spark.stop()
