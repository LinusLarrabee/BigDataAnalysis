from pyspark.sql import SparkSession, Row
import gzip

# 初始化SparkSession
spark = SparkSession.builder.appName("ReadEvenLinesFromS3").getOrCreate()
sc = spark.sparkContext

# S3指定文件路径
input_path = 's3://aps1-tauc-data-analysis/source/qoe-raw/2024/07/26/messages-111.txt.gz'

# 读取文件内容
file_rdd = sc.textFile(input_path)

# 打印读取到的前10行，确认文件是否正确读取
first_lines = file_rdd.take(10)
print("First 10 lines from S3 file:")
print(first_lines)

from pyspark.sql import Row

# 偶数行列表
even_lines = []

# 遍历文件的内容，提取偶数行
for i, line in enumerate(first_lines, 1):  # enumerate 从 1 开始计数
    if i % 2 == 0:  # 偶数行
        even_lines.append(line.strip())

# 打印前十个偶数行（如果有）
print("Even lines extracted from the file:")
print(even_lines[:10])  # 打印前十个偶数行

# 将偶数行转换为 Row 对象列表
rows = [Row(payload=line) for line in even_lines]

# 创建 DataFrame
df = spark.createDataFrame(rows)

# 显示 DataFrame
df.show(truncate=False)

df1= spark.read.text(input_path)

# 显示 DataFrame
df1.show(truncate=False)

# 停止Spark会话
spark.stop()
