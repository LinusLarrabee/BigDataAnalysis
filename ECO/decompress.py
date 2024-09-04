from pyspark.sql import SparkSession, Row
import gzip
import io

# 初始化SparkSession
spark = SparkSession.builder.appName("ReadEvenLinesFromS3").getOrCreate()
sc = spark.sparkContext

# S3存储桶路径
input_path = 's3://aps1-tauc-data-analysis/source/qoe-raw/2024/07/26/'  # 替换为你的S3路径

# 使用Spark列出符合条件的文件（以messages-开头，.txt.gz结尾）
file_rdd = sc.wholeTextFiles(input_path + "messages-*.txt.gz")

# 初始化存储偶数行的列表
even_lines = []

# 处理S3上的文件内容，解压并只保留偶数行
for file_path, content in file_rdd.collect():
    # 将文件内容作为gzip内容读取
    with gzip.open(io.BytesIO(content.encode()), 'rt') as f:  # 'rt' 模式表示以文本形式读取
        for i, line in enumerate(f, 1):  # 从 1 开始计数
            if i % 2 == 0:  # 偶数行
                even_lines.append(line.strip())
                if len(even_lines) == 10:  # 只取前十个偶数行
                    break
    if len(even_lines) == 10:
        break

# 打印前十个偶数行
for line in even_lines:
    print(line)

# 将偶数行转换为 Row 对象列表
rows = [Row(value=line) for line in even_lines]

# 创建 DataFrame
df = spark.createDataFrame(rows)

# 显示 DataFrame 内容（可选）
df.show(truncate=False)

# 停止Spark会话
spark.stop()
