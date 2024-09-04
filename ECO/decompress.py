from pyspark.sql import SparkSession, Row

# 初始化SparkSession
spark = SparkSession.builder.appName("ReadEvenLinesFromS3").getOrCreate()
sc = spark.sparkContext

# S3存储桶路径
input_path = 's3://aps1-tauc-data-analysis/source/qoe-raw/2024/07/26/messages-111.txt.gz'
file_rdd = sc.textFile(input_path)
print("First 10 lines from specific file:")
print(file_rdd.take(10))  # 查看该文件中的前 10 行


# 初始化存储偶数行的列表
even_lines = []

# 处理文件内容，只保留偶数行
for line in file_rdd.collect():
    for i, line_content in enumerate(line.splitlines(), 1):  # 从 1 开始计数
        if i % 2 == 0:  # 偶数行
            even_lines.append(line_content.strip())
            if len(even_lines) == 10:  # 只取前十个偶数行
                break
    if len(even_lines) == 10:
        break

# 检查是否读取到了偶数行
if not even_lines:
    print("No even lines were extracted.")
else:
    # 打印前十个偶数行
    for line in even_lines:
        print(line)

    # 将偶数行转换为 Row 对象列表
    rows = [Row(value=line) for line in even_lines]

    # 创建 DataFrame
    df = spark.createDataFrame(rows)

    # 显示 DataFrame 内容
    df.show(truncate=False)

# 停止Spark会话
spark.stop()
