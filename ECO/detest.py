from pyspark.sql import SparkSession, Row

# 初始化 SparkSession
spark = SparkSession.builder.appName("ProcessEvenLines").getOrCreate()

# 示例行列表（假设你的偶数行为 payload 数据）
lines = [
    "Key: 00FF00-Device2-T232053000009, Partition: 4, Offset: 197006",  # 奇数行
    "{\"messageId\":\"3e026360-9d4d-432c-8868-5b8ec0e5766f\",\"totalSize\":2426,...}",  # 偶数行
    "Key: 00FF00-Device2-T232053000010, Partition: 4, Offset: 197007",  # 奇数行
    "{\"messageId\":\"4e026360-9d4d-432c-8868-5b8ec0e5766g\",\"totalSize\":3000,...}",  # 偶数行
    # 更多行...
]

# 创建一个空列表存储偶数行的 payload
even_lines = []

# 遍历并提取偶数行（假设偶数行是 payload 数据）
for i in range(1, len(lines), 2):  # 从索引 1 开始，步长为 2
    even_lines.append(lines[i])

# 将偶数行转换为 Row 对象列表
rows = [Row(payload=line) for line in even_lines]

# 创建 DataFrame
df = spark.createDataFrame(rows)

# 显示 DataFrame
df.show(truncate=False)
