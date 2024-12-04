# hello_count.py

from pyspark.sql import SparkSession

# 初始化 Spark 会话
spark = SparkSession.builder.appName("HelloCountApp").getOrCreate()

# 创建一个简单的 DataFrame
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Value"])

# 打印 DataFrame 的内容
df.show()

# 计算行数并打印
row_count = df.count()
print(f"Hello, World! The DataFrame has {row_count} rows.")

# 停止 Spark 会话
spark.stop()
