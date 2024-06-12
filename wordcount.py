from pyspark import SparkContext
from pyspark.sql import SparkSession

# 创建SparkContext和SparkSession
sc = SparkContext(appName="WordCount")
spark = SparkSession(sc)

# 读取输入文件
input_path = "s3://linkanalysis/input.txt"
output_path = "s3://linkanalysis/output"

# 读取文本文件
text_file = sc.textFile(input_path)

# 计算单词出现次数
counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# 将结果保存到输出路径
counts.saveAsTextFile(output_path)

# 停止SparkContext
sc.stop()
