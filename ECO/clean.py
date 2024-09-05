from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建 SparkSession
spark = SparkSession.builder.appName("CSV to Parquet with Snappy Compression and Date Partitioning").getOrCreate()

# 1. 读取CSV文件
df_ap = spark.read.csv('/Users/sunhao/s3/qoe_rawLocal/ap_data.csv/part-00000-155bdd98-d8b9-41b7-827d-90bde0ba9680-c000.csv', header=True, inferSchema=True)
df_client = spark.read.csv('/Users/sunhao/s3/qoe_rawLocal/client_data.csv/part-00000-57ff38f5-c5e4-40e7-a37c-948ad8d0ca9c-c000.csv', header=True, inferSchema=True)
df_multiap = spark.read.csv('/Users/sunhao/s3/qoe_rawLocal/multiap_data.csv/part-00000-4bdbc864-3899-4433-8bfd-415014005a8e-c000.csv', header=True, inferSchema=True)

# 2. 将collection_time转换为'YYYY-MM-DD'格式
def add_date_column(df):
    # 使用 from_unixtime 将 UNIX 时间戳转换为日期
    return df.withColumn("dt", F.date_format(F.from_unixtime(F.col("collection_time")), 'yyyy-MM-dd'))

# 3. 将CSV文件保存为Snappy压缩的Parquet格式，并在目录中加入日期
def csv_to_parquet(df, table_name):
    # 为每个DataFrame添加日期列
    df_with_date = add_date_column(df)

    # 获取不同的日期并按日期存储
    distinct_dates = df_with_date.select("dt").distinct().collect()

    for row in distinct_dates:
        dt = row["dt"]  # 获取日期字符串
        output_path = f'/Users/sunhao/s3/{table_name}/dt={dt}/'  # 将日期作为路径的一部分

        # 过滤出当前日期的数据并保存
        df_with_date.filter(df_with_date["dt"] == dt) \
            .write.mode('overwrite').parquet(output_path, compression='snappy')

# 4. 将 ap_data、client_data 和 multiap_data 转为 Parquet 格式并按日期存储
csv_to_parquet(df_ap, "ap_data")
csv_to_parquet(df_client, "client_data")
csv_to_parquet(df_multiap, "multiap_data")

# 停止 SparkSession
spark.stop()
