from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, udf
from pyspark.sql.types import StringType
import jieba

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("WordSegmentationToHive") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 CSV 文件（逗号分隔）
csv_path = "/user/soukou/Comments.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True, sep=",")

# 加载停用词（HDFS 路径）
stopwords_rdd = spark.sparkContext.textFile("/user/soukou/stopwords.txt")
stopwords = set(stopwords_rdd.collect())

# 定义分词函数
def jieba_cut(text):
    if not text:
        return ""
    words = jieba.lcut(text)
    filtered = [word for word in words if len(word) > 1 and word not in stopwords and word.strip()]
    return ", ".join(filtered)

jieba_udf = udf(jieba_cut, StringType())

# 应用分词并展开词语
df_words = df.filter(col("comment").isNotNull()) \
    .withColumn("words", jieba_udf(col("comment"))) \
    .withColumn("word", explode(split(col("words"), ", "))) \
    .filter(col("word") != "") \
    .select("user", "word", "time")

# 写入 Hive 表（按日期分区）
df_words.write \
    .partitionBy("time") \
    .mode("append") \
    .saveAsTable("wordcloud.comment_words")

print("分词结果已成功写入 Hive 表。")