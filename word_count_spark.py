import re
import jieba
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ChineseWordCount") \
    .getOrCreate()
sc = spark.sparkContext

stopwords_path = "hdfs:///user/soukou/stopwords.txt"
stopwords = set(sc.textFile(stopwords_path).collect())

text_path = "hdfs:///user/soukou/Comments.csv"
lines = sc.textFile(text_path)
print("原始数据行数：", lines.count())

def clean_text(text):
    # 保留中文和常见中文标点
    text = re.sub(r'[^\u4e00-\u9fa5，。！？、；：“”‘’（）《》]', '', text)
    return text

def tokenize(text):
    text = clean_text(text)
    print("清洗后文本：", text)
    words = jieba.lcut(text)
    print("分词结果：", words)
    return [w for w in words if w not in stopwords]

words = lines.flatMap(tokenize)
print("分词后词语总数：", words.count())

word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

all_word_counts = word_counts.collect()
with open("all_words.txt", "w", encoding="utf-8") as f:
    for word, count in all_word_counts:
        f.write(f"{word}\t{count}\n")

spark.stop()