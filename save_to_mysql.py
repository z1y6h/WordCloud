import pymysql

# 读取词频文件
word_freq = []
with open("/usr/local/spark/mycode/python/WordCloud/all_words.txt", "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            word, freq = parts
            word_freq.append((word, int(freq)))

# 连接MySQL
conn = pymysql.connect(
    host='localhost',      # MySQL服务器地址
    user='root',           # 用户名
    password='123456',    # 密码
    database='wordcloud',  # 数据库名
    charset='utf8mb4'
)
cursor = conn.cursor()

# 插入数据
for word, freq in word_freq:
    cursor.execute(
        "INSERT INTO word_freq (word, freq) VALUES (%s, %s)",
        (word, freq)
    )

conn.commit()
cursor.close()
conn.close()
print("写入MySQL完成！")