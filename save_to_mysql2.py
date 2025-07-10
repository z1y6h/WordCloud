import pymysql
import csv

# 连接MySQL
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='wordcloud',
    charset='utf8mb4'
)
cursor = conn.cursor()

# 创建表（如果不存在）
cursor.execute("""
CREATE TABLE IF NOT EXISTS comments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user VARCHAR(255),
    comment TEXT,
    time DATETIME,
    `like` INT
)
""")

# 读取CSV文件并插入数据
csv_file = '/usr/local/spark/mycode/python/WordCloud/Comments.csv'

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=',')
    header = next(reader)  # 跳过表头
    print("表头:", header)
    for i, row in enumerate(reader):
        if i < 5:
            print("读取到的行:", row)
        if len(row) == 4:
            user, comment, time_str, like_count = row
            try:
                cursor.execute(
                    "INSERT INTO comments (user, comment, time, `like`) VALUES (%s, %s, %s, %s)",
                    (user, comment, time_str, int(like_count))
                )
            except Exception as e:
                print("插入失败:", e, row)
        else:
            print("跳过格式不正确的行:", row)

conn.commit()
cursor.close()
conn.close()
print("CSV 数据写入完成。")