import pymysql
from pyecharts import options as opts
from pyecharts.charts import WordCloud, Line, Page
from pyecharts.components import Table
from pyecharts.globals import ThemeType
from collections import defaultdict
import datetime

# 连接 MySQL
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='wordcloud',
    charset='utf8mb4'
)
cursor = conn.cursor()

# 读取词频数据（Top 500）
cursor.execute("SELECT word, freq FROM word_freq ORDER BY freq DESC LIMIT 500")
word_freq_data = cursor.fetchall()
words = [(row[0], row[1]) for row in word_freq_data]

# 读取时间趋势数据（按天统计词频）
cursor.execute("""
    SELECT DATE(time) as date, word, COUNT(*) as freq
    FROM (
        SELECT comment, time, word
        FROM comments
        JOIN (
            SELECT word FROM word_freq ORDER BY freq DESC LIMIT 20
        ) AS top_words
        ON comment LIKE CONCAT('%', word, '%')
    ) AS sub
    GROUP BY DATE(time), word
    ORDER BY date
""")
trend_data = cursor.fetchall()

# 处理趋势数据
trend_dict = defaultdict(lambda: defaultdict(int))
dates = set()
for date, word, freq in trend_data:
    trend_dict[word][date] = freq
    dates.add(date)

dates = sorted(dates)
top_words = list(trend_dict.keys())[:10]  # 取前10个高频词展示趋势

# 生成趋势图
line = (
    Line(init_opts=opts.InitOpts(width="1000px", height="400px"))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="热点词汇变化趋势"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        legend_opts=opts.LegendOpts(orient="horizontal", pos_top="5%"),
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(type_="value"),
    )
)

for word in top_words:
    freq_list = [trend_dict[word][date] for date in dates]
    line.add_xaxis([str(d) for d in dates])
    line.add_yaxis(word, freq_list, is_smooth=True)

# 生成词云图
wordcloud = (
    WordCloud(init_opts=opts.InitOpts(width="1000px", height="600px"))
    .add(
        series_name="词频统计",
        data_pair=words,
        word_size_range=[20, 100],
        shape="circle",
        tooltip_opts=opts.TooltipOpts(is_show=True, formatter="{b}: {c}"),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="评论词云图", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(is_show=True),
    )
)

# 定义点击词云显示详情函数
def generate_detail_page(word):
    cursor.execute("SELECT freq FROM word_freq WHERE word = %s", (word,))
    freq_result = cursor.fetchone()
    freq = freq_result[0] if freq_result else 0

    cursor.execute(
        "SELECT user, comment, time FROM comments WHERE comment LIKE %s ORDER BY time DESC LIMIT 3",
        (f"%{word}%",)
    )
    comments = cursor.fetchall()

    table = Table()
    headers = ["用户", "评论", "时间"]
    rows = [[row[0], row[1], str(row[2])] for row in comments]
    table.add(headers, rows)
    table.set_global_opts(title_opts=opts.ComponentTitleOpts(title=f"“{word}”出现次数：{freq}"))
    return table

# 组合页面
page = Page()
page.add(wordcloud, line)

# 生成 HTML 文件
output_html = "wordcloud_trend_interactive.html"
page.render(output_html)

# 关闭数据库连接
cursor.close()
conn.close()

print(f"可视化页面已生成：{output_html}")