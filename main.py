from weibo_crawler.weibo_crawler import WeiboCrawler
import os

result_dir = "search_result"
if not os.path.exists(result_dir):
    os.makedirs(result_dir)

# 搜索关键词列表
search_keywords_list = [
    "陕西 高温 预警",
    "陕西 干旱 预警",
]

# 单个关键词搜索数量限制
result_count_limit = 100
start_time = "2022-06-01"
end_time = "2022-08-31"

crawl = WeiboCrawler()
for keyword in search_keywords_list:
    crawl.search_weibo(keyword, limit=result_count_limit, export_dir=result_dir, start_time=start_time,
                       end_time=end_time)
