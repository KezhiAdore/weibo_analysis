from weibo_crawler.weibo_crawler import WeiboCrawler
import os

result_dir = "search_result"
if not os.path.exists(result_dir):
    os.makedirs(result_dir)

# 搜索关键词列表
search_keywords_list = [
    "王冰冰",
    "西安交通大学",
]

# 单个关键词搜索数量限制
result_count_limit = 100

crawl = WeiboCrawler()
for keyword in search_keywords_list:
    crawl.search_weibo(keyword, limit=result_count_limit, export_dir=result_dir)
