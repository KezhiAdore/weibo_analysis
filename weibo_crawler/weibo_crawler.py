import os.path
import time
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
import datetime
import pytz
import warnings

from config import MAX_RETRIES, SLEEP_INTERVAL, headers, cookie

warnings.filterwarnings("ignore")

retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=SLEEP_INTERVAL,
    status_forcelist=[403, 500, 502, 503, 504],
)

# check cookie and join cookie in headers if not empty
if cookie:
    headers["cookie"] = cookie


class WeiboCrawler:
    def __init__(self):
        self.client = requests.Session()
        self.client.mount("http://", HTTPAdapter(max_retries=retries))
        self.client.mount("https://", HTTPAdapter(max_retries=retries))

        self.weibo_datetime_format = "%a %b %d %H:%M:%S %z %Y"
        self.datetime_format = "%Y-%m-%d"

    def search_weibo(self, keyword, limit=100, export_dir=".", start_time="", end_time=""):
        page = 0
        weibo_df = pd.DataFrame()
        while True:
            time.sleep(0.1)
            page += 1
            search_result = self._get_search_result(keyword, page)
            if not len(search_result.get('data').get('cards')):
                break
            temp_weibo_df = self._parse_search_result(search_result, start_time, end_time)
            weibo_df = weibo_df.append(temp_weibo_df)

            if not weibo_df.empty:
                export_path = os.path.join(export_dir, keyword + ".xlsx")
                weibo_df.to_excel(export_path)

            if len(weibo_df) > limit:
                break

    def _get_search_result(self, keyword, page):
        url = f"https://m.weibo.cn/api/container/getIndex"
        params = {
            "containerid": "100103type=61&q={}".format(keyword),
            "page_type": "searchall",
            "page": page
        }
        response = self.client.get(url, params=params)
        if response.status_code == 200:
            response = response.json()
        else:
            raise ValueError("{}访问状态: {}".format(url, response.status_code))
        return response

    def _get_long_text(self, id):
        params = {
            'id': id
        }
        url = "https://m.weibo.cn/statuses/extend"
        response = self.client.get(url, params=params)
        if response.status_code == 200:
            response = response.json()
        else:
            raise ValueError("{}访问状态: {}".format(url, response.status_code))
        data = response.get('data')
        return data.get("longTextContent")

    def _parse_search_result(self, search_result, start_time="", end_time=""):
        weibo_df = pd.DataFrame()
        items = search_result.get('data').get('cards')
        if not len(items):
            return weibo_df
        for index, item in enumerate(items):
            if item.get('card_type') == 7:  # 导语
                continue
            elif item.get('card_type') == 8 or (item.get('card_type') == 11 and item.get('card_group') is None):
                continue
            if item.get('mblog', None):
                item = item.get('mblog')
            else:
                item = item.get('card_group')[0].get('mblog')
            if item:
                # 检查是否在要求的时间区间内
                if not self.check_time(start_time, end_time, item.get('created_at')):
                    continue

                publish_time = self._str2datetime(item.get('created_at'), self.weibo_datetime_format).replace(tzinfo=None)
                data = {
                    'wid': item.get('id'),
                    'user_name': item.get('user').get('screen_name'),  # 用户昵称
                    'user_id': item.get('user').get('id'),  # 用户id
                    'gender': item.get('user').get('gender'),  # 用户性别
                    'publish_time': publish_time,  # 发布时间
                    'text': item.get("text"),  # 仅提取内容中的文本
                    'like_count': item.get('attitudes_count'),  # 点赞数
                    'comment_count': item.get('comments_count'),  # 评论数
                    'forward_count': item.get('reposts_count'),  # 转发数
                    'origin_publish_time': item.get('created_at'),  # 原发布时间
                }
                if item.get('isLongText'):
                    long_text = self._get_long_text(item.get('id'))  # 获取长文本
                    data["text"] = long_text

                weibo_df = weibo_df.append(data, ignore_index=True)
        return weibo_df

    @staticmethod
    def _str2datetime(string, datetime_format):
        return datetime.datetime.strptime(string, datetime_format)

    def get_uid(self, nickname: str) -> int:
        """get uid by nickname

        Args:
            nickname (str): the precise nickname of the user

        Returns:
            int: the uid of the user
        """
        url = "https://weibo.com/n/{}".format(nickname)
        r = self.client.get(url, headers=headers)
        if r.status_code != 200:
            print("Error: {}".format(r.status_code))
            raise ValueError(f"{nickname} is not a weibo user name or weibo has a bad response")
        else:
            return r.json()["data"]["uid"]

    def check_time(self, start_time, end_time, weibo_time):
        check_result = True
        weibo_time = datetime.datetime.strptime(weibo_time, self.weibo_datetime_format)

        if start_time:
            start_time = datetime.datetime.strptime(start_time, self.datetime_format)
            start_time = start_time.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
            if weibo_time < start_time:
                check_result = False

        if end_time:
            end_time = datetime.datetime.strptime(end_time, self.datetime_format)
            end_time = end_time.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
            if weibo_time > end_time:
                check_result = False

        return check_result

    @staticmethod
    def check_keyword_list(keyword_list: list, text: str) -> bool:
        """check if the text contains one of the keywords

        Args:
            keyword_list (list): keywords list
            text (str): raw text of weibo

        Returns:
            bool: True if the text contains one of the keywords, False otherwise
        """
        if not keyword_list:
            return True
        else:
            for keyword in keyword_list:
                if keyword in text:
                    return True
            return False

    def get_weibo_by_uid(self, uid: int, start: str = None, end: str = None) -> pd.DataFrame:
        """get weibo info by uid

        Args:
            uid (int): the uid of the weibo user
            start (str, optional): the start date, format:"YYYY-MM-DD", for example: "2022-01-01". Defaults to None.
            end (str, optional): the end date, format:"YYYY-MM-DD", for example: "2022-03-01". Defaults to None.
            
            if start and end are None, the default is crawl all the weibo of the user.

        Returns:
            pd.DataFrame: the weibo info of the user

        Yields:
            Iterator[pd.DataFrame]: the weibo info of the user, None if there is no weibo
        """
        page = 1
        weibo_info = ["user_id", "id", "微博正文", "发布时间", "点赞数", "评论数", "转发数"]
        while True:
            # get weibo by uid and page
            try:
                url = "https://weibo.com/ajax/statuses/mymblog?uid={}&page={}&feature=0".format(uid, page)
                response = self.client.get(url, headers=headers).json()
            except Exception as e:
                raise e

            if not response["data"]["list"]:
                weibo_df = pd.DataFrame(columns=weibo_info)
                yield None
            else:
                weibo_df = pd.DataFrame(columns=weibo_info)
                for item in response["data"]["list"]:
                    weibo = {
                        "user_id": item["user"]["id"],
                        "id": item["id"],
                        "微博正文": item["text_raw"],
                        "发布时间": item["created_at"],
                        "点赞数": item["attitudes_count"],
                        "评论数": item["comments_count"],
                        "转发数": item["reposts_count"],
                    }
                    if self.check_time(start, end, weibo) and self.check_keyword_list(keyword_list, weibo):
                        weibo_df = weibo_df.append(weibo, ignore_index=True)
                page += 1
                yield weibo_df
