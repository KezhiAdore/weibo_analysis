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

    def search_weibo(self, keyword, limit=100, export_dir="."):
        weibo_count = 0
        page = 0
        weibo_df = pd.DataFrame()
        while True:
            time.sleep(0.1)
            page += 1
            search_result = self._get_search_result(keyword, page)
            items = search_result.get('data').get('cards')
            if not len(items):
                break
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
                    if item.get('isLongText') is False:  # 不是长文本
                        data = {
                            'wid': item.get('id'),
                            'user_name': item.get('user').get('screen_name'),
                            'user_id': item.get('user').get('id'),
                            'gender': item.get('user').get('gender'),
                            'publish_time': item.get('created_at'),
                            'text': item.get("text"),  # 仅提取内容中的文本
                            'like_count': item.get('attitudes_count'),  # 点赞数
                            'comment_count': item.get('comments_count'),  # 评论数
                            'forward_count': item.get('reposts_count'),  # 转发数
                        }
                    else:  # 长文本涉及文本的展开
                        long_text = self._get_long_text(item.get('id'))  # 调用函数
                        data = {
                            'wid': item.get('id'),
                            'user_name': item.get('user').get('screen_name'),
                            'user_id': item.get('user').get('id'),
                            'gender': item.get('user').get('gender'),
                            'publish_time': item.get('created_at'),
                            'text': long_text,  # 仅提取内容中的文本
                            'like_count': item.get('attitudes_count'),
                            'comment_count': item.get('comments_count'),
                            'forward_count': item.get('reposts_count'),
                        }

                    weibo_df = weibo_df.append(data, ignore_index=True)
                    weibo_count += 1
            export_path = os.path.join(export_dir, keyword + ".xlsx")
            weibo_df.to_excel(export_path)
            if weibo_count > limit:
                break

    def _get_search_result(self, keyword, page):
        url = f"https://m.weibo.cn/api/container/getIndex"
        params = {
            "containerid": "100103type=1&q={}".format(keyword),
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

    def _parse_search_result(self, search_result):
        pass

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

    @staticmethod
    def check_time(start, end, weibo):
        start_time = datetime.datetime.strptime(start, '%Y-%m-%d')
        end_time = datetime.datetime.strptime(end, '%Y-%m-%d')

        start_time = start_time.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
        end_time = end_time.replace(tzinfo=pytz.timezone('Asia/Shanghai'))

        checked_time = datetime.datetime.strptime(weibo["发布时间"], '%a %b %d %H:%M:%S %z %Y')
        if start_time <= checked_time and checked_time <= end_time:
            return True
        else:
            if checked_time < start_time + datetime.timedelta(days=30):
                return
            return False

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
