# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 22:05:59 2022

@author: Jinyi Zhang
"""
import re
import pandas as pd
import json
import requests
import time
import signal
import hashlib
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

import multitasking
from func_timeout import func_set_timeout

signal.signal(signal.SIGINT, multitasking.killall)


######新闻资讯数据
def news_data(news_type=None, start=None, end=None, code=None):
    """news_type:新闻类型：cctv'或'新闻联播'
    'js'或'金十数据'；'stock' 或'个股新闻'
    不输入参数，默认输出财联社电报新闻数据
    start:起始日期，如'20220930',不输入默认当前最新日期
    end:结束日期，如'20221001'，不输入默认当前最新日期
    stock：个股代码

    """

    if news_type == "cctv" or news_type == "新闻联播":
        return news_cctv(start, end)
    elif news_type == "js" or news_type == "金十数据":
        return news_js(start, end)

    elif code is not None or news_type in ["stock", "个股新闻", "个股"]:
        return stock_news(code)
    else:
        return news_cls()


def news_cls():
    """
    财联社-电报
    https://www.cls.cn/telegraph
    :return: 财联社-电报
    :rtype: pandas.DataFrame
    """

    current_time = int(time.time())
    url = "https://www.cls.cn/nodeapi/telegraphList"
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
    }
    text = requests.get(url, params=params).url.split("?")[1]
    if not isinstance(text, bytes):
        text = bytes(text, "utf-8")
    sha1 = hashlib.sha1(text).hexdigest()
    code = hashlib.md5(sha1.encode()).hexdigest()

    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
        "sign": code,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=utf-8",
        "Host": "www.cls.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.cls.cn/telegraph",
        "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    }
    data = requests.get(url, headers=headers, params=params).json()
    df = pd.DataFrame(data["data"]["roll_data"])
    df = df[["title", "content", "ctime"]]
    df["ctime"] = pd.to_datetime(df["ctime"], unit="s", utc=True).dt.tz_convert(
        "Asia/Shanghai"
    )
    df.columns = ["标题", "内容", "发布时间"]
    df.sort_values(["发布时间"], inplace=True)
    df.reset_index(inplace=True, drop=True)
    df["发布日期"] = df["发布时间"].dt.date
    df["发布时间"] = df["发布时间"].dt.time
    return df


def get_news_cctv(date=None):
    """
    新闻联播文字稿
    https://tv.cctv.com/lm/xwlb/?spm=C52056131267.P4y8I53JvSWE.0.0
    date: 需要获取数据的日期
    """

    now = datetime.now()
    now_date = now.strftime("%Y%m%d")
    if date is None:
        date = now_date
    else:
        date = "".join(date.split("-"))
    if date >= now_date:
        date = (now - timedelta(1)).strftime("%Y%m%d")

    url = f"http://cctv.cntv.cn/lm/xinwenlianbo/{date}.shtml"
    res = requests.get(url)
    title_list = []
    content_list = []
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Cookie": "cna=DLYSGBDthG4CAbRVCNxSxGT6",
        "Host": "tv.cctv.com",
        "Pragma": "no-cache",
        "Proxy-Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    }
    if int(date) <= int("20130708"):
        res.encoding = "gbk"
        raw_list = re.findall(r"title_array_01\((.*)", res.text)
        page_url = [
            re.findall("(http.*)", item)[0].split("'")[0] for item in raw_list[1:]
        ]

    elif int(date) < int("20160203"):
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "lxml")
        page_url = [
            item.find("a")["href"]
            for item in soup.find(
                "div", attrs={"id": "contentELMT1368521805488378"}
            ).find_all("li")[1:]
        ]

    else:
        url = f"https://tv.cctv.com/lm/xwlb/day/{date}.shtml"
        res = requests.get(url)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "lxml")
        page_url = [item.find("a")["href"] for item in soup.find_all("li")[1:]]

    for page in page_url:
        try:
            r = requests.get(page, headers=headers)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "lxml")
            if soup.find("h3"):
                title = soup.find("h3").text
            else:
                title = soup.find("div", attrs={"class": "tit"}).text
            if soup.find("div", attrs={"class": "cnt_bd"}):
                content = soup.find("div", attrs={"class": "cnt_bd"}).text
            else:
                content = soup.find("div", attrs={"class": "content_area"}).text
            title_list.append(title.strip("[视频]").strip().replace("\n", " "))
            content_list.append(
                content.strip()
                .strip("央视网消息(新闻联播)：")
                .strip("央视网消息（新闻联播）：")
                .strip("(新闻联播)：")
                .strip()
                .replace("\n", " ")
            )
        except:
            continue
    df = pd.DataFrame(
        [[date] * len(title_list), title_list, content_list],
        index=["date", "title", "content"],
    ).T
    return df


def get_dates(start=None, end=None):
    if start is None:
        start = (datetime.now()).strftime("%Y%m%d")
    if end is None:
        end = (datetime.now()).strftime("%Y%m%d")
    if start > end:
        start = end
    dates = pd.date_range(start, end)
    dates = [s.strftime("%Y%m%d") for s in dates]
    return dates


def news_cctv(start=None, end=None):
    """获取某日期期间的所有新闻联播数据
    start:起始日期，如'20220930'
    end:结束日期，如'20221001'
    """
    dates = get_dates(start, end)
    data_list = []

    @multitasking.task
    @func_set_timeout(5)
    def run(date):
        data = get_news_cctv(date)
        data_list.append(data)

    for date in dates:
        try:
            run(date)
        except:
            continue
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=0, ignore_index=True)
    return df


def news_js(start, end):
    """获取某日期期间的所有金十数据-市场快讯数据
    start:起始日期，如'20220930'
    end:结束日期，如'20221001'
    """
    dates = get_dates(start, end)
    data_list = []

    @multitasking.task
    @func_set_timeout(5)
    def run(date):
        data = get_js_news(date)
        data_list.append(data)

    for date in dates:
        run(date)
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=0, ignore_index=True)
    return df


def get_js_news(date=None):
    """
    金十数据-市场快讯
    https://www.jin10.com/
    date: 日期
    """
    if date is None:
        date = (datetime.now() + timedelta(1)).strftime("%Y%m%d")
    else:
        date = "".join(date.split("-"))
        date = datetime.strptime(date, "%Y%m%d") + timedelta(1)
        date = date.strftime("%Y%m%d")

    url = "https://flash-api.jin10.com/get_flash_list"
    params = {
        "channel": "-8200",
        "vip": "1",
        "t": "1625623640730",
        "max_time": date,
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "handleerror": "true",
        "origin": "https://www.jin10.com",
        "pragma": "no-cache",
        "referer": "https://www.jin10.com/",
        "sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "x-app-id": "bVBF4FyRTn5NJF5n",
        "x-version": "1.0.0",
    }
    data = requests.get(url, params=params, headers=headers).json()
    temp_df = pd.DataFrame(data["data"])
    temp_list = []
    for item in temp_df["data"]:
        if "content" in item.keys():
            temp_list.append(item["content"])
        elif "pic" in item.keys():
            temp_list.append(item["pic"])
        else:
            temp_list.append("-")
    df = pd.DataFrame([temp_df["time"].to_list(), temp_list]).T
    df.columns = ["datetime", "content"]
    df.sort_values(["datetime"], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df


def stock_news(stock):
    """
    东方财富-个股新闻
    stock: 股票代码
    """
    url = "https://search-api-web.eastmoney.com/search/jsonp"

    # 动态生成 callback 参数
    timestamp = int(time.time() * 1000)
    cb = f"jQuery3510875346244069884_{timestamp}"

    # 构造 param 参数
    search_scope = "default"
    sort = "default"
    page_index = 1
    page_size = 100
    pre_tag = "<em>"
    post_tag = "</em>"

    # 新的 param 构造方式（假设 API 结构发生变化）
    new_param = {
        "uid": "",
        "keyword": stock,
        "type": ["cmsArticle"],
        "client": "web",
        "clientType": "web",
        "clientVersion": "curr",
        "param": {
            "cmsArticle": {
                "searchScope": search_scope,
                "sort": sort,
                "pageIndex": page_index,
                "pageSize": page_size,
                "preTag": pre_tag,
                "postTag": post_tag,
            }
        },
    }

    params = {
        "cb": cb,
        "param": json.dumps(new_param),
        "_": timestamp + 1,
    }

    try:
        res = requests.get(url, params=params)
        res.raise_for_status()  # 检查 HTTP 请求是否成功

        data_text = res.text

        # 解析 JSON 数据
        data_json = json.loads(data_text.strip(f"{cb}(")[:-1])

        # 检查数据结构
        if "result" not in data_json:
            print("No result found in the response.")
            return None

        result = data_json["result"]

        # 检查 cmsArticle 是否存在（假设新键为 cmsArticle）
        if "cmsArticle" not in result:
            print("The key 'cmsArticle' is missing in the result.")
            return None

        df = pd.DataFrame(result["cmsArticle"])

        # 清洗标题和内容中的 HTML 标签
        df["title"] = df["title"].apply(lambda s: re.sub(r"[<em></em>]", "", s))
        df["content"] = df["content"].apply(lambda s: re.sub(r"[<em></em>]", "", s))

        df["code"] = stock
        df = df[["date", "code", "title", "content", "url"]]

        return df

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except json.JSONDecodeError:
        print("Failed to decode JSON response.")
        return None
    except KeyError as e:
        print(f"Missing key in JSON response: {e}")
        return None
