# -*- coding: utf-8 -*-
"""
Created on Fri May  5 19:16:32 2023

@author: tkfy9
"""
import re
import pandas as pd
import requests
import random
import os
import json
import execjs
import datetime as dt

WENCAI_LOGIN_URL = {
    "scrape_transaction": 'http://www.iwencai.com/traceback/strategy/transaction',
    "scrape_report": 'http://www.iwencai.com/traceback/strategy/submit',
    'strategy': 'http://www.iwencai.com/traceback/strategy/submit',
    "search": "http://www.iwencai.com/data-robot/extract-new",
    'recommend_strategy': 'http://www.iwencai.com/traceback/list/get-strategy',
    'backtest': 'http://backtest.10jqka.com.cn/backtest/app.html#/backtest',
    'lastjs':'http://x.10jqka.com.cn/stockpick?tid=stockpick&ts=1&qs=pc_~soniu~stock~stock~znxg~topbar&allow_redirect=false&zhineng=opened'
}

WENCAI_CRAWLER_URL = {
    'history_detail': 'http://backtest.10jqka.com.cn/backtestonce/historydetail?sort_by=desc&id={backtest_id}&start_date={start_date}&end_date={end_date}&period={period}',
    "backtest": "http://backtest.10jqka.com.cn/backtestonce/backtest",
    "yieldbacktest": "http://backtest.10jqka.com.cn/tradebacktest/yieldbacktest",
    "history_pick": 'http://backtest.10jqka.com.cn/tradebacktest/historypick?query={query}&hold_num={hold_num}&trade_date={trade_date}',
    'eventbacktest': 'http://backtest.10jqka.com.cn/eventbacktest/backtest',
    'lastjs':'http://d.10jqka.com.cn/v2/time/{}/last.js',
    'search': 'http://x.10jqka.com.cn/unifiedwap/unified-wap/v2/result/get-robot-data'

}

WENCAI_HEADERS = {
    'backtest': {
        'Host': "backtest.10jqka.com.cn",
        'Origin': "http://backtest.10jqka.com.cn",
        "Referer": "http://backtest.10jqka.com.cn/backtest/app.html",
    },
    'lastjs':{
        'Host': "d.10jqka.com.cn",
        # 'Origin': "http://backtest.10jqka.com.cn",
        "Referer": "http://x.10jqka.com.cn/",
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
    }

}


class WencaiCookie:

    def __init__(self):
        self.json_path = os.path.dirname(__file__) + '/cookies.json'

    def getHeXinVByHttp(self):
        with open(os.path.dirname(os.path.dirname(__file__)) + '\\data\\hexin.js', 'r') as f:
            jscontent = f.read()
        context = execjs.compile(jscontent)
        return context.call("v")

    def setHexinByJson(self, source, cookies=None):
        if cookies is None: cookies = dict()
        henxin_v = self.getHeXinVByHttp()
        cookies[source] = henxin_v
        cookies['expire_time'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.json_path, 'w') as f:
            json.dump(cookies, f)
        return henxin_v

    def is_expire(self, expire_time, days=3):
        delta = dt.datetime.today() - dt.datetime.strptime(expire_time, '%Y-%m-%d %H:%M:%S')
        if delta.days >= days:
            return True
        else:
            return False

    def getHexinVByJson(self, source):
        json_path = os.path.dirname(__file__) + '/cookies.json'
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                cookies = json.load(f)

            if source in cookies:
                if not self.is_expire(cookies['expire_time']):
                    return cookies[source]
                else:
                    return self.setHexinByJson(source=source, cookies=cookies)
            else:
                return self.setHexinByJson(source=source, cookies=cookies)
        else:
            self.setHexinByJson(source=source)


class Session(requests.Session):
    headers = {
        "Accept": "application/json,text/javascript,*/*;q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.8",
        'Connection': 'keep-alive',
        'Content-Type': "application/x-www-form-urlencoded; charset=UTF-8",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
        'X-Requested-With': "XMLHttpRequest"

    }

    def __init__(self, proxies=None, verify=False):
        requests.Session.__init__(self)
        self.headers.update(Session.headers)
        if proxies is not None:
            if not isinstance(proxies, (list, dict)):
                raise TypeError('proxies should be list or dict')
            if isinstance(proxies, list):
                proxies = random.choice(proxies)
        self.proxies = proxies
        self.verify = verify

    def update_headers(self, source, add_headers, force_cookies=False):
        if force_cookies:
            self.headers['hexin-v'] = WencaiCookie().getHeXinVByHttp()
        else:
            self.headers['hexin-v'] = WencaiCookie().getHexinVByJson(source=source)
        if add_headers is not None:
            if not isinstance(add_headers, dict):
                raise TypeError('update_headers should be `dict` type.')
            for k, v in add_headers.items():
                self.headers[k] = v

    def get_result(self, url, source=None, force_cookies=False, add_headers=None, **kwargs):
        self.update_headers(add_headers=add_headers, source=source, force_cookies=force_cookies)
        if self.proxies is None:
            return super(Session, self).get(url=url, **kwargs)
        else:
            return super(Session, self).get(url=url, proxies=self.proxies, verify=self.verify, **kwargs)

    def post_result(self, url, source=None, data=None, json=None, add_headers=None, force_cookies=False, **kwargs):
        self.update_headers(add_headers=add_headers, source=source, force_cookies=force_cookies)
        if self.proxies is None:
            return super(Session, self).post(url=url, data=data, json=json, **kwargs)
        else:
            return super(Session, self).post(url=url, data=data, json=json, proxies=self.proxies, verify=self.verify,
                                             **kwargs)

cookies = WencaiCookie()
session = Session(proxies=None, verify=False)
session.headers.update({'Host':'www.iwencai.com'})

def wencai(question):
    '''
    question:输入你要问的条件，不同条件使用“，”或“；”或空格
    如'均线多头排列'
    
    '''
    payload = {
            "question": question,
            "page": 100,
            "perpage": 100,
            "log_info": '{"input_type": "typewrite"}',
            "source": "Ths_iwencai_Xuangu",
            "version": 2.0,
            "secondary_intent": "",
            "query_area": "",
            "block_list": "",
            "add_info": '{"urp": {"scene": 1, "company": 1, "business": 1}, "contentType": "json", "searchInfo": true}'
        }

    r = session.post_result(url=WENCAI_CRAWLER_URL['search'],
                                     data=payload, force_cookies=True)
    try:
        result = r.json()['data']['answer'][0]['txt'][0]['content']['components'][0]['data']['datas']
    except:
        result={}
        print('没有你要的结果')

    def _re_str(x: str):
        _re = re.findall('(.*):前复权', x)
        if len(_re) >= 1:
            x = _re[-1]
        check_date = re.search(r"(\d{4}\d{1,2}\d{1,2})",x)
        if check_date is not None:
            return x.replace('[{}]'.format(check_date.group()), '')
        else:
            return x

    data = pd.DataFrame().from_dict(result)
    if not data.empty:
        columns = {i: _re_str(i) for i in data.columns}
        data = data.rename(columns=columns)
        for col in ['market_code', 'code', '关键词资讯', '涨跌幅']:
            if col in data.columns:
                del data[col]
    return data