# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 16:24:19 2022

@author: Jinyi Zhang
"""

import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from py_mini_racer import py_mini_racer
from tqdm import tqdm
from jsonpath import jsonpath

from qstock.data.util import (trans_num,get_code_id,session,request_header)

##############################################################################
##########个股资金流向

# 获取单只股票最新交易日的日内分钟级单子流入流出数据
def intraday_money(code):
    """
    获取单只股票最新交易日的日内分钟级单子流入流出数据
    code : 股票、债券代码
    """
    code_id = get_code_id(code)
    params = (
        ('lmt', '0'),
        ('klt', '1'),
        ('secid', code_id),
        ('fields1', 'f1,f2,f3,f7'),
        ('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63'),
    )
    url = 'http://push2.eastmoney.com/api/qt/stock/fflow/kline/get'
    res = session.get(url,
                      headers=request_header,
                      params=params).json()
    columns = ['时间', '主力净流入', '小单净流入', '中单净流入', '大单净流入', '超大单净流入']
    name = jsonpath(res, '$..name')[0]
    code = code_id.split('.')[-1]
    data = jsonpath(res, '$..klines[:]')
    if not data:
        columns.insert(0, '代码')
        columns.insert(0, '名称')
        return pd.DataFrame(columns=columns)
    rows = [d.split(',') for d in data]
    df = pd.DataFrame(rows, columns=columns)
    df.insert(0, '代码', code)
    df.insert(0, '名称', name)
    cols = ['代码', '名称', '时间']
    df = trans_num(df, cols)
    return df

# 个股或债券或期货历史资金流向数据
def hist_money(code):
    """
    获取单支股票、债券的历史单子流入流出数据
    code : 股票、债券代码
    """
    history_money_dict = {
        'f51': '日期',
        'f52': '主力净流入',
        'f53': '小单净流入',
        'f54': '中单净流入',
        'f55': '大单净流入',
        'f56': '超大单净流入',
        'f57': '主力净流入占比',
        'f58': '小单流入净占比',
        'f59': '中单流入净占比',
        'f60': '大单流入净占比',
        'f61': '超大单流入净占比',
        'f62': '收盘价',
        'f63': '涨跌幅'}

    fields = list(history_money_dict.keys())
    columns = list(history_money_dict.values())
    fields2 = ",".join(fields)
    code_id = get_code_id(code)
    params = (
        ('lmt', '100000'),
        ('klt', '101'),
        ('secid', code_id),
        ('fields1', 'f1,f2,f3,f7'),
        ('fields2', fields2),

    )
    url = 'http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get'
    res = session.get(url,
                      headers=request_header,
                      params=params).json()

    data = jsonpath(res, '$..klines[:]')
    if not data:
        columns.insert(0, '代码')
        columns.insert(0, '名称')
        return pd.DataFrame(columns=columns)
    rows = [d.split(',') for d in data]
    name = jsonpath(res, '$..name')[0]
    code = code_id.split('.')[-1]
    df = pd.DataFrame(rows, columns=columns)
    df.insert(0, '代码', code)
    df.insert(0, '名称', name)
    cols = ['代码', '名称', '日期']
    df = trans_num(df, cols)
    return df

# 个股n日资金流
def stock_money(code, ndays=[3, 5, 10, 20]):
    '''stock可以为股票简称或代码，如晓程科技或300139
       ndays为时间周期，如3日、5日、10日等
    '''
    # 获取个股资金流向数据
    df = hist_money(code)
    df.index = pd.to_datetime(df['日期'])
    df = df.sort_index().dropna()

    if isinstance(ndays, int):
        ndays = [ndays]
    for n in ndays:
        df[str(n) + '日累计'] = df['主力净流入'].rolling(n).sum()
    cols = ['主力净流入'] + [(str(n) + '日累计') for n in ndays]

    # 单位转为万元
    new_cols = [str(i) + '日主力净流入' for i in [1] + ndays]
    result = (df[cols] / 10000).dropna()
    result = result.rename(columns=dict(zip(cols, new_cols)))
    return result

##############################################################################
##########北向资金流向
def north_money(flag=None,n=1):
    '''flag=None，默认返回北上资金总体每日净流入数据
    flag='行业',代表北向资金增持行业板块排行
    flag='概念',代表北向资金增持概念板块排行
    flag='个股',代表北向资金增持个股情况
    n:  代表n日排名，n可选1、3、5、10、‘M’，‘Q','Y'
    即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
    '''
    if flag=='个股':
        return north_money_stock(n)
        
    elif flag in ['行业','概念','地域']:
        return north_money_sector(flag, n)
    elif flag in ["沪股通", "深股通"]:
        return north_money_flow(flag)
    else:
        dd=north_money_flow("北上")
        dd.index=pd.to_datetime(dd.date)
        dd=dd['净流入(亿)']
        return dd



def north_money_flow(flag= "北上"):
    """
    获取东方财富网沪深港通持股-北向资金净流入
    http://data.eastmoney.com/hsgtcg/
    flag: {"沪股通", "深股通", "北上"}
    """
    url = "http://push2his.eastmoney.com/api/qt/kamt.kline/get"
    params = {
        "fields1": "f1,f3,f5",
        "fields2": "f51,f52",
        "klt": "101",
        "lmt": "5000",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "cb": "jQuery18305732402561585701_1584961751919",
        "_": "1584962164273",
    }
    r = requests.get(url, params=params)
    data_text = r.text
    data_json = json.loads(data_text[data_text.find("{") : -2])
    flag_dict={"沪股通":"hk2sh","深股通":"hk2sz","北上":"s2n"}
    fd=flag_dict[flag]
    temp_df = (
            pd.DataFrame(data_json["data"][fd])
            .iloc[:, 0]
            .str.split(",", expand=True))
    
    temp_df.columns = ["date", "净流入(亿)"]
    temp_df["净流入(亿)"] = (pd.to_numeric(temp_df["净流入(亿)"])/10000).round(3)
    return temp_df


def north_money_stock(n=1):
    """
    获取东方财富北向资金增减持个股情况
    http://data.eastmoney.com/hsgtcg/list.html
    n:  代表n日排名，n可选1、3、5、10、‘M’，‘Q','Y'
    即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
    """
    url = "http://data.eastmoney.com/hsgtcg/list.html"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    date = (
        soup.find("div", attrs={"class": "title"})
        .find("span")
        .text.strip("（")
        .strip("）"))
    url = "http://datacenter-web.eastmoney.com/api/data/v1/get"
    
    _type=str(n).upper()
    type_dict={'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
    period=type_dict[_type]
    filter_str = (f"""(TRADE_DATE='{date}')(INTERVAL_TYPE="{_type}")""")
    params = {
        "sortColumns": "ADD_MARKET_CAP",
        "sortTypes": "-1",
        "pageSize": "50000",
        "pageNumber": "1",
        "reportName": "RPT_MUTUAL_STOCK_NORTHSTA",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    df.columns = [
        "序号","_","_","日期","_","名称","_","_","代码","_", "_","_","_",
        "持股数","持股市值","持股占流通股比","持股占总股本比",
        "收盘","涨幅","_","所属板块","_","_","_","_","_","_","_", "_",
        "_","_", f'{period}增持市值',f'{period}增持股数',f'{period}增持市值增幅',
        f'{period}增持占流通股比',f'{period}增持占总股本比',
        "_","_","_","_","_","_","_","_"]
    df = df[
        ["代码","名称","收盘","涨幅", "持股数","持股市值","持股占流通股比",
         "持股占总股本比",f'{period}增持股数',f'{period}增持市值',
         f'{period}增持市值增幅',f'{period}增持占流通股比',f'{period}增持占总股本比',
        "所属板块", "日期",] ]
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    ignore_cols = ["代码","名称","所属板块", "日期",]
    df = trans_num(df, ignore_cols)
    return df

def north_money_sector(flag = "行业", n=1):
    """
    东方财富网北向资金增持行业板块排行
    http://data.eastmoney.com/hsgtcg/hy.html
    flag:可选："行业","概念","地域"
        {"行业": "5","概念": "4","地域": "3",}
    n:  代表n日排名，n可选1、3、5、10、‘M’，‘Q','Y'
    即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
    """
    _type=str(n).upper()
   
    url = "https://data.eastmoney.com/hsgtcg/hy.html"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    current_date = (
        soup.find(attrs={"id": "bkph_date"}).text.strip("（").strip("）")
    )
    flag_dict = {"行业": "5","概念": "4","地域": "3",}
    
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "ADD_MARKET_CAP",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_MUTUAL_BOARD_HOLDRANK_WEB",
        "columns": "ALL",
        "quoteColumns": "f3~05~SECURITY_CODE~INDEX_CHANGE_RATIO",
        "source": "WEB",
        "client": "WEB",
        "filter": f"""(BOARD_TYPE="{flag_dict[flag]}")(TRADE_DATE='{current_date}')(INTERVAL_TYPE="{_type}")""",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    df = pd.DataFrame(data_json["result"]["data"])
    df.reset_index(inplace=True)
    df["index"] = df.index + 1
    df.columns = ["序号","_","_","名称","_","涨跌幅","日期","_",
        "_","增持股票只数","持有股票只数","增持市值","增持市值增幅",
        "-","增持占板块比","增持占北向资金比","持股市值","持股占北向资金比","持股占板块比",
        "_","_","增持最大股市值","_","_","减持最大股市值","增持最大股占总市值比",
        "_","_","减持最大股占总市值比","_","_","_","_","_","_", "_",]
    df = df[["日期","名称","涨跌幅","增持股票只数","持有股票只数","增持市值","增持市值增幅",
        "增持占板块比","增持占北向资金比","持股市值","持股占北向资金比","持股占板块比",
        "增持最大股市值","减持最大股市值","增持最大股占总市值比","减持最大股占总市值比",]]
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    
    return df

###############################################################################
########同花顺资金流
def ths_money(flag=None,n=None):
    '''
    获取同花顺个股、行业、概念资金流数据
    flag:'个股','概念','行业'
    n=1,3,5,10,20分别表示n日排行
    '''
    if flag=='行业' or flag=='行业板块':
        return ths_industry_money(n)
    
    elif flag=='概念' or flag=='概念板块':
        return ths_concept_money(n)
    
    else:
        return ths_stock_money(n=None)

def ths_header():
    file= Path(__file__).parent/"ths.js"
    with open(file) as f:
        js_data = f.read()
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(js_data)
    v_code = js_code.call("v")
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "hexin-v": v_code,
        "Host": "data.10jqka.com.cn",
        "Pragma": "no-cache",
        "Referer": "http://data.10jqka.com.cn/funds/hyzjl/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    return headers

def ths_stock_money(n=None):
    """
    获取同花顺个股资金流向
    http://data.10jqka.com.cn/funds/ggzjl/#refCountId=data_55f13c2c_254
    n: None、3、5、10、20分别代表 “即时”, "3日排行", "5日排行", "10日排行", "20日排行"
    """
    url = "http://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/ajax/1/free/1/"
    res = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(res.text, "lxml")
    raw_page = soup.find("span", attrs={"class": "page_info"}).text
    page_num = raw_page.split("/")[1]
        
    df = pd.DataFrame()
    for page in tqdm(range(1, int(page_num) + 1),leave=False):
        if n is None:
            url = f"http://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/page/{page}/ajax/1/free/1/"
        else:
            url = f"http://data.10jqka.com.cn/funds/ggzjl/board/{n}/field/zdf/order/desc/page/{page}/ajax/1/free/1/"
        r = requests.get(url, headers=ths_header())
        temp_df = pd.read_html(r.text)[0]
        df = pd.concat([df, temp_df], ignore_index=True)

    del df["序号"]
    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    if n is None:
        df.columns = ["序号","代码","简称","最新价", "涨幅","换手率",
            '流入资金','流出资金','净额(万)','成交额',]
    else:
        df.columns = ["序号","代码","简称","最新价","涨幅",
            "换手率","净额(万)",]
    cols=["代码","简称","最新价", "涨幅","换手率",'净额(万)']    
    df=df[cols]
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df['净额(万)']=df['净额(万)'].apply(lambda s:float(
                s.strip('亿'))*10000 if s.endswith('亿') 
                    else s.strip('万'))
    df[['涨幅','换手率']]=df[['涨幅','换手率']].apply(lambda s:s.str.strip('%'))
    
    ignore_cols = ["代码","简称"]
    df = trans_num(df, ignore_cols)
    return df

def ths_concept_money(n=None) :
    """
    获取同花顺概念板块资金流
    http://data.10jqka.com.cn/funds/gnzjl/#refCountId=data_55f13c2c_254
    n: None、3、5、10、20分别代表 “即时”, "3日排行", "5日排行", "10日排行", "20日排行"
    """
    url = "http://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/ajax/1/free/1/"
    res = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(res.text, "lxml")
    raw_page = soup.find("span", attrs={"class": "page_info"}).text
    page_num = raw_page.split("/")[1]
        
    df = pd.DataFrame()
    for page in tqdm(range(1, int(page_num) + 1),leave=False):
        if n is None:
            url = f"http://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/page/{page}/ajax/1/free/1/"
        else:
            url = f"http://data.10jqka.com.cn/funds/gnzjl/board/{n}/field/tradezdf/order/desc/page/{page}/ajax/1/free/1/"
        r = requests.get(url, headers=ths_header())
        temp_df = pd.read_html(r.text)[0]
        df = pd.concat([df, temp_df], ignore_index=True)
    del df["序号"]
    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    
    if n is None:
        df.columns = ["序号","概念名称","概念指数","涨幅","流入资金","流出资金",
            "净额(亿)","公司家数","领涨股","领涨股涨幅","当前价",]
        df["涨幅"] = df["涨幅"].str.strip("%")
        df["领涨股涨幅"] = df["领涨股涨幅"].str.strip("%")
        df["涨幅"] = pd.to_numeric(df["涨幅"], errors="coerce")
        df["领涨股涨幅"] = pd.to_numeric(df["领涨股涨幅"], errors="coerce")
        cols=["概念名称","公司家数","概念指数","涨幅","净额(亿)","领涨股","领涨股涨幅","当前价"]
        return df[cols]
    else:
        df.columns = ["序号","概念名称","公司家数","概念指数","涨幅","流入资金","流出资金","净额(亿)",]
        cols=["概念名称","公司家数","概念指数","涨幅","净额(亿)"]
        return df[cols]

def ths_industry_money(n = None) :
    """
    获取同花顺行业资金流
    http://data.10jqka.com.cn/funds/hyzjl/#refCountId=data_55f13c2c_254
    n: None、3、5、10、20分别代表 “即时”, "3日排行", "5日排行", "10日排行", "20日排行
    """
    url = "http://data.10jqka.com.cn/funds/hyzjl/field/tradezdf/order/desc/ajax/1/free/1/"
    res = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(res.text, "lxml")
    raw_page = soup.find("span", attrs={"class": "page_info"}).text
    page_num = raw_page.split("/")[1]
        
    df = pd.DataFrame()
    for page in tqdm(range(1, int(page_num) + 1),leave=False):
        if n is None:
            url = f"http://data.10jqka.com.cn/funds/hyzjl/field/tradezdf/order/desc/page/{page}/ajax/1/free/1/"
        else:
            url=f"http://data.10jqka.com.cn/funds/hyzjl/board/{n}/field/tradezdf/order/desc/page/{page}/ajax/1/free/1/"
        r = requests.get(url, headers=ths_header())
        temp_df = pd.read_html(r.text)[0]
        df = pd.concat([df, temp_df], ignore_index=True)

    del df["序号"]
    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    
    if n is None:
        df.columns = ["序号","行业名称","行业指数","涨幅","流入资金", "流出资金",
            "净额(亿)","公司家数","领涨股","领涨股涨幅","当前价"]
        df["涨幅"] = df["涨幅"].str.strip("%")
        df["领涨股涨幅"] = df["领涨股涨幅"].str.strip("%")
        df["涨幅"] = pd.to_numeric(df["涨幅"], errors="coerce")
        df["领涨股涨幅"] = pd.to_numeric(df["领涨股涨幅"], errors="coerce")
        cols= ["行业名称","公司家数","行业指数","涨幅","净额(亿)","领涨股","领涨股涨幅","当前价"]
        return df[cols]
    else:
        df.columns = ["序号","行业名称","公司家数","行业指数","涨幅","流入资金","流出资金","净额(亿)"]
        cols=["行业名称","公司家数","行业指数","涨幅","净额(亿)"]
        return df[cols]




