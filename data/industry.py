# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 13:38:58 2022

@author: Jinyi Zhang
"""
import re
import requests
import pandas as pd
import signal

import random 
import time

from functools import lru_cache
from tqdm import tqdm
from func_timeout import func_set_timeout
import multitasking
from bs4 import BeautifulSoup
from datetime import datetime
from functools import lru_cache
from qstock.data.util import ths_code_name,trans_num
from qstock.data.trade import latest_trade_date
from qstock.data import demjson
from qstock.stock.ths_em_pool import ths_header
signal.signal(signal.SIGINT, multitasking.killall)


#同花顺概念板块(简称ths)
#获取同花顺概念板块名称
def ths_index_name(flag='概念'):
    '''
    获取同花顺概念或行业板块名称
    flag='概念板块' or '行业板块'
    '''
    if flag=='概念板块' or flag=='概念':
        return ths_concept_name()
    else:
        names=list(ths_code_name.values())
    return names

#概念板块成分股
def ths_index_member(code=None):
    '''
    获取同花顺概念或行业板块成份股
    code:输入板块行业或概念代码或简称
    '''
    code_list=list(ths_code_name)+list(ths_code_name.values())
    if code in code_list:
        return ths_industry_member(code)
    else:
        return ths_concept_member(code)


#多线程获取同花顺指数数据
def ths_index_price(flag='概念'):
    df_list=[]
    codes=ths_index_name(flag)
    pbar = tqdm(total=len(codes),leave=False)
    
    #@multitasking.task
    #@func_set_timeout(5)
    def run(code):
        try: 
            temp=ths_index_data(code)
            temp[code]=temp.close
            df_list.append(temp[code])
            pbar.update()
        except:
            pass
    
    
    for code in codes:
        try:
            run(code)
        except:
            continue
    #multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(df_list, axis=1)
    return df



#概念指数行情数据
def ths_index_data(code=None):
    '''
    获取同花顺概念或行业板块指数行情数据
    code:输入板块行业或概念代码或简称
    '''
    code_list=list(ths_code_name)+list(ths_code_name.values())
    if code in code_list:
        return ths_industry_data(code)
    else:
        return ths_concept_data(code)

def ths_name_code(code):
    """
    获取同花顺行业对应代码
    """
    if code.isdigit():
        return code
    name_code={value:key for key, value in ths_code_name.items()}
    code=name_code[code]
    return code

#修正函数，但只能获取第一页的数据，无法获取翻页的
def ths_industry_member(code= "半导体"):
    """
    同花顺-板块-行业板块
    http://q.10jqka.com.cn/gn/detail/code/881270/
   
    """
    if code.isdigit():
        symbol=code
    else:
        symbol=ths_name_code(code)
    # 定义要请求的网页 URL 
    url = f"http://q.10jqka.com.cn/thshy/detail/code/{symbol}/"
    
    # 设置请求头，模拟浏览器访问 
    try: 
       # 发送 HTTP 请求获取网页内容 
       response = requests.get(url,  headers=ths_header()) 
       # 检查响应状态码 
       response.raise_for_status()  
 
       # 设置响应内容的编码 
       response.encoding  = response.apparent_encoding  
       # 获取网页源代码 
       html = response.text  
 
       # 使用 BeautifulSoup 解析 HTML 
       soup = BeautifulSoup(html, 'html.parser')  
 
       # 查找表格元素，这里假设表格的 class 为 'm-table'，你可以根据实际情况修改 
       table = soup.find('table',  class_='m-table') 
 
       if table: 
           # 使用 pandas 读取表格数据 
           df = pd.read_html(str(table))[0] 
           return df           
       else: 
           print('未找到表格元素') 
    except requests.RequestException as e: 
       print(f'请求出错: {e}') 
    except Exception as e: 
       print(f'发生其他错误: {e}') 

def ths_industry_data(code= "半导体及元件",start= "20200101",end= None) :
    """
    获取同花顺行业板块指数数据
    http://q.10jqka.com.cn/gn/detail/code/301558/
    start: 开始时间
    end: 结束时间
    """
    if end is None:
        end=latest_trade_date()
    if code.isdigit():
        symbol=code
    else:
        symbol=ths_name_code(code)
    df = pd.DataFrame()
    current_year = datetime.now().year
    for year in range(2000, current_year + 1):
        url = f"http://d.10jqka.com.cn/v4/line/bk_{symbol}/01/{year}.js"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
            "Referer": "http://q.10jqka.com.cn",
            "Host": "d.10jqka.com.cn",
        }
        res = requests.get(url, headers=headers)
        data_text = res.text
        try:
            demjson.decode(data_text[data_text.find("{") : -1])
        except:
            continue
        temp_df = demjson.decode(data_text[data_text.find("{") : -1])
        temp_df = pd.DataFrame(temp_df["data"].split(";"))
        temp_df = temp_df.iloc[:, 0].str.split(",", expand=True)
        df = pd.concat([df, temp_df], ignore_index=True)

    if len(df.columns) == 11:
        df.columns = ["日期","开盘价","最高价","最低价","收盘价","成交量",
            "成交额","_","_","_","_",]
    else:
        df.columns = ["日期","开盘价","最高价","最低价","收盘价","成交量",
            "成交额","_", "_","_","_","_",]
    
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    c1 = pd.to_datetime(start) < df["日期"]
    c2 = pd.to_datetime(end) > df["日期"]
    df = df[c1 & c2]
    cols1 = ["日期","开盘价","最高价","最低价","收盘价","成交量"]
    cols2 = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = df.rename(columns=dict(zip(cols1,cols2)))[cols2]
    df.set_index('date',inplace=True)
    ignore_cols = ['date']
    df = trans_num(df, ignore_cols)
    return df

@lru_cache()
def ths_concept_name_code():
    """
    同花顺概念板块概念名称
    http://q.10jqka.com.cn/gn/detail/code/301558/
    """
    page=1
    url = f"http://q.10jqka.com.cn/gn/index/field/addtime/order/desc/page/{page}/ajax/1/"
    
    res = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(res.text, "lxml")
    total_page = soup.find("span", attrs={"class": "page_info"}).text.split("/")[1]
    df = pd.DataFrame()
    for page in tqdm(range(1, int(total_page) + 1), leave=False):
        r = requests.get(url, headers=ths_header())
        soup = BeautifulSoup(r.text, "lxml")
        url_list = []
        for item in (
            soup.find("table", attrs={"class": "m-table m-pager-table"})
            .find("tbody")
            .find_all("tr")):
            inner_url = item.find_all("td")[1].find("a")["href"]
            url_list.append(inner_url)
        temp_df = pd.read_html(r.text)[0]
        temp_df["网址"] = url_list
        df = pd.concat([df, temp_df], ignore_index=True)
    df = df[["日期", "概念名称", "成分股数量", "网址"]]
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    df["成分股数量"] = pd.to_numeric(df["成分股数量"])
    df["代码"] = df["网址"].str.split("/", expand=True).iloc[:, 6]
    df.drop_duplicates(keep="last", inplace=True)
    df.reset_index(inplace=True, drop=True)

    # 处理遗漏的板块
    url = "http://q.10jqka.com.cn/gn/detail/code/301558/"
    r = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(r.text, "lxml")
    need_list = [
        item.find_all("a")
        for item in soup.find_all(attrs={"class": "cate_group"})
    ]
    temp_list = []
    for item in need_list:
        temp_list.extend(item)
    temp_df = pd.DataFrame(
        [
            [item.text for item in temp_list],
            [item["href"] for item in temp_list],
        ]
    ).T
    temp_df.columns = ["概念名称", "网址"]
    temp_df["日期"] = None
    temp_df["成分股数量"] = None
    temp_df["代码"] = (
        temp_df["网址"].str.split("/", expand=True).iloc[:, 6].tolist()
    )
    temp_df = temp_df[["日期", "概念名称", "成分股数量", "网址", "代码"]]
    df = pd.concat([df, temp_df], ignore_index=True)
    df.drop_duplicates(subset=["概念名称"], keep="first", inplace=True)
    return df

def ths_concept_name():
    """
    获取同花顺概念板块-概念名称
    """
    ths_df = ths_concept_name_code()
    name_list = ths_df["概念名称"].tolist()
    return name_list

def ths_concept_code():
    """
    获取同花顺概念板块-概念代码
    """
    ths_df = ths_concept_name_code()
    name_list = ths_df["概念名称"].tolist()
    code_list = list(ths_df['代码'])
    name_code_dict = dict(zip(name_list, code_list))
    return name_code_dict

#修正函数，但只能获取第一页的数据，无法获取翻页的
def ths_concept_member(code= "阿里巴巴概念"):
    """
    同花顺-板块-概念板块
    https://q.10jqka.com.cn/gn/detail/code/301558/
   
    """
    if code.isdigit():
        symbol=code
    else:
        symbol=ths_concept_code()[code]
    # 定义要请求的网页 URL 
    url = f" https://q.10jqka.com.cn/gn/detail/code/{symbol}/"
    
    # 设置请求头，模拟浏览器访问 
    try: 
       # 发送 HTTP 请求获取网页内容 
       response = requests.get(url,  headers=ths_header()) 
       # 检查响应状态码 
       response.raise_for_status()  
 
       # 设置响应内容的编码 
       response.encoding  = response.apparent_encoding  
       # 获取网页源代码 
       html = response.text  
 
       # 使用 BeautifulSoup 解析 HTML 
       soup = BeautifulSoup(html, 'html.parser')  
 
       # 查找表格元素，这里假设表格的 class 为 'm-table'，你可以根据实际情况修改 
       table = soup.find('table',  class_='m-table') 
 
       if table: 
           # 使用 pandas 读取表格数据 
           df = pd.read_html(str(table))[0] 
           return df           
       else: 
           print('未找到表格元素') 
    except requests.RequestException as e: 
       print(f'请求出错: {e}') 
    except Exception as e: 
       print(f'发生其他错误: {e}') 

def ths_concept_data(code='白酒概念',start= "2020"):
    """
    同花顺-板块-概念板块-指数数据
    http://q.10jqka.com.cn/gn/detail/code/301558/
    start: 开始年份; e.g., 2019
    """
    code_map = ths_concept_code()
    symbol_url = f"http://q.10jqka.com.cn/gn/detail/code/{code_map[code]}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }
    r = requests.get(symbol_url, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    symbol_code = (
        soup.find("div", attrs={"class": "board-hq"}).find("span").text
    )
    df = pd.DataFrame()
    current_year = datetime.now().year
    for year in range(int(start), current_year + 1):
        url = f"http://d.10jqka.com.cn/v4/line/bk_{symbol_code}/01/{year}.js"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
            "Referer": "http://q.10jqka.com.cn",
            "Host": "d.10jqka.com.cn",
        }
        r = requests.get(url, headers=headers)
        data_text = r.text
        try:
            demjson.decode(data_text[data_text.find("{") : -1])
        except:
            continue
        temp_df = demjson.decode(data_text[data_text.find("{") : -1])
        temp_df = pd.DataFrame(temp_df["data"].split(";"))
        temp_df = temp_df.iloc[:, 0].str.split(",", expand=True)
        df = pd.concat([df, temp_df], ignore_index=True)
    if df.columns.shape[0] == 12:
        df.columns = ["日期","开盘价","最高价","最低价","收盘价","成交量",
            "成交额","_","_","_","_","_",]
    else:
        df.columns = ["日期","开盘价","最高价","最低价","收盘价", "成交量",
            "成交额","_","_","_","_",]
    df = df[["日期","开盘价","最高价","最低价","收盘价","成交量","成交额",]]
    cols1 = ["日期","开盘价","最高价","最低价","收盘价","成交量"]
    cols2 = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = df.rename(columns=dict(zip(cols1,cols2)))[cols2]
    df.set_index('date',inplace=True)
    ignore_cols = ['date']
    df = trans_num(df, ignore_cols)
    return df
    
# 辅助函数：获取板块代码及类型（行业/概念）
def __get_sector_code(name: str) -> tuple:
    """
    根据板块名称或代码获取对应的板块代码和类型（行业/概念）
    :param name: 板块名称或代码
    :return: (板块代码, 类型)
    """
    # 检查是否为板块代码
    if re.match(r"^BK\d+", name):
        # 查询行业板块
        industry_df = em_index_name('industry')
        if name in industry_df['板块代码'].values:
            return (name, 'industry')
        # 查询概念板块
        concept_df = em_index_name('concept')
        if name in concept_df['板块代码'].values:
            return (name, 'concept')
        raise ValueError(f"无效的板块代码: {name}")
    else:
        # 按名称查询
        industry_df = em_index_name('industry')
        if name in industry_df['板块名称'].values:
            code = industry_df.loc[industry_df['板块名称'] == name, '板块代码'].iloc[0]
            return (code, 'industry')
        concept_df = em_index_name('concept')
        if name in concept_df['板块名称'].values:
            code = concept_df.loc[concept_df['板块名称'] == name, '板块代码'].iloc[0]
            return (code, 'concept')
        raise ValueError(f"无效的板块名称: {name}")

###########获取东方财富网（简称em)行业概念板块数据
# 板块名称列表（带缓存）
@lru_cache()
def em_index_name(sector_type: str = 'concept') -> pd.DataFrame:
    """
    获取行业或概念板块名称列表
    :param sector_type: 板块类型，'concept'（概念）或'industry'（行业）
    :return: DataFrame
    """
    # 请求参数配置
    config = {
        'concept': {
            'url': "https://79.push2.eastmoney.com/api/qt/clist/get",
            'fs': "m:90 t:3 f:!50",
            'fields': "f2,f3,f4,f8,f12,f14,f15,f16,f17,f18,f20,f21,f24,f25,f22,f33,f11,f62,f128,f124,f107,f104,f105,f136"
        },
        'industry': {
            'url': "https://17.push2.eastmoney.com/api/qt/clist/get",
            'fs': "m:90 t:2 f:!50",
            'fields': "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222"
        }
    }
    sector_type_dict={'1':'concept','2':'industry','c':'concept','i':'industry','概念':'concept','行业':'industry',
                      '行业板块':'concept','概念板块':'industry','concept':'concept','industry':'industry'}
    sector_type=sector_type_dict[str(sector_type)]
    cfg = config.get(sector_type)
    if not cfg:
        raise ValueError("sector_type 必须为 'concept' 或 'industry'")

    params = {
        "pn": "1", "pz": "50000", "po": "1", "np": "2",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": cfg['fs'], "fields": cfg['fields'],
        "_": "1626075887768"
    }
    
    response = requests.get(cfg['url'], params=params)
    data = response.json()
    df = pd.DataFrame(data['data']['diff']).T
    df.reset_index(inplace=True)

    # 列名处理
    if sector_type == 'concept':
        df.columns = [
            "排名", "最新价", "涨跌幅", "涨跌额", "换手率", "_", "板块代码", "板块名称",
            "_", "_", "_", "_", "总市值", "_", "_", "_", "_", "_", "_",
            "上涨家数", "下跌家数", "_", "_", "领涨股票", "_", "_", "领涨股票-涨跌幅"
        ]
    else:
        df.columns = [
            "排名", "-", "最新价", "涨跌幅", "涨跌额", "-", "_", "-", "换手率", "-",
            "-", "-", "板块代码", "-", "板块名称", "-", "-", "-", "-", "总市值",
            "-", "-", "-", "-", "-", "-", "-", "-", "上涨家数", "下跌家数",
            "-", "-", "-", "领涨股票", "-", "-", "领涨股票-涨跌幅", "-", "-", "-", "-", "-"
        ]
    
    # 统一列选择及类型转换
    cols = [
        "排名", "板块名称", "板块代码", "最新价", "涨跌额", "涨跌幅",
        "总市值", "换手率", "上涨家数", "下跌家数", "领涨股票", "领涨股票-涨跌幅"
    ]
    df = df[cols]
    numeric_cols = ["最新价", "涨跌额", "涨跌幅", "总市值", "换手率", 
                   "上涨家数", "下跌家数", "领涨股票-涨跌幅"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df

# 板块成分股
def em_index_member(name: str) -> pd.DataFrame:
    """
    获取板块成分股
    :param name: 板块名称或代码
    :return: 成分股数据
    """
    code, _ = __get_sector_code(name)
    url = "https://29.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "50000", "po": "1", "np": "2",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": f"b:{code} f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        "_": "1626081702127"
    }
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.DataFrame(data['data']['diff']).T
    df.reset_index(inplace=True)
    df.columns = [
        "序号", "_", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅",
        "换手率", "市盈率-动态", "_", "_", "代码", "_", "名称", "最高", "最低",
        "今开", "昨收", "_", "_", "_", "市净率", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_"
    ]
    df = df[[
        "序号", "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
        "振幅", "最高", "最低", "今开", "昨收", "换手率", "市盈率-动态", "市净率"
    ]]
    numeric_cols = ["最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅",
                    "最高", "最低", "今开", "昨收", "换手率", "市盈率-动态", "市净率"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df

# 实时行情
def em_index_realtime(name: str) -> pd.DataFrame:
    """
    获取板块实时行情
    :param name: 板块名称或代码
    :return: 实时行情数据
    """
    code, _ = __get_sector_code(name)
    url = "https://91.push2.eastmoney.com/api/qt/stock/get"
    fields = {
        "f43": "最新价", "f44": "最高", "f45": "最低", "f46": "开盘",
        "f47": "成交量", "f48": "成交额", "f170": "涨跌幅", 
        "f171": "振幅", "f168": "换手率", "f169": "涨跌额"
    }
    params = {
        "fields": ",".join(fields.keys()),
        "mpi": "1000", "invt": "2", "fltt": "1",
        "secid": f"90.{code}", 
        "ut": "fa5fd1943c7b386f172d6893dbfba10b"
    }
    response = requests.get(url, params=params)
    data = response.json()['data']
    df = pd.Series(data).reset_index()
    df.columns = ['字段', '值']
    df['字段'] = df['字段'].map(fields)
    # 单位转换
    df['值'] = pd.to_numeric(df['值'], errors='coerce') / 100
    df.loc[df['字段'].isin(['成交量', '成交额']), '值'] *= 100
    return df

# 历史行情数据
def em_index_data(name: str, freq: str = 'd', 
               start: str = None, end: str = None,
               fqt: str = '') -> pd.DataFrame:
    """
    获取板块历史行情
    :param name: 板块名称或代码
    :param freq: 周期（d/w/m）
    :param start: 起始日期（YYYYMMDD）
    :param end: 结束日期（YYYYMMDD）
    :param fqt: 复权类型（''/qfq/hfq）
    :return: 历史行情数据
    """
    code, sector_type = __get_sector_code(name)
    period_map = {'d': '101', 'w': '102', 'm': '103'}
    adjust_map = {'': '0', 'qfq': '1', 'hfq': '2', '1': '1', '2': '2'}
    
    # URL选择
    if sector_type == 'industry':
        url = "http://7.push2his.eastmoney.com/api/qt/stock/kline/get"
    else:
        url = "https://91.push2his.eastmoney.com/api/qt/stock/kline/get"
    
    params = {
        "secid": f"90.{code}",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": period_map[freq],
        "fqt": adjust_map[fqt],
        "beg": start or "19000101",
        "end": end or "20500101",
        "lmt": "1000000",
        "_": str(int(pd.Timestamp.now().timestamp()*1000))
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    rows = [item.split(',') for item in data['data']['klines']]
    df = pd.DataFrame(rows, columns=[
        '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额',
        '振幅', '涨跌幅', '涨跌额', '换手率'
    ])[['日期', '开盘', '收盘', '最高', '最低', '涨跌幅', 
        '涨跌额', '成交量', '成交额', '振幅', '换手率']]
    numeric_cols = df.columns[1:]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df