#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  ：Jinyi Zhang 
@Date    ：2022/9/29 20:27 
'''

import json
import re
import signal
import time
import requests

import pandas as pd
import multitasking
from tqdm import tqdm
from func_timeout import func_set_timeout

from jsonpath import jsonpath
from datetime import datetime, timedelta

from qstock.data.util import (request_header, session, market_num_dict,
                  get_code_id, trans_num, trade_detail_dict, )

signal.signal(signal.SIGINT, multitasking.killall)

#获取某市场所有股票/债券/基金代码
def get_code(market='沪深A'):
    """
    获取某市场交易标的代码和名称
     market表示行情名称或列表，默认沪深A股
    '沪深京A':沪深京A股市场; '沪深A':沪深A股市场;'沪A':沪市A股市场
    '深A':深市A股市场;北A :北证A股市场;'可转债':沪深可转债市场;
    '期货':期货市场;'创业板':创业板市场行情;'美股':美股市场;
    '港股':港股市场;'中概股':中国概念股市场;'新股':沪深新股市场;
    '科创板':科创板市场;'沪股通' 沪股通市场;'深股通':深股通市场;
    '行业板块':行业板块市场;'概念板块':概念板块市场;
    '沪深指数':沪深系列指数市场;'上证指数':上证系列指数市场
    '深证指数':深证系列指数市场;'ETF' ETF基金市场;'LOF' LOF 基金市场
    """
    df=market_realtime(market)
    codes=list(df['代码'])
    return codes

#获取某市场所有股票/债券/基金名称
def get_name(market='沪深A'):
    """
    获取某市场交易标的代码和名称
     market表示行情名称或列表，默认沪深A股
    '沪深京A':沪深京A股市场; '沪深A':沪深A股市场;'沪A':沪市A股市场
    '深A':深市A股市场;北A :北证A股市场;'可转债':沪深可转债市场;
    '期货':期货市场;'创业板':创业板市场行情;'美股':美股市场;
    '港股':港股市场;'中概股':中国概念股市场;'新股':沪深新股市场;
    '科创板':科创板市场;'沪股通' 沪股通市场;'深股通':深股通市场;
    '行业板块':行业板块市场;'概念板块':概念板块市场;
    '沪深指数':沪深系列指数市场;'上证指数':上证系列指数市场
    '深证指数':深证系列指数市场;'ETF' ETF基金市场;'LOF' LOF 基金市场
    """
    df=market_realtime(market)
    names=list(df['名称'])
    return names

#获取某市场所有股票/债券/基金名称代码字典
def get_name_code(market='沪深A'):
    """
    获取某市场交易标的代码和名称
     market表示行情名称或列表，默认沪深A股
    '沪深京A':沪深京A股市场; '沪深A':沪深A股市场;'沪A':沪市A股市场
    '深A':深市A股市场;北A :北证A股市场;'可转债':沪深可转债市场;
    '期货':期货市场;'创业板':创业板市场行情;'美股':美股市场;
    '港股':港股市场;'中概股':中国概念股市场;'新股':沪深新股市场;
    '科创板':科创板市场;'沪股通' 沪股通市场;'深股通':深股通市场;
    '行业板块':行业板块市场;'概念板块':概念板块市场;
    '沪深指数':沪深系列指数市场;'上证指数':上证系列指数市场
    '深证指数':深证系列指数市场;'ETF' ETF基金市场;'LOF' LOF 基金市场
    """
    df=market_realtime(market)
    name_code_dict=dict(df[['名称','代码']].values)
    return name_code_dict

# 获取某指定市场所有标的最新行情指标
def market_realtime(market='沪深A'):
    """
    获取沪深市场最新行情总体情况（涨跌幅、换手率等信息）
     market表示行情名称或列表，默认沪深A股
    '沪深京A':沪深京A股市场行情; '沪深A':沪深A股市场行情;'沪A':沪市A股市场行情
    '深A':深市A股市场行情;北A :北证A股市场行情;'可转债':沪深可转债市场行情;
    '期货':期货市场行情;'创业板':创业板市场行情;'美股':美股市场行情;
    '港股':港股市场行情;'中概股':中国概念股市场行情;'新股':沪深新股市场行情;
    '科创板':科创板市场行情;'沪股通' 沪股通市场行情;'深股通':深股通市场行情;
    '行业板块':行业板块市场行情;'概念板块':概念板块市场行情;
    '沪深指数':沪深系列指数市场行情;'上证指数':上证系列指数市场行情
    '深证指数':深证系列指数市场行情;'ETF' ETF基金市场行情;'LOF' LOF 基金市场行情
    """
    # 市场与编码
    market_dict = {
        'stock': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
        '沪深A': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
        '上证A': 'm:1 t:2,m:1 t:23',
        '沪A': 'm:1 t:2,m:1 t:23',
        '深证A': 'm:0 t:6,m:0 t:80',
        '深A': 'm:0 t:6,m:0 t:80',
        '北证A': 'm:0 t:81 s:2048',
        '北A': 'm:0 t:81 s:2048',
        '创业板': 'm:0 t:80',
        '科创板': 'm:1 t:23',
        '沪深京A': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
        '沪股通': 'b:BK0707',
        '深股通': 'b:BK0804',
        '风险警示板': 'm:0 f:4,m:1 f:4',
        '两网及退市': 'm:0 s:3',
        '新股': 'm:0 f:8,m:1 f:8',
        '美股': 'm:105,m:106,m:107',
        '港股': 'm:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2',
        '英股': 'm:155 t:1,m:155 t:2,m:155 t:3,m:156 t:1,m:156 t:2,m:156 t:5,m:156 t:6,m:156 t:7,m:156 t:8',
        '中概股': 'b:MK0201',
        '中国概念股': 'b:MK0201',
        '地域板块': 'm:90 t:1 f:!50',
        '地域': 'm:90 t:1 f:!50',
        '行业板块': 'm:90 t:2 f:!50',
        '行业': 'm:90 t:2 f:!50',
        '概念板块': 'm:90 t:3 f:!50',
        '概念': 'm:90 t:3 f:!50',
        '上证指数': 'm:1 s:2',
        '上证系列指数': 'm:1 s:2',
        '深证指数': 'm:0 t:5',
        '深证系列指数': 'm:0 t:5',
        '沪深指数': 'm:1 s:2,m:0 t:5',
        '沪深系列指数': 'm:1 s:2,m:0 t:5',
        'bond': 'b:MK0354',
        '债券': 'b:MK0354',
        '可转债': 'b:MK0354',
        'future': 'm:113,m:114,m:115,m:8,m:142',
        '期货': 'm:113,m:114,m:115,m:8,m:142',
        'ETF': 'b:MK0021,b:MK0022,b:MK0023,b:MK0024',
        'LOF': 'b:MK0404,b:MK0405,b:MK0406,b:MK0407', }

    fs = market_dict[market]
   
    fields = ",".join(trade_detail_dict.keys())
    # 初始化DataFrame
    df_total = pd.DataFrame()
    
    # 分页参数
    page_size = 200  # 每页最大返回200条
    page_number = 1
    
    while True:
        params = (
            ('pn', str(page_number)),
            ('pz', str(page_size)),
            ('po', '1'),
            ('np', '1'),
            ('fltt', '2'),
            ('invt', '2'),
            ('fid', 'f3'),
            ('fs', fs),
            ('fields', fields)
        )
        
        url = 'http://push2.eastmoney.com/api/qt/clist/get' 
        
        try:
            # 添加随机延迟和请求头伪装
            time.sleep(0.5)   # 防止被反爬
            response = session.get(url,  headers=request_header, params=params)
            json_response = response.json() 
            
            if not json_response['data']['diff']:
                break
                
            # 将当前页数据合并到总DataFrame
            df_current = pd.DataFrame(json_response['data']['diff'])
            df_total = pd.concat([df_total,  df_current], ignore_index=True)
            
            page_number += 1
            
        except Exception as e:
            #print(f"Error occurred at page {page_number}: {str(e)}")
            break
    
    # 数据清洗和转换
    df_total = df_total.rename(columns=trade_detail_dict) 
    df_total = df_total[trade_detail_dict.values()] 
    
    df_total['ID'] = df_total['编号'].astype(str) + '.' + df_total['代码'].astype(str)
    df_total['市场'] = df_total['编号'].astype(str).apply(lambda x: market_num_dict.get(x)) 
    df_total['时间'] = df_total['更新时间戳'].apply(lambda x: str(datetime.fromtimestamp(x))) 
    
    del df_total['更新时间戳']
    del df_total['编号']
    del df_total['ID']
    del df_total['市场']
    
    ignore_cols = ['代码', '名称', '时间']
    df_total = trans_num(df_total, ignore_cols)
    
    return df_total


# 获取单个或多个证券的最新行情指标
def stock_realtime(code_list):
    """
    获取股票、期货、债券的最新行情指标
    code_list:输入单个或多个证券的list
    """
    if isinstance(code_list, str):
        code_list = [code_list]
    secids = [get_code_id(code)
              for code in code_list]

    fields = ",".join(trade_detail_dict.keys())
    params = (
        ('OSVersion', '14.3'),
        ('appVersion', '6.3.8'),
        ('fields', fields),
        ('fltt', '2'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('secids', ",".join(secids)),
        ('serverVersion', '6.3.6'),
        ('version', '6.3.8'),
    )
    url = 'https://push2.eastmoney.com/api/qt/ulist.np/get'
    json_response = session.get(url,
                                headers=request_header,
                                params=params).json()
    rows = jsonpath(json_response, '$..diff[:]')
    if not rows:
        df = pd.DataFrame(columns=trade_detail_dict.values())
    else:
        df = pd.DataFrame(rows)[list(trade_detail_dict.keys())].rename(columns=trade_detail_dict)
    df['市场'] = df['编号'].apply(lambda x: market_num_dict.get(str(x)))
    del df['编号']
    df['时间'] = df['更新时间戳'].apply(lambda x: str(datetime.fromtimestamp(x)))
    del df['更新时间戳']
    # 将object类型转为数值型
    ignore_cols = ['名称', '代码', '市场', '时间']
    df = trans_num(df, ignore_cols)
    return df

# 将接口market_indics和stock_indics封装在一起
# 获取指定市场所有标的或单个或多个证券最新行情指标
def realtime_data(market='沪深A', code=None):
    '''获取指定市场所有标的或单个或多个证券最新行情指标
    market表示行情名称或列表，默认沪深A股
    '沪深京A':沪深京A股市场行情; '沪深A':沪深A股市场行情;'沪A':沪市A股市场行情
    '深A':深市A股市场行情;北A :北证A股市场行情;'可转债':沪深可转债市场行情;
    '期货':期货市场行情;'创业板':创业板市场行情;'美股':美股市场行情;
    '港股':港股市场行情;'中概股':中国概念股市场行情;'新股':沪深新股市场行情;
    '科创板':科创板市场行情;'沪股通' 沪股通市场行情;'深股通':深股通市场行情;
    '行业板块':行业板块市场行情;'概念板块':概念板块市场行情;
    '沪深指数':沪深系列指数市场行情;'上证指数':上证系列指数市场行情
    '深证指数':深证系列指数市场行情;'ETF' ETF基金市场行情;'LOF' LOF 基金市场行情
    code:输入单个或多个证券的list，不输入参数，默认返回某市场实时指标
    如code='中国平安'，或code='000001'，或code=['中国平安','晓程科技','东方财富']
    '''
    if code is None:
        return market_realtime(market)
    else:
        return stock_realtime(code)


# 获取单只证券最新交易日日内数据
def intraday_data(code):
    """
    code可以为股票、期货、债券代码简称或代码，如晓程科技或300139
    也可以是多个股票或期货或债券的list,如['300139','西部建设','云南铜业']
    返回股票、期货、债券的最新交易日成交情况
    """
    max_count = 10000000
    code_id = get_code_id(code)
    columns = ['名称', '代码', '时间', '昨收', '成交价', '成交量', '单数']
    params = (
        ('secid', code_id),
        ('fields1', 'f1,f2,f3,f4,f5'),
        ('fields2', 'f51,f52,f53,f54,f55'),
        ('pos', f'-{int(max_count)}')
    )

    response = session.get(
        'https://push2.eastmoney.com/api/qt/stock/details/get', params=params)

    res = response.json()
    texts = res['data']['details']
    rows = [txt.split(',')[:4] for txt in texts]
    df = pd.DataFrame(columns=columns, index=range(len(rows)))
    df.loc[:, '代码'] = code_id.split('.')[1]
    df.loc[:, '名称'] = stock_info(code)['名称']
    detail_df = pd.DataFrame(rows, columns=['时间', '成交价', '成交量', '单数'])
    detail_df.insert(1, '昨收', res['data']['prePrice'])
    df.loc[:, detail_df.columns] = detail_df.values
    # 将object类型转为数值型
    ignore_cols = ['名称', '代码', '时间']
    df = trans_num(df, ignore_cols)
    return df


# 获取个股当天实时交易快照数据
def stock_snapshot(code):
    """
    获取沪深市场股票最新行情快照
    code:股票代码
    """
    code = get_code_id(code).split('.')[1]
    params = (
        ('id', code),
        ('callback', 'jQuery183026310160411569883_1646052793441'),
    )
    columns = {
        'code': '代码',
        'name': '名称',
        'time': '时间',
        'zd': '涨跌额',
        'zdf': '涨跌幅',
        'currentPrice': '最新价',
        'yesClosePrice': '昨收',
        'openPrice': '今开',
        'open': '开盘',
        'high': '最高',
        'low': '最低',
        'avg': '均价',
        'topprice': '涨停价',
        'bottomprice': '跌停价',
        'turnover': '换手率',
        'volume': '成交量',
        'amount': '成交额',
        'sale1': '卖1价',
        'sale2': '卖2价',
        'sale3': '卖3价',
        'sale4': '卖4价',
        'sale5': '卖5价',
        'buy1': '买1价',
        'buy2': '买2价',
        'buy3': '买3价',
        'buy4': '买4价',
        'buy5': '买5价',
        'sale1_count': '卖1数量',
        'sale2_count': '卖2数量',
        'sale3_count': '卖3数量',
        'sale4_count': '卖4数量',
        'sale5_count': '卖5数量',
        'buy1_count': '买1数量',
        'buy2_count': '买2数量',
        'buy3_count': '买3数量',
        'buy4_count': '买4数量',
        'buy5_count': '买5数量',
    }
    response = requests.get(
        'https://hsmarketwg.eastmoney.com/api/SHSZQuoteSnapshot', params=params)
    start_index = response.text.find('{')
    end_index = response.text.rfind('}')

    s = pd.Series(index=columns.values(), dtype='object')
    try:
        data = json.loads(response.text[start_index:end_index + 1])
    except:
        return s
    if not data.get('fivequote'):
        return s
    d = {**data.pop('fivequote'), **data.pop('realtimequote'), **data}
    ss = pd.Series(d).rename(index=columns)[columns.values()]
    str_type_list = ['代码', '名称', '时间']
    all_type_list = columns.values()
    for column in (set(all_type_list) - set(str_type_list)):
        ss[column] = str(ss[column]).strip('%')
    df = pd.DataFrame(ss).T
    # 将object类型转为数值型
    ignore_cols = ['名称', '代码', '时间']
    df = trans_num(df, ignore_cols)
    return df


# 获取最近n日（最多五天）的1分钟数据

def get_1min_data(code, n=5):
    """
    获取股票、期货、债券的最近n日的1分钟K线行情
    code : 代码、名称
    n: 默认为 1,最大为 5
    """
    intraday_dict = {
        'f51': '日期',
        'f52': '开盘',
        'f53': '收盘',
        'f54': '最高',
        'f55': '最低',
        'f56': '成交量',
        'f57': '成交额', }
    fields = list(intraday_dict.keys())
    columns = list(intraday_dict.values())
    fields2 = ",".join(fields)
    n = n if n <= 5 else 5
    code_id = get_code_id(code)
    params = (
        ('fields1', 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13'),
        ('fields2', fields2),
        ('ndays', n),
        ('iscr', '0'),
        ('iscca', '0'),
        ('secid', code_id),
    )

    res = session.get('http://push2his.eastmoney.com/api/qt/stock/trends2/get',
                      params=params).json()

    data = jsonpath(res, '$..trends[:]')
    if not data:
        columns.insert(0, '代码')
        columns.insert(0, '名称')
        return pd.DataFrame(columns=columns)

    rows = [d.split(',') for d in data]
    name = res['data']['name']
    code = code_id.split('.')[-1]
    df = pd.DataFrame(rows, columns=columns)
    df.insert(0, '代码', code)
    df.insert(0, '名称', name)
    cols1 = ['日期', '名称', '代码', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
    cols2 = ['date', 'name', 'code', 'open', 'high', 'low', 'close', 'vol', 'turnover']
    df = df.rename(columns=dict(zip(cols1, cols2)))
    df.index = pd.to_datetime(df['date'])
    df = df[cols2[1:]]
    # 将object类型转为数值型
    ignore_cols = ['name', 'code']
    df = trans_num(df, ignore_cols)
    return df

#实时交易盘口异动数据
def realtime_change(flag=None):
    '''
    flag：盘口异动类型，默认输出全部类型的异动情况
    可选：['火箭发射', '快速反弹','加速下跌', '高台跳水', '大笔买入', '大笔卖出', 
        '封涨停板','封跌停板', '打开跌停板','打开涨停板','有大买盘','有大卖盘', 
        '竞价上涨', '竞价下跌','高开5日线','低开5日线',  '向上缺口','向下缺口', 
        '60日新高','60日新低','60日大幅上涨', '60日大幅下跌']
    '''
    #默认输出市场全部类型的盘口异动情况（相当于短线精灵）
    changes_list=['火箭发射', '快速反弹','加速下跌', '高台跳水', '大笔买入', 
        '大笔卖出', '封涨停板','封跌停板', '打开跌停板','打开涨停板','有大买盘',
        '有大卖盘', '竞价上涨', '竞价下跌','高开5日线','低开5日线', '向上缺口',
        '向下缺口', '60日新高','60日新低','60日大幅上涨', '60日大幅下跌']
    n=range(1,len(changes_list)+1)
    change_dict=dict(zip(n,changes_list))
    if flag is not None:
        if isinstance(flag,int):
            flag=change_dict[flag]
        return stock_changes(symbol=flag)
    else:
        
        df=stock_changes(symbol=changes_list[0])
        for s in changes_list[1:]:
            temp=stock_changes(symbol=s)
            df=pd.concat([df,temp])
            df=df.sort_values('时间',ascending=False)
        return df

#东方财富网实时交易盘口异动数据
def stock_changes(symbol):
    """
    东方财富行盘口异动
    http://quote.eastmoney.com/changes/
    :symbol:  {'火箭发射', '快速反弹', '大笔买入', '封涨停板', '打开跌停板', 
               '有大买盘', '竞价上涨', '高开5日线', '向上缺口', '60日新高', 
               '60日大幅上涨', '加速下跌', '高台跳水', '大笔卖出', '封跌停板', 
               '打开涨停板', '有大卖盘', '竞价下跌', '低开5日线', '向下缺口', 
               '60日新低', '60日大幅下跌'}
    """
    url = "http://push2ex.eastmoney.com/getAllStockChanges"
    symbol_map = {
        "火箭发射": "8201",
        "快速反弹": "8202",
        "大笔买入": "8193",
        "封涨停板": "4",
        "打开跌停板": "32",
        "有大买盘": "64",
        "竞价上涨": "8207",
        "高开5日线": "8209",
        "向上缺口": "8211",
        "60日新高": "8213",
        "60日大幅上涨": "8215",
        "加速下跌": "8204",
        "高台跳水": "8203",
        "大笔卖出": "8194",
        "封跌停板": "8",
        "打开涨停板": "16",
        "有大卖盘": "128",
        "竞价下跌": "8208",
        "低开5日线": "8210",
        "向下缺口": "8212",
        "60日新低": "8214",
        "60日大幅下跌": "8216",
    }
    reversed_symbol_map = {v: k for k, v in symbol_map.items()}
    params = {
        "type": symbol_map[symbol],
        "pageindex": "0",
        "pagesize": "5000",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "dpt": "wzchanges",
        "_": "1624005264245",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    df = pd.DataFrame(data_json["data"]["allstock"])
    df["tm"] = pd.to_datetime(df["tm"], format="%H%M%S").dt.time
    df.columns = ["时间","代码","_","名称","板块","相关信息",]
    df= df[["时间","代码","名称","板块","相关信息",]]
    df["板块"] = df["板块"].astype(str)
    df["板块"] = df["板块"].map(reversed_symbol_map)
    return df


# 获取股票、债券、期货、基金历史K线数据
def web_data(code, start='19000101', end=None, freq='d', fqt=1):
    """
    获取股票、指数、债券、期货、基金等历史K线行情
    code可以是股票或指数（包括美股港股等）代码或简称
    start和end为起始和结束日期，年月日
    freq:时间频率，默认日，1 : 分钟；5 : 5 分钟；15 : 15 分钟；30 : 30 分钟；
    60 : 60 分钟；101或'D'或'd'：日；102或‘w’或'W'：周; 103或'm'或'M': 月
    注意1分钟只能获取最近5个交易日一分钟数据
    fqt:复权类型，0：不复权，1：前复权；2：后复权，默认前复权
    """
    if end in [None,'']:
        end=latest_trade_date()
    if freq == 1:
        return get_1min_data(code)
    start = ''.join(start.split('-'))
    end = ''.join(end.split('-'))
    if type(freq) == str:
        freq = freq.lower()
        if freq == 'd':
            freq = 101
        elif freq == 'w':
            freq = 102
        elif freq == 'm':
            freq = 103
        else:
            print('时间频率输入有误')
    kline_field = {
        'f51': '日期',
        'f52': '开盘',
        'f53': '收盘',
        'f54': '最高',
        'f55': '最低',
        'f56': '成交量',
        'f57': '成交额',
        'f58': '振幅',
        'f59': '涨跌幅',
        'f60': '涨跌额',
        'f61': '换手率'}
    fields = list(kline_field.keys())
    columns = list(kline_field.values())
    cols1 = ['日期', '名称', '代码', '开盘', '最高', '最低', '收盘', '成交量', '成交额', '换手率']
    cols2 = ['date', 'name', 'code', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'turnover_rate']
    fields2 = ",".join(fields)
    code_id = get_code_id(code)
    params = (
        ('fields1', 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13'),
        ('fields2', fields2),
        ('beg', start),
        ('end', end),
        ('rtntype', '6'),
        ('secid', code_id),
        ('klt', f'{freq}'),
        ('fqt', f'{fqt}'),
    )

    url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    # 多线程装饰器

    json_response = session.get(
        url, headers=request_header, params=params).json()
    klines = jsonpath(json_response, '$..klines[:]')
    if not klines:
        columns.insert(0, '代码')
        columns.insert(0, '名称')
        return pd.DataFrame(columns=cols2)

    rows = [k.split(',') for k in klines]
    name = json_response['data']['name']
    code = code_id.split('.')[-1]
    df = pd.DataFrame(rows, columns=columns)

    df.insert(0, '代码', code)
    df.insert(0, '名称', name)

    df = df.rename(columns=dict(zip(cols1, cols2)))
    df.index = pd.to_datetime(df['date'])
    df = df[cols2[1:]]
    # 将object类型转为数值型
    ignore_cols = ['name', 'code']
    df = trans_num(df, ignore_cols)
    return df

def latest_trade_date():
    date=stock_realtime('上证指数')['时间'].values[0][:10]
    return date
    

# 获取单只或多只证券（股票、基金、债券、期货)的收盘价格dataframe
def get_price(code_list, start='19000101', end=None, freq='d', fqt=1):
    '''code_list输入股票list列表
    如code_list=['中国平安','贵州茅台','工业富联']
    '''
    if isinstance(code_list, str):
        code_list = [code_list]
    
    if end is None:
        end=latest_trade_date()

    @multitasking.task
    #@retry(tries=3, delay=1)
    @func_set_timeout(10)
    def run(code):
        try:
            temp = web_data(code, start, end, freq, fqt)
            temp[temp.name[0]]=temp.close
            data_list.append(temp[temp.name[0]])
            pbar.update()
        except:
            pass
    pbar = tqdm(total=len(code_list),leave=False)
    data_list = []
    for code in code_list:
        try:
            run(code)
        except:
            continue
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=1)
    return df


# 获取单只或多只证券（股票、基金、债券、期货)的历史K线数据
def get_data(code_list, start='19000101', end=None, freq='d', fqt=1):
    '''code_list输入股票list列表
    如code_list=['中国平安','贵州茅台','工业富联']
    返回多只股票多期时间的面板数据
    '''
    if isinstance(code_list, str):
        code_list = [code_list]
    if end is None:
        end=latest_trade_date()

    data_list = []
    pbar = tqdm(total=len(code_list),leave=False)
    
    @multitasking.task
    #@retry(tries=3, delay=1)
    @func_set_timeout(10)
    def run(code):
        data = web_data(code, start, end, freq, fqt)
        data_list.append(data)
        pbar.update()
    for code in code_list:
        run(code)
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=0)
    return df

#获取沪深指数对应代码名称字典
def index_code_name():
    df=realtime_data('沪深指数')
    code_name_dict=dict((df[['代码','名称']].values))
    return code_name_dict

#获取指数历史交易数据
def get_index_data(code_list, start='19000101', end=None, freq='d'):
    
    if isinstance(code_list, str):
        code_list = [code_list]
    if end is None:
        end=latest_trade_date()

    data_list = []
    pbar = tqdm(total=len(code_list),leave=False)
    
    @multitasking.task
    @func_set_timeout(5)
    def run(code):
        data = web_data(code, start=start, end=end, freq=freq)
        data_list.append(data)
        pbar.update()
    for code in code_list:
        if code.isdigit():
            code_name_dict=index_code_name()
            code=code_name_dict[code]
        run(code)
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=0)
    return df

# 获取指数价格数据
def get_index_price(code_list, start='19000101', end=None, freq='d'):
    '''code_list输入指数list列表
   
    '''
    
    if isinstance(code_list, str):
        code_list = [code_list]
    
    if end is None:
        end=latest_trade_date()

    @multitasking.task
    @func_set_timeout(5)
    def run(code):
        try:
            temp = web_data(code, start, end, freq)
            temp[temp.name[0]]=temp.close
            data_list.append(temp[temp.name[0]])
            pbar.update()
        except:
            pass
            
    pbar = tqdm(total=len(code_list),leave=False)
    data_list = []
    for code in code_list:
        if code.isdigit():
            code_name_dict=index_code_name()
            code=code_name_dict[code]
        try:
            run(code)
        except:
            continue
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=1)
    return df


###股票数据接口
# 获取单只个股最新的基本财务指标
def stock_info(code):
    '''code输入股票代码或简称'''
    stock_info_dict = {
        'f57': '代码',
        'f58': '名称',
        'f162': '市盈率(动)',
        'f167': '市净率',
        'f127': '所处行业',
        'f116': '总市值',
        'f117': '流通市值',
        'f173': 'ROE',
        'f187': '净利率',
        'f105': '净利润',
        'f186': '毛利率'}

    code_id = get_code_id(code)
    fields = ",".join(stock_info_dict.keys())
    params = (
        ('ut', 'fa5fd1943c7b386f172d6893dbfba10b'),
        ('invt', '2'),
        ('fltt', '2'),
        ('fields', fields),
        ('secid', code_id)
    )
    url = 'http://push2.eastmoney.com/api/qt/stock/get'
    json_response = session.get(url,
                                headers=request_header,
                                params=params).json()
    items = json_response['data']
    if not items:
        return pd.Series(index=stock_info_dict.values(), dtype='object')

    s = pd.Series(items, dtype='object').rename(
        index=stock_info_dict)
    return s

# 获取单只或多只股票最新的基本财务指标
def stock_basics(code_list):
    '''code_list:代码或简称，可以输入单只或多只个股的list
    如：code_list='中国平安'
    code_list=['晓程科技','中国平安','西部建设']
    返回：代码、名称、净利润、总市值、流通市值、所处行业、市盈率、市净率、ROE、毛利率和净利率指标
    '''
    if isinstance(code_list, str):
        code_list = [code_list]
    df = pd.DataFrame(stock_info(code_list[0])).T
    for code in tqdm(code_list[1:],leave=False):
        try:
            temp = pd.DataFrame(stock_info(code)).T
            df = pd.concat([df, temp])
        except:
            continue
    cols = ['代码', '名称', '所处行业']
    df = trans_num(df, cols)
    return df

# 获取沪深市场全部股票报告期信息
def report_date():
    """
    获取沪深市场的全部股票报告期信息
    """
    fields = {
        'REPORT_DATE': '报告日期',
        'DATATYPE': '季报名称'
    }
    params = (
        ('type', 'RPT_LICO_FN_CPD_BBBQ'),
        ('sty', ','.join(fields.keys())),
        ('p', '1'),
        ('ps', '2000'),

    )
    url = 'https://datacenter.eastmoney.com/securities/api/data/get'
    response = requests.get(
        url,
        headers=request_header,
        params=params)
    items = jsonpath(response.json(), '$..data[:]')
    if not items:
        pd.DataFrame(columns=fields.values())
    df = pd.DataFrame(items)
    df = df.rename(columns=fields)
    df['报告日期'] = df['报告日期'].apply(lambda x: x.split()[0])
    return df


def latest_report_date():
    df = report_date()
    return df['报告日期'].iloc[0]


def index_member(code):
    """
    获取指数成分股信息
    code : 指数名称或者指数代码
    """
    fields = {
        'IndexCode': '指数代码',
        'IndexName': '指数名称',
        'StockCode': '股票代码',
        'StockName': '股票名称',
        'MARKETCAPPCT': '股票权重'
    }
    code_id = get_code_id(code).split('.')[1]
    params = (
        ('IndexCode', code_id),
        ('pageIndex', '1'),
        ('pageSize', '10000'),
        ('deviceid', '1234567890'),
        ('version', '6.9.9'),
        ('product', 'EFund'),
        ('plat', 'Iphone'),
        ('ServerVersion', '6.9.9'),
    )
    url = 'https://fundztapi.eastmoney.com/FundSpecialApiNew/FundSpecialZSB30ZSCFG'
    res = requests.get(
        url,
        params=params,
        headers=request_header).json()
    data = res['Datas']
    if not data:
        return
    df = pd.DataFrame(data).rename(
        columns=fields)[fields.values()]
    df['股票权重'] = pd.to_numeric(df['股票权重'], errors='coerce')
    return df


# 获取沪深市场股票某一季度的表现情况
def company_indicator(date=None):
    """
    获取沪深市场股票某一季度的表财务指标
    date报告发布日期，默认最新，如‘2022-09-30’
    一季度：‘2021-03-31’；二季度：'2021-06-30'
    三季度：'2021-09-30'；四季度：'2021-12-31'
    """
    if date is not None and '-' not in date:
        date_trans = lambda s: '-'.join([s[:4], s[4:6], s[6:]])
        date = date_trans(date)
    if date not in report_date()['报告日期'].to_list():
        date = latest_report_date()

    fields = {
        'SECURITY_CODE': '代码',
        'SECURITY_NAME_ABBR': '简称',
        'NOTICE_DATE': '公告日期',
        'TOTAL_OPERATE_INCOME': '营收',
        'YSTZ': '营收同比',
        'YSHZ': '营收环比',
        'PARENT_NETPROFIT': '净利润',
        'SJLTZ': '净利润同比',
        'SJLHZ': '净利润环比',
        'BASIC_EPS': '每股收益',
        'BPS': '每股净资产',
        'WEIGHTAVG_ROE': '净资产收益率',
        'XSMLL': '销售毛利率',
        'MGJYXJJE': '每股经营现金流'
    }

    date = f"(REPORTDATE=\'{date}\')"
    page = 1
    dfs = []
    while True:
        params = (
            ('st', 'NOTICE_DATE,SECURITY_CODE'),
            ('sr', '-1,-1'),
            ('ps', '500'),
            ('p', f'{page}'),
            ('type', 'RPT_LICO_FN_CPD'),
            ('sty', 'ALL'),
            ('token', '894050c76af8597a853f5b408b759f5d'),
            # 沪深A股
            ('filter',
             f'(SECURITY_TYPE_CODE in ("058001001","058001008")){date}'),

        )
        url = 'http://datacenter-web.eastmoney.com/api/data/get'
        response = session.get(url,
                               headers=request_header,
                               params=params)
        items = jsonpath(response.json(), '$..data[:]')
        if not items:
            break
        df = pd.DataFrame(items)
        dfs.append(df)
        page += 1
    if len(dfs) == 0:
        df = pd.DataFrame(columns=fields.values())
        return df
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = df.rename(columns=fields)[fields.values()]
    cols = ['代码', '简称', '公告日期']
    df = trans_num(df, cols).round(3)
    return df


# 龙虎榜详情数据
def stock_billboard(start=None, end=None):
    '''起始和结束日期默认为None，表示最新，日期格式'2021-08-21'
    '''
    # 如果输入日期没有带'-'连接符，转换一下
    date_trans = lambda s: '-'.join([s[:4], s[4:6], s[6:]])
    if start is not None:
        if '-' not in start:
            start = date_trans(start)
    if end is not None:
        if '-' not in end:
            end = date_trans(end)

    today = datetime.today().date()
    mode = 'auto'
    if start is None:
        start_date = today

    if end is None:
        end_date = today

    if isinstance(start, str):
        mode = 'user'
        start_date = datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        mode = 'user'
        end_date = datetime.strptime(end, '%Y-%m-%d')

    fields = {
        'SECURITY_CODE': '股票代码',
        'SECURITY_NAME_ABBR': '股票名称',
        'TRADE_DATE': '上榜日期',
        'EXPLAIN': '解读',
        'CLOSE_PRICE': '收盘价',
        'CHANGE_RATE': '涨跌幅',
        'TURNOVERRATE': '换手率',
        'BILLBOARD_NET_AMT': '龙虎榜净买额',
        'BILLBOARD_BUY_AMT': '龙虎榜买入额',
        'BILLBOARD_SELL_AMT': '龙虎榜卖出额',
        'BILLBOARD_DEAL_AMT': '龙虎榜成交额',
        'ACCUM_AMOUNT': '市场总成交额',
        'DEAL_NET_RATIO': '净买额占总成交比',
        'DEAL_AMOUNT_RATIO': '成交额占总成交比',
        'FREE_MARKET_CAP': '流通市值',
        'EXPLANATION': '上榜原因'
    }
    bar = None
    while True:
        dfs = []
        page = 1
        while 1:
            params = (
                ('sortColumns', 'TRADE_DATE,SECURITY_CODE'),
                ('sortTypes', '-1,1'),
                ('pageSize', '500'),
                ('pageNumber', page),
                ('reportName', 'RPT_DAILYBILLBOARD_DETAILS'),
                ('columns', 'ALL'),
                ('source', 'WEB'),
                ('client', 'WEB'),
                ('filter',
                 f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')"),
            )

            url = 'http://datacenter-web.eastmoney.com/api/data/v1/get'

            response = session.get(url, params=params)
            if bar is None:
                pages = jsonpath(response.json(), '$..pages')

                if pages and pages[0] != 1:
                    total = pages[0]
                    bar = tqdm(total=int(total),leave=False)
            if bar is not None:
                bar.update()

            items = jsonpath(response.json(), '$..data[:]')
            if not items:
                break
            page += 1
            df = pd.DataFrame(items).rename(columns=fields)[fields.values()]
            dfs.append(df)
        if mode == 'user':
            break
        if len(dfs) == 0:
            start_date = start_date - timedelta(1)
            end_date = end_date - timedelta(1)

        if len(dfs) > 0:
            break
    if len(dfs) == 0:
        df = pd.DataFrame(columns=fields.values())
        return df

    df = pd.concat(dfs, ignore_index=True)
    df['上榜日期'] = df['上榜日期'].astype('str').apply(lambda x: x.split(' ')[0])
    # 保留需要的数据特征
    cols = ['股票代码', '股票名称', '上榜日期', '收盘价', '涨跌幅', '换手率',
            '龙虎榜净买额', '流通市值', '上榜原因', '解读']
    # 有些股票可能因不同原因上榜，剔除重复记录样本
    df = df[cols].drop_duplicates(['股票代码', '上榜日期'])
    # 剔除退市、B股和新股N
    s1 = df['股票名称'].str.contains('退')
    s2 = df['股票名称'].str.contains('B')
    s3 = df['股票名称'].str.contains('N')
    s = s1 | s2 | s3
    df = df[-(s)]
    return df


# 获取股票所属板块
def stock_sector(code):
    """
    获取股票所属板块
    code : 股票代码或者股票名称
    """
    code_id = get_code_id(code)

    params = (
        ('forcect', '1'),
        ('spt', '3'),
        ('fields', 'f1,f12,f152,f3,f14,f128,f136'),
        ('pi', '0'),
        ('pz', '1000'),
        ('po', '1'),
        ('fid', 'f3'),
        ('fid0', 'f4003'),
        ('invt', '2'),
        ('secid', code_id),
    )

    res = session.get(
        'https://push2.eastmoney.com/api/qt/slist/get', params=params)
    df = pd.DataFrame(res.json()['data']['diff']).T
    df.index = range(len(df))
    filelds = {
        'f12': '代码',
        'f14': '简称',
        'f3': '涨幅',
    }
    df = df.rename(columns=filelds)[filelds.values()]
    code = code_id.split('.')[-1]
    # df.insert(0, '股票名称', name)
    #df.insert(1, '代码', code)
    cols = ['代码', '简称']
    df = trans_num(df, cols).round(3)
    #df['涨幅'] = (df['涨幅']/ 100)
    return df

##############################################################################
####基金fund

fund_header = {
    'User-Agent': 'EMProjJijin/6.2.8 (iPhone; iOS 13.6; Scale/2.00)',
    'GTOKEN': '98B423068C1F4DEF9842F82ADF08C5db',
    'clientInfo': 'ttjj-iPhone10,1-iOS-iOS13.6',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'fundmobapi.eastmoney.com',
    'Referer': 'https://mpservice.com/516939c37bdb4ba2b1138c50cf69a2e1/release/pages/FundHistoryNetWorth',
}


# 获取基金单位净值（当前净资产大小）和累计净值（自成立以来的整体收益情况）
def fund_data_single(code):
    """
    根据基金代码和要获取的页码抓取基金净值信息
    code : 6 位基金代码
    """
    # 页码
    pz = 50000
    data = {
        'FCODE': f'{code}',
        'IsShareNet': 'true',
        'MobileKey': '1',
        'appType': 'ttjj',
        'appVersion': '6.2.8',
        'cToken': '1',
        'deviceid': '1',
        'pageIndex': '1',
        'pageSize': f'{pz}',
        'plat': 'Iphone',
        'product': 'EFund',
        'serverVersion': '6.2.8',
        'uToken': '1',
        'userId': '1',
        'version': '6.2.8'
    }
    url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList'
    json_response = requests.get(
        url,
        headers=fund_header,
        data=data).json()
    rows = []
    columns = ['日期', '单位净值', '累计净值', '涨跌幅']
    if json_response is None:
        return pd.DataFrame(rows, columns=columns)
    datas = json_response['Datas']
    if len(datas) == 0:
        return pd.DataFrame(rows, columns=columns)
    rows = []
    for stock in datas:
        date = stock['FSRQ']
        rows.append({
            '日期': date,
            '单位净值': stock['DWJZ'],
            '累计净值': stock['LJJZ'],
            '涨跌幅': stock['JZZZL']
        })
    df = pd.DataFrame(rows)
    df.index = pd.to_datetime(df['日期'])
    df['涨跌幅'] = df['涨跌幅'].apply(lambda x: 0 if x == '--' else float(x))
    df = df.iloc[:, 1:].astype('float').sort_index()
    return df


# 获取多只基金的累计净值dataframe
def fund_price(code_list):
    '''code_list输入基金list列表
    如code_list=['180003','340006','159901']
    '''

    @multitasking.task
    @func_set_timeout(5)
    def run(code):
        temp = fund_data_single(code)
        data[code] = temp['累计净值']
        pbar.update()
    pbar = tqdm(total=len(code_list),leave=False)
    data = pd.DataFrame()
    for code in code_list:
        run(code)
    multitasking.wait_for_tasks()

    return data


# 获取单只或多只基金的历史净值数据
def fund_data(code_list):
    '''code_list输入股票list列表
    如code_list=['中国平安','贵州茅台','工业富联']
    返回多只股票多期时间的面板数据
    '''
    if isinstance(code_list, str):
        code_list = [code_list]
    data_list = []
    pbar = tqdm(total=len(code_list),leave=False)
    
    @multitasking.task
    @func_set_timeout(5)
    def run(code):
        data = fund_data_single(code)
        data['code'] = code
        data_list.append(data)
        pbar.update()
    for code in code_list:
        run(code)
    multitasking.wait_for_tasks()
    # 转换为dataframe
    df = pd.concat(data_list, axis=0)
    return df

def fund_code(ft=None):
    """
    获取天天基金网公开的全部公墓基金名单
    ft : 'zq': 债券类型基金
        'gp': 股票类型基金
        'etf': ETF 基金
        'hh': 混合型基金
        'zs': 指数型基金
        'fof': FOF 基金
        'qdii': QDII 型基金
        `None` : 全部
    """
    params = [
        ('op', 'dy'),
        ('dt', 'kf'),
        ('rs', ''),
        ('gs', '0'),
        ('sc', 'qjzf'),
        ('st', 'desc'),
        ('es', '0'),
        ('qdii', ''),
        ('pi', '1'),
        ('pn', '50000'),
        ('dx', '0')]

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75',
        'Accept': '*/*',
        'Referer': 'http://fund.eastmoney.com/data/fundranking.html',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    }
    if ft is not None:
        params.append(('ft', ft))

    url = 'http://fund.eastmoney.com/data/rankhandler.aspx'
    response = requests.get(
        url,
        headers=headers,
        params=params)

    columns = ['基金代码', '基金简称']
    results = re.findall('"(\d{6}),(.*?),', response.text)
    df = pd.DataFrame(results, columns=columns)
    return df


def fund_position(code, n=1):
    '''code:基金代码，n:获取最近n期数据，n默认为1表示最近一期数据
    '''
    columns = {
        'GPDM': '股票代码',
        'GPJC': '股票简称',
        'JZBL': '持仓占比',
        'PCTNVCHG': '较上期变化',
    }
    df = pd.DataFrame(columns=columns.values())
    dates = fund_dates(code)[:n]
    dfs = []
    for date in dates:
        params = [
            ('FCODE', code),
            ('appType', 'ttjj'),
            ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
            ('plat', 'Iphone'),
            ('product', 'EFund'),
            ('serverVersion', '6.2.8'),
            ('version', '6.2.8'),
        ]
        if date is not None:
            params.append(('DATE', date))
        url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition'
        json_response = requests.get(url,
                                     headers=fund_header,
                                     params=params).json()
        stocks = jsonpath(json_response, '$..fundStocks[:]')
        if not stocks:
            continue
        date = json_response['Expansion']
        _df = pd.DataFrame(stocks)
        _df['公开日期'] = date
        _df.insert(0, '基金代码', code)
        dfs.append(_df)
    fields = ['基金代码'] + list(columns.values()) + ['公开日期']
    if not dfs:
        return pd.DataFrame(columns=fields)
    df = pd.concat(dfs, axis=0, ignore_index=True).rename(
        columns=columns)[fields]
    # 将object类型转为数值型
    ignore_cols = ['基金代码', '股票代码', '股票简称', '公开日期']
    df = trans_num(df, ignore_cols)
    return df


def fund_dates(code):
    """
    获取历史上更新持仓情况的日期列表
    code : 6 位基金代码
    """
    params = (
        ('FCODE', code),
        ('appVersion', '6.3.8'),
        ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('serverVersion', '6.3.6'),
        ('version', '6.3.8'),
    )
    url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNIVInfoMultiple'
    json_response = requests.get(
        url,
        headers=fund_header,
        params=params).json()
    if json_response['Datas'] is None:
        return []
    return json_response['Datas']


def fund_perfmance(code):
    """
    获取基金阶段涨跌幅度
    code : 6 位基金代码
    """
    params = (
        ('AppVersion', '6.3.8'),
        ('FCODE', code),
        ('MobileKey', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('OSVersion', '14.3'),
        ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('passportid', '3061335960830820'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('version', '6.3.6'),
    )
    url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNPeriodIncrease'
    json_response = requests.get(
        url,
        headers=fund_header,
        params=params).json()
    columns = {
        'syl': '收益率',
        'avg': '同类平均',
        'rank': '同类排行',
        'sc': '同类总数',
        'title': '时间段'}
    titles = {'Z': '近一周',
              'Y': '近一月',
              '3Y': '近三月',
              '6Y': '近六月',
              '1N': '近一年',
              '2Y': '近两年',
              '3N': '近三年',
              '5N': '近五年',
              'JN': '今年以来',
              'LN': '成立以来'}
    df = pd.DataFrame(json_response['Datas'])
    df = df[list(columns.keys())].rename(columns=columns)
    df['时间段'] = titles.values()
    df.insert(0, '基金代码', code)
    # 将object类型转为数值型
    ignore_cols = ['基金代码', '时间段']
    df = trans_num(df, ignore_cols)
    return df


def fund_base_info(code):
    """
    获取基金的一些基本信息
    code : 6 位基金代码
    """
    params = (
        ('FCODE', code),
        ('deviceid', '3EA024C2-7F22-408B-95E4-383D38160FB3'),
        ('plat', 'Iphone'),
        ('product', 'EFund'),
        ('version', '6.3.8'),
    )
    url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNNBasicInformation'
    json_response = requests.get(
        url,
        headers=fund_header,
        params=params).json()
    columns = {
        'FCODE': '基金代码',
        'SHORTNAME': '基金简称',
        'ESTABDATE': '成立日期',
        'RZDF': '涨跌幅',
        'DWJZ': '最新净值',
        'JJGS': '基金公司',
        'FSRQ': '净值更新日期',
        'COMMENTS': '简介',
    }
    items = json_response['Datas']
    if not items:
        return pd.Series(index=columns.values())

    ss = pd.Series(json_response['Datas']).rename(
        index=columns)[columns.values()]

    ss = ss.apply(lambda x: x.replace('\n', ' ').strip()
                  if isinstance(x, str) else x)
    return ss


def fund_info(code_list=None, ft='gp'):
    """
    获取基金基本信息
    code:可以输入单只基金代码或多只基金的list
    """
    if code_list is None:
        code_list = list(fund_code(ft)['基金代码'])
    if isinstance(code_list, str):
        code_list = [code_list]
    ss = []

    @multitasking.task
    @func_set_timeout(5)
    def start(code):
        s = fund_base_info(code)
        ss.append(s)
        pbar.update()

    pbar = tqdm(total=len(code_list),leave=False)
    for c in code_list:
        start(c)
    multitasking.wait_for_tasks()
    df = pd.DataFrame(ss)
    return df


#############################################################################
####债券bond
# 债券基本信息表头
bond_info_field = {
    'SECURITY_CODE': '债券代码',
    'SECURITY_NAME_ABBR': '债券名称',
    'CONVERT_STOCK_CODE': '正股代码',
    'SECURITY_SHORT_NAME': '正股名称',
    'RATING': '债券评级',
    'PUBLIC_START_DATE': '申购日期',
    'ACTUAL_ISSUE_SCALE': '发行规模(亿)',
    'ONLINE_GENERAL_LWR': '网上发行中签率(%)',
    'LISTING_DATE': '上市日期',
    'EXPIRE_DATE': '到期日期',
    'BOND_EXPIRE': '期限(年)',
    'INTEREST_RATE_EXPLAIN': '利率说明'}


def bond_info_single(code):
    """
    获取单只债券基本信息
    code:债券代码
    """
    columns = bond_info_field
    params = (
        ('reportName', 'RPT_BOND_CB_LIST'),
        ('columns', 'ALL'),
        ('source', 'WEB'),
        ('client', 'WEB'),
        ('filter', f'(SECURITY_CODE="{code}")'),
    )

    url = 'http://datacenter-web.eastmoney.com/api/data/v1/get'
    json_response = requests.get(url,
                                 headers=request_header,
                                 params=params).json()
    if json_response['result'] is None:
        return pd.Series(index=columns.values(), dtype='object')
    items = json_response['result']['data']
    s = pd.Series(items[0]).rename(index=columns)
    s = s[columns.values()]
    return s


def bond_info_all():
    """
    获取全部债券基本信息列表
    """
    page = 1
    dfs = []
    columns = bond_info_field
    while 1:
        params = (
            ('sortColumns', 'PUBLIC_START_DATE'),
            ('sortTypes', '-1'),
            ('pageSize', '500'),
            ('pageNumber', f'{page}'),
            ('reportName', 'RPT_BOND_CB_LIST'),
            ('columns', 'ALL'),
            ('source', 'WEB'),
            ('client', 'WEB'),
        )

        url = 'http://datacenter-web.eastmoney.com/api/data/v1/get'
        json_response = requests.get(url,
                                     headers=request_header,
                                     params=params).json()
        if json_response['result'] is None:
            break
        data = json_response['result']['data']
        df = pd.DataFrame(data).rename(
            columns=columns)[columns.values()]
        dfs.append(df)
        page += 1

    df = pd.concat(dfs, ignore_index=True)
    return df


def bond_info(code_list=None):
    """
    获取单只或多只债券基本信息
    code_list : 债券代码列表
    """
    if code_list is None:
        return bond_info_all()
    if isinstance(code_list, str):
        code_list = [code_list]
    ss = []

    @multitasking.task
    @func_set_timeout(5)
    def run(code):
        s = bond_info_single(code)
        ss.append(s)
        pbar.update()

    pbar = tqdm(total=len(code_list),leave=False)
    for code in code_list:
        run(code)
    multitasking.wait_for_tasks()
    df = pd.DataFrame(ss)
    return df


####可转债历史K线和实时交易数据可通过统一接口get_k_data和intraday_data获取

#########################################################################
####期货future
def future_info():
    '''返回期货'代码', '名称', '涨幅', '最新','ID','市场','时间'
    '''
    df = market_realtime('future')
    cols = ['代码', '名称', '涨幅', '最新', 'ID', '市场', '时间']
    return df[cols]

####期货历史K线和实时交易数据可通过统一接口get_k_data和intraday_data获取
