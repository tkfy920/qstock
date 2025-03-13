# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 13:53:48 2022

@author: Jinyi Zhang
"""
import pandas as pd
import requests
import calendar
from datetime import datetime
from jsonpath import jsonpath
from tqdm import tqdm
from bs4 import BeautifulSoup

from qstock.data.trade import latest_report_date,market_realtime
from qstock.data.util import (trans_num,cn_headers,get_code_id,request_header, session,)

#########################################################################
# 股东变动情况
def stock_holder(holder=None,date=None, code=None, n=2):
    '''
    holder:股东类型：'实控人'，返回实控人持股变动情况，
           '股东'，返回股东持股变动情况，
           None,返回全市场个股股东增减持情况或某指定个股前十大股票变化情况
    date:日期，code:股票代码或简称
    默认参数下返回的是全市场个股最新日期的变动情况
    '''
    
    if holder in [1,'实控人','con','controller','control','实际控制人','skr']:
        return stock_holder_con()
    
    elif holder in [2,'股东','股东增减持','股东持股','gd']:
        return stock_holder_change()
    
    else:
        if code is None:
            # 获取市场数据
            if date is not None and '-' not in date:
                date = '-'.join([date[:4], date[4:6], date[6:]])
            df = stock_holder_num(date)
            if len(df) < 1:
                df = stock_holder_num(latest_report_date())
            return df
        else:
            return stock_holder_top10(code, n)


# 获取沪深市场某股票前十大股东信息
def stock_holder_top10(code, n=2):
    """
    获取沪深市场指定股票前十大股东信息
    code : 股票代码
    n :最新 n个季度前10大流通股东公开信息
    """
    fields = {
        'GuDongDaiMa': '股东代码',
        'GuDongMingCheng': '股东名称',
        'ChiGuShu': '持股数(亿)',
        'ChiGuBiLi': '持股比例(%)',
        'ZengJian': '增减',
        'BianDongBiLi': '变动率(%)'}

    code_id = get_code_id(code)
    mk = code_id.split('.')[0]
    stock_code = code_id.split('.')[1]
    fc = f'{stock_code}02' if mk == '0' else f'{stock_code}01'
    data0 = {"fc": fc}
    url0 = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetFirstRequest2Data'
    res = requests.post(url0, json=data0).json()
    dates = jsonpath(res, '$..BaoGaoQi')
    df_list = []

    for date in dates[:n]:
        data = {"fc": fc, "BaoGaoQi": date}
        url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetShiDaLiuTongGuDong'
        response = requests.post(url, json=data)
        response.encoding = 'utf-8'
        items = jsonpath(
            response.json(), '$..ShiDaLiuTongGuDongList[:]')
        if not items:
            continue
        df = pd.DataFrame(items)
        df.rename(columns=fields, inplace=True)
        df.insert(0, '代码', [stock_code for _ in range(len(df))])
        df.insert(1, '日期', [date for _ in range(len(df))])
        del df['IsLink']
        del df['股东代码']
        df_list.append(df)

    dff = pd.concat(df_list, axis=0, ignore_index=True)
    # 将object类型转为float
    trans_cols = ['持股数(亿)', '持股比例(%)', '变动率(%)']
    for col in trans_cols:
        dff[col] = dff[col].apply(lambda x: float(x[:-1]) if x[-1] in ['亿', '%'] else 0)
    return dff


# 获取沪深A股最新公开的股东数量
def stock_holder_num(date=None):
    """
    获取沪深A股市场公开的股东数目变化情况
    date : 默认最新的报告期,
    指定某季度如'2022-03-31','2022-06-30','2022-09-30','2022-12-31'
    """

    if date is not None and '-' not in date:
        date_trans = lambda s: '-'.join([s[:4], s[4:6], s[6:]])
        date = date_trans(date)

    dfs = []
    if date is not None:
        date= datetime.strptime(date, '%Y-%m-%d')
        year = date.year
        month = date.month
        if month % 3 != 0:
            month -= month % 3
        if month < 3:
            year -= 1
            month = 12
        _, last_day = calendar.monthrange(year, month)
        date: str = datetime.strptime(
            f'{year}-{month}-{last_day}', '%Y-%m-%d').strftime('%Y-%m-%d')
    page = 1
    fields = {
        'SECURITY_CODE': '代码',
        'SECURITY_NAME_ABBR': '名称',
        'END_DATE': '截止日',
        'HOLDER_NUM': '股东人数',
        'HOLDER_NUM_RATIO': '增减(%)',
        'HOLDER_NUM_CHANGE': '较上期变化',
        'AVG_MARKET_CAP': '户均持股市值',
        'AVG_HOLD_NUM': '户均持股数量',
    }

    while True:
        params = [
            ('sortColumns', 'HOLD_NOTICE_DATE,SECURITY_CODE'),
            ('sortTypes', '-1,-1'),
            ('pageSize', '500'),
            ('pageNumber', page),
            ('columns',
             'SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,END_DATE,PRE_END_DATE'),
            ('quoteColumns', 'f2,f3'),
            ('source', 'WEB'),
            ('client', 'WEB'),
        ]
        if date is not None:
            params.append(('filter', f'(END_DATE=\'{date}\')'))
            params.append(('reportName', 'RPT_HOLDERNUM_DET'))
        else:
            params.append(('reportName', 'RPT_HOLDERNUMLATEST'))

        params = tuple(params)
        url = 'http://datacenter-web.eastmoney.com/api/data/v1/get'
        response = session.get(url,
                               headers=request_header,
                               params=params)
        items = jsonpath(response.json(), '$..data[:]')
        if not items:
            break
        df = pd.DataFrame(items)
        df = df.rename(columns=fields)[fields.values()]
        page += 1
        dfs.append(df)
    if len(dfs) == 0:
        df = pd.DataFrame(columns=fields.values())
        return df
    df = pd.concat(dfs, ignore_index=True)
    df['截止日'] = pd.to_datetime(df['截止日']).apply(lambda x: x.strftime('%Y%m%d'))
    cols = ['代码', '名称', '截止日']
    df = trans_num(df, cols).round(2)
    return df

#实际控制人持股变动
def stock_holder_con():
    """
    巨潮资讯-数据中心-专题统计-股东股本-实际控制人持股变动
    http://webapi.cninfo.com.cn/#/thematicStatistics
    """
    url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1033"
    
    params = {"ctype": "",}
    r = requests.post(url, headers=cn_headers, params=params)
    data_json = r.json()
    df = pd.DataFrame(data_json["records"])
    old_cols=["控股比例","控股数量","简称","实控人",
        "直接控制人","控制类型","代码","变动日期",]
    
    new_cols=["变动日期","代码","简称","控股比例","控股数量","实控人",
        "直接控制人","控制类型"]
    df.columns = old_cols
    df = df[new_cols]
    df["变动日期"] = pd.to_datetime(df["变动日期"]).dt.date
    df[["控股数量","控股比例"]] =df[["控股数量","控股比例"]].apply(lambda s:pd.to_numeric(s))
    return df

#大股东增减持变动明细
def stock_holder_change():
    """
    获取大股东增减持变动明细
    """
    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    params = {
        'sortColumns': 'END_DATE,SECURITY_CODE,EITIME',
        'sortTypes': '-1,-1,-1',
        'pageSize': '500',
        'pageNumber': '1',
        'reportName': 'RPT_SHARE_HOLDER_INCREASE',
        'quoteColumns': 'f2~01~SECURITY_CODE~NEWEST_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE_QUOTES',
        'columns': 'ALL',
        'source': 'WEB',
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json['result']['pages']
    df = pd.DataFrame()
    for page in tqdm(range(1, total_page+1), leave=False):
        params.update({
            'pageNumber': page,
        })
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json['result']['data'])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.columns = ['变动数量','公告日','代码','股东名称',
        '变动占总股本比例','_','-','截止日','-',
        '变动后持股总数','变动后占总股本比例','_',
        '变动后占流通股比例','变动后持流通股数','_',
        '名称','增减','_','变动占流通股比例','开始日',
        '_', '最新价','涨跌幅','_',]

    df = df[['代码','名称','最新价','涨跌幅','股东名称','增减',
        '变动数量','变动占总股本比例','变动占流通股比例',
        '变动后持股总数','变动后占总股本比例','变动后持流通股数',
        '变动后占流通股比例','开始日','截止日','公告日',]]
    df['开始日'] = pd.to_datetime(df['开始日']).dt.date
    df['截止日'] = pd.to_datetime(df['截止日']).dt.date
    df['公告日'] = pd.to_datetime(df['公告日']).dt.date
   
    cols = ['代码', '名称', '股东名称','增减','开始日','截止日','公告日',]
    df = trans_num(df, cols).round(2)
    return df

##########################################################################
##机构持股
def institute_hold(quarter = "20221"):
    """
    获取新浪财经机构持股一览表
    http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/jgcg/index.phtml
    quarter: 如'20221表示2022年一季度，
    其中的 1 表示一季报; "20193", 其中的 3 表示三季报;
    """
    url = "http://vip.stock.finance.sina.com.cn/q/go.php/vComStockHold/kind/jgcg/index.phtml?symbol=%D6%A4%C8%AF%BC%F2%B3%C6%BB%F2%B4%FA%C2%EB"
    params = {
        "p": "1",
        "num": "5000",
        "reportdate": quarter[:-1],
        "quarter": quarter[-1],
    }
    res = requests.get(url, params=params)
    df = pd.read_html(res.text)[0]
    df["证券代码"] = df["证券代码"].astype(str).str.zfill(6)
    del df["明细"]
    df.columns = ['证券代码', '简称', '机构数', '机构数变化', '持股比例', '持股比例增幅', '占流通股比例', '占流通股比例增幅']
    return df

##########################################################################
#主营业务构成
def main_business(code= "000001"):
    """
    获取公司主营业务构成
    http://f10.emoney.cn/f10/zygc/1000001
    code: 股票代码或股票简称
    """
    if not code.isdigit():
        code=stock_code_dict()[code]
    url = f"http://f10.emoney.cn/f10/zygc/{code}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "lxml")
    year_list = [item.text.strip()
        for item in soup.find(attrs={"class": "swlab_t"}).find_all("li")]

    df = pd.DataFrame()
    
    for i, item in enumerate(year_list, 2):
        temp_df = pd.read_html(res.text, header=0)[i]
        temp_df.columns = [
            "分类方向",
            "分类",
            "营业收入(万)",
            "营收同比",
            "占主营收入比",
            "营业成本(万)",
            "营业成本同比",
            "占主营成本比",
            "毛利率",
            "毛利率同比",
        ]
        temp_df["报告期"] = item
        df = pd.concat([df, temp_df], ignore_index=True)

    df = df[
       [
            "报告期",
            "分类方向",
            "分类",
            "营业收入(万)",
            "营收同比",
            "占主营收入比",
            "营业成本(万)",
            "营业成本同比",
            "占主营成本比",
            "毛利率",
            "毛利率同比",
        ]
    ]
    df[['营业收入(万)','营业成本(万)']]=df[['营业收入(万)','营业成本(万)']
                                 ].apply(lambda s:s.str.strip('亿'))
    cols= ["营收同比",
            "占主营收入比",
            "营业成本同比",
            "占主营成本比",
            "毛利率",
            "毛利率同比",]
    df[cols]=df[cols].apply(lambda s:s.str.strip('%'))
    ignore_cols = ["报告期","分类方向","分类",]
    df = trans_num(df, ignore_cols)
    df[['营业收入(万)','营业成本(万)']]=df[['营业收入(万)','营业成本(万)']]*10000
    return df
    


##财务报表和业绩指标
def financial_statement(flag='业绩报表',date=None):
    '''flag:报表类型,默认输出业绩报表，注意flag或date输出也默认输出业绩报表
    '业绩报表'或'yjbb'：返回年报季报财务指标
    '业绩快报'或'yjkb'：返回市场最新业绩快报
    '业绩预告'或'yjyg'：返回市场最新业绩预告
    '资产负债表'或'zcfz'：返回最新资产负债指标
    '利润表'或'lrb'：返回最新利润表指标
    '现金流量表'或'xjll'：返回最新现金流量表指标
    date:报表日期，如‘20220630’，‘20220331’，默认当前最新季报（或半年报或年报）
    '''
    date=trans_date(date)
    if flag in ['业绩快报','yjkb']:
        return stock_yjkb(date)
    
    elif flag in ['业绩预告','yjyg']:
        return stock_yjyg(date)
    
    elif flag in ['资产负债表','资产负债','zcfz','zcfzb']:
        return balance_sheet(date)
    
    elif flag in ['利润表','利润' ,'lr' ,'lrb']:
        return income_statement(date)
    
    elif flag in ['现金流量表','现金流量' ,'xjll' ,'xjllb']:
        return cashflow_statement(date)
    
    else:
        return stock_yjbb(date)

#资产负债表
def trans_date(date=None):
    '''将日期格式'2022-09-30'转为'20220930'
    '''
    if date is None:
        date=latest_report_date()
    date=''.join(date.split('-'))
    return date
    
def balance_sheet(date= None):
    """
    东方财富年报季报资产负债表
    http://data.eastmoney.com/bbsj/202003/zcfz.html
    date:如"20220331", "20220630",
    """
    date=trans_date(date)
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_DMSK_FN_BALANCE",
        "columns": "ALL",
        "filter": f"""(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CODE!="069001017")(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')""",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = df.index + 1
    df.columns = ["序号", "_","代码","_","_","简称", "_","_","_","_", "_","_",
        "_","公告日","_","总资产","_","货币资金","_","应收账款","_","存货","_",
        "总负债","应付账款","_","预收账款","_","股东权益","_","总资产同比","总负债同比",
        "_","资产负债率","_","_","_","_","_","_","_","_","_","_","_","_","_","_",
        "_","_","_","_", "_","_","_","_","_","_",]
    df = df[["代码", "简称","货币资金","应收账款","存货","总资产", "总资产同比",
            "应付账款","预收账款","总负债","总负债同比","资产负债率","股东权益","公告日",]]
    df["公告日"] = pd.to_datetime(df["公告日"]).dt.date
    cols = ['代码', '简称', '公告日',]
    df = trans_num(df, cols).round(2)
    return df

def income_statement(date = None):
    """
    获取东方财富年报季报-利润表
    http://data.eastmoney.com/bbsj/202003/lrb.html
    date: 如"20220331", "20220630"
    """
    date=trans_date(date)
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_DMSK_FN_INCOME",
        "columns": "ALL",
        "filter": f"""(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CODE!="069001017")(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')""",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = df.index + 1
    df.columns = ["序号","_","代码","_","_","简称","_","_","_","_","_","_",
        "_","公告日", "_","净利润","营业总收入","营业总支出","_","营业支出",
        "_","_","销售费用","管理费用","财务费用","营业利润","利润总额",
        "_","_","_", "_","_","_","_","_","_", "_","_","_","_","_","_",
        "营业总收入同比","_","净利润同比", "_","_",]
    
    df = df[["代码","简称","净利润","净利润同比","营业总收入","营业总收入同比",
            "营业支出","销售费用","管理费用","财务费用","营业总支出",
            "营业利润","利润总额","公告日",]]
    
    df["公告日"] = pd.to_datetime(df["公告日"]).dt.date
    cols = ['代码', '简称', '公告日',]
    df = trans_num(df, cols).round(2)
    return df


def cashflow_statement(date= None):
    """
    获取东方财富年报季报现金流量表
    http://data.eastmoney.com/bbsj/202003/xjll.html
    date: 如"20220331", "20220630"
    """
    date=trans_date(date)
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_DMSK_FN_CASHFLOW",
        "columns": "ALL",
        "filter": f"""(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CODE!="069001017")(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')""",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = df.index + 1
    df.columns = ["序号","_","代码","_","_","简称","_","_","_","_","_","_","_",
        "公告日","_","经营性现金流量净额","经营性净现金流占比","_","_","_","_",
        "投资性现金流量净额","投资性净现金流占比","_","_","_","_",
        "融资性现金流量净额","融资性净现金流占比","净现金流",
        "净现金流同比增长","_","_","_","_","_","_","_","_","_","_","_","_",
        "_","_", "_","_","_","_",]
    
    df = df[["代码","简称","净现金流","净现金流同比增长","经营性现金流量净额",
            "经营性净现金流占比","投资性现金流量净额", "投资性净现金流占比",
            "融资性现金流量净额","融资性净现金流占比","公告日",]]

    df["公告日"] = pd.to_datetime(df["公告日"]).dt.date
    cols = ['代码', '简称', '公告日',]
    df = trans_num(df, cols).round(2)
    return df

def stock_yjkb(date= None):
    """
    获取东方财富年报季报-业绩快报
    http://data.eastmoney.com/bbsj/202003/yjkb.html
    date: 如"20220331", "20220630"
    """
    date=trans_date(date)
    url = "http://datacenter.eastmoney.com/api/data/get"
    params = {
        "st": "UPDATE_DATE,SECURITY_CODE",
        "sr": "-1,-1",
        "ps": "5000",
        "p": "1",
        "type": "RPT_FCI_PERFORMANCEE",
        "sty": "ALL",
        "token": "894050c76af8597a853f5b408b759f5d",
        "filter": f"(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    old_cols=["序号","代码","简称","板块", "_","类型","_","公告日","_",
            "每股收益","营业收入","营业收入去年同期","净利润","净利润去年同期",
            "每股净资产","净资产收益率","营业收入同比","净利润同比",
            "营业收入季度环比","净利润季度环比","行业","_","_","_","_","_",
            "_","_","_",]
    new_cols= ["代码","简称","每股收益","营业收入", "营业收入去年同期", 
                   "营业收入同比","营业收入季度环比","净利润","净利润去年同期", 
                   "净利润同比","净利润季度环比","每股净资产","净资产收益率",
                   "行业","公告日","板块","类型",]
    
    if page_num > 1:
        df = pd.DataFrame()
        for page in tqdm(range(1, page_num + 1), leave=True):
            params = {
                "st": "UPDATE_DATE,SECURITY_CODE",
                "sr": "-1,-1",
                "ps": "5000",
                "p": page,
                "type": "RPT_FCI_PERFORMANCEE",
                "sty": "ALL",
                "token": "894050c76af8597a853f5b408b759f5d",
                "filter": f"(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
            }
            r = requests.get(url, params=params)
            data_json = r.json()
            temp_df = pd.DataFrame(data_json["result"]["data"])
            temp_df.reset_index(inplace=True)
            temp_df["index"] = range(1, len(temp_df) + 1)
            df = pd.concat([df, temp_df], ignore_index=True)
        
        df.columns = old_cols
        df = df[new_cols]
        return df
    df2 = pd.DataFrame(data_json["result"]["data"])
    df2.reset_index(inplace=True)
    df2["index"] = range(1, len(df2) + 1)
    df2.columns = old_cols
    df2 = df2[new_cols]
    return df2


def stock_yjyg(date = None) :
    """
    东方财富业绩预告
    date: 如"20220331", "20220630"
    """
    date=trans_date(date)
    url = "http://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "50",
        "pageNumber": "1",
        "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
        "columns": "ALL",
        "token": "894050c76af8597a853f5b408b759f5d",
        "filter": f" (REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    df = pd.DataFrame()
    total_page = data_json["result"]["pages"]
    for page in tqdm(range(1, total_page + 1), leave=False):
        params = {
            "sortColumns": "NOTICE_DATE,SECURITY_CODE",
            "sortTypes": "-1,-1",
            "pageSize": "50",
            "pageNumber": page,
            "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
            "columns": "ALL",
            "token": "894050c76af8597a853f5b408b759f5d",
            "filter": f" (REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    
    old_cols=['SECURITY_CODE', 'SECURITY_NAME_ABBR','NOTICE_DATE', 'REPORT_DATE', 
              'PREDICT_FINANCE','PREDICT_CONTENT', 'CHANGE_REASON_EXPLAIN','PREDICT_TYPE',
              'PREYEAR_SAME_PERIOD', 'INCREASE_JZ', 'FORECAST_JZ']
    
    new_cols=["代码","简称","预测指标","业绩变动","预测数值","变动幅度",
            "变动原因","预告类型","上年同期","公告日"]
    df=df.rename(columns=dict(zip(old_cols,new_cols)))[new_cols]
    
    return df


def stock_yjbb(date= "20200331"):
    """
    东方财富年报季报业绩报表
    http://data.eastmoney.com/bbsj/202003/yjbb.html
    date: 如"20220331", "20220630"
    """
    date=trans_date(date)
    url = "http://datacenter.eastmoney.com/api/data/get"
    params = {
        "st": "UPDATE_DATE,SECURITY_CODE",
        "sr": "-1,-1",
        "ps": "5000",
        "p": "1",
        "type": "RPT_LICO_FN_CPD",
        "sty": "ALL",
        "token": "894050c76af8597a853f5b408b759f5d",
        "filter": f"(REPORTDATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    page_num = data_json["result"]["pages"]
    df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params = {
            "st": "UPDATE_DATE,SECURITY_CODE",
            "sr": "-1,-1",
            "ps": "500",
            "p": page,
            "type": "RPT_LICO_FN_CPD",
            "sty": "ALL",
            "token": "894050c76af8597a853f5b408b759f5d",
            "filter": f"(REPORTDATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df], ignore_index=True)

    df.reset_index(inplace=True)
    df["index"] = range(1, len(df) + 1)
    df.columns = ["序号", "代码","简称","_","_","_","_","最新公告日","_","每股收益",
        "_","营业收入","净利润","净资产收益率","营业收入同比","净利润同比","每股净资产",
        "每股经营现金流量","销售毛利率","营业收入季度环比","净利润季度环比",
        "_","_","行业","_","_","_","_","_","_","_","_","_","_","_",]
    
    df = df[["代码","简称","每股收益","营业收入","营业收入同比","营业收入季度环比",
            "净利润","净利润同比","净利润季度环比","每股净资产","净资产收益率",
            "每股经营现金流量","销售毛利率","行业", "最新公告日",]]
    return df

###############################################################################
#个股股票基本面数据
#个股财务指标数据
def stock_indicator(code):
    """
    获取个股历史报告期所有财务分析指标
    https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/600004/ctrl/2019/displaytype/4.phtml
    code: 股票代码或简称
    """
    if not code.isdigit():
        code=stock_code_dict()[code]
    
    url = f"https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/ctrl/2020/displaytype/4.phtml"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    year_context = soup.find(attrs={"id": "con02-1"}).find("table").find_all("a")
    year_list = [item.text for item in year_context]
    df = pd.DataFrame()
    for year in tqdm(year_list, leave=False):
        url = f"https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/ctrl/{year}/displaytype/4.phtml"
        r = requests.get(url)
        temp_df = pd.read_html(r.text)[12].iloc[:, :-1]
        temp_df.columns = temp_df.iloc[0, :]
        temp_df = temp_df.iloc[1:, :]
        df0 = pd.DataFrame()
        indicator_list = ["每股指标", "盈利能力", "成长能力", "营运能力", "偿债及资本结构", "现金流量", "其他指标"]
        for i in range(len(indicator_list)):
            if i == 6:
                inner_df = temp_df[
                    temp_df.loc[
                        temp_df.iloc[:, 0].str.find(indicator_list[i]) == 0, :
                    ].index[0] :
                ].T
            else:
                inner_df = temp_df[
                    temp_df.loc[temp_df.iloc[:, 0].str.find(indicator_list[i]) == 0, :]
                    .index[0] : temp_df.loc[
                        temp_df.iloc[:, 0].str.find(indicator_list[i + 1]) == 0, :
                    ]
                    .index[0]
                    - 1
                ].T
            inner_df = inner_df.reset_index(drop=True)
            df0 = pd.concat([df0, inner_df], axis=1)
        df0.columns = df0.iloc[0, :].tolist()
        df0 = df0.iloc[1:, :]
        df0.index = temp_df.columns.tolist()[1:]
        df = pd.concat([df, df0])

    df.dropna(inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns={'index': '日期'}, inplace=True)
    fields=['日期','摊薄每股收益(元)','每股净资产_调整后(元)','每股经营性现金流(元)',
            '每股资本公积金(元)','每股未分配利润(元)','总资产(元)','扣除非经常性损益后的净利润(元)',
            '主营业务利润率(%)','总资产净利润率(%)','销售净利率(%)','净资产报酬率(%)','资产报酬率(%)',
            '净资产收益率(%)','加权净资产收益率(%)','成本费用利润率(%)','主营业务成本率(%)',
            '应收账款周转率(次)','存货周转率(次)','固定资产周转率(次)','总资产周转率(次)',
            '流动资产周转率(次)','流动比率','速动比率','现金比率(%)','产权比率(%)','资产负债率(%)',
            '经营现金净流量对销售收入比率(%)','经营现金净流量与净利润的比率(%)','经营现金净流量对负债比率(%)',
            '主营业务收入增长率(%)','净利润增长率(%)','净资产增长率(%)','总资产增长率(%)']

    new_names=['日期','每股收益','调整每股净资产','每股现金流','每股公积金','每股未分配利润','总资产','扣非净利润',
          '主营利润率','总资产净利率','销售净利率','净资产报酬率','资产报酬率','净资产收益率','加权净资产收益率',
          '成本费用利润率','主营业务成本率','应收账款周转率','存货周转率','固定资产周转率','总资产周转率',
          '流动资产周转率','流动比率','速动比率','现金比率','产权比率','资产负债率','现金流销售比',
          '现金流净利润比','现金流负债比','主营收入增长率','净利润增长率','净资产增长率','总资产增长率']
    
    result=df[fields].rename(columns=dict(zip(fields,new_names)))
    return result

def stock_code_dict():
    df=market_realtime()
    name_code=dict(df[['名称','代码']].values)
    return name_code
