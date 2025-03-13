# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 10:53:55 2022

@author: Jinyi Zhang
"""
import requests
from tqdm import tqdm
import pandas as pd

from bs4 import BeautifulSoup
from qstock.data.trade import latest_trade_date
from qstock.data.util import trans_num,ths_header

trade_date=latest_trade_date().replace('-','')
###################
#同花顺股票池
def ths_pool(ta=None):
    '''ta：输入技术形态选股
    可选："创月新高", "半年新高", "一年新高", "历史新高"，
     "创月新低", "半年新低", "一年新低", "历史新低"，'险资举牌'
     '连续上涨','连续下跌'，'持续放量'，'持续缩量'，'量价齐升'，
     '量价齐跌'，'强势股',f'u{n}',f'd{n}',n=
     10、20、30、60、90、250、500，突破n日均线
     如'u20'代表向上突破20日均线，'d10'：跌破10日均线
     
    '''
    u={"历史新高":1,"一年新高":2,"半年新高":3,"创月新高":4}
    d={"历史新低":1,"一年新低":2,"半年新低":3,"创月新低":4}
    ns=[10,20,30,60,90,250,500]
    uns=['u'+str(n) for n in ns]
    dns=['d'+str(n) for n in ns]
    uns_dict=dict(zip(uns,ns))
    dns_dict=dict(zip(dns,ns))
    
    if ta in ['ljqs','量价齐升']:
        return ths_price_vol('ljqs')
    elif ta in ['ljqd','量价齐跌']:
        return ths_price_vol('ljqd')
    elif ta in ['xzjp' ,'险资举牌']:
        return ths_xzjp()
    elif ta in ['lxsz','连续上涨']:
        return ths_up_down(flag='lxsz')
    elif ta in ['lxxd','连续下跌']:
        return ths_up_down(flag='lxxd')
    elif ta in ['cxfl','持续放量']:
        return ths_vol_change(flag='cxfl')
    elif ta in ['cxsl' ,'持续缩量']:
        return ths_vol_change(flag='cxsl')
   
    elif ta in u.keys():
        return ths_break_price(flag= "cxg",n=u[ta])
    elif ta in d.keys():
        return ths_break_price(flag= "cxd",n=d[ta])
    
    elif ta in uns:
        return ths_break_ma(flag='xstp',n=uns_dict[ta])
    elif ta in dns:
        return ths_break_ma(flag='xxtp',n=dns_dict[ta])
    else:
        return stock_strong_pool()

#获取东方财富网涨停（跌停）板股票池
def limit_pool(flag='u',date=None):
    '''date：日期如'20220916'
    flag='u'代表涨停板，'d'代表跌停，'s'代表强势股
    默认为最新交易日'''
    if date is None:
        date=trade_date
    if flag=='u' or flag=='up' or flag=='涨停':
        return stock_zt_pool(date)
    elif flag=='d' or flag=='down' or flag=='跌停':
        return stock_dt_pool(date)
    else:
        return stock_strong_pool(date)

#涨停股池

def stock_zt_pool(date= None) :
    """
    获取东方财富网涨停板行情
    date:日期
    """
    if date is None:
        date=trade_date
    url = 'http://push2ex.eastmoney.com/getTopicZTPool'
    params = {
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'dpt': 'wz.ztzt',
        'Pageindex': '0',
        'pagesize': '10000',
        'sort': 'fbt:asc',
        'date': date,
        '_': '1621590489736',
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if data_json['data'] is None:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json['data']['pool'])
    temp_df.reset_index(inplace=True)
    temp_df['index'] = range(1, len(temp_df)+1)
    old_cols=['序号','代码','_','名称','最新价','涨跌幅','成交额(百万)','流通市值(百万)',
        '总市值(百万)', '换手率','连板数','首次封板时间','最后封板时间',
        '封板资金(百万)','炸板次数','所属行业','涨停统计',]
    temp_df.columns =  old_cols
    temp_df['涨停统计'] = (temp_df['涨停统计'].apply(lambda x: dict(x)['days']
                ).astype(str) + "/" + temp_df['涨停统计']
               .apply(lambda x: dict(x)['ct']).astype(str))
    new_cols=['代码','名称','涨跌幅','最新价','换手率','成交额(百万)','流通市值(百万)',
        '总市值(百万)','封板资金(百万)','首次封板时间','最后封板时间','炸板次数',
        '涨停统计','连板数','所属行业',]
    df = temp_df[new_cols].copy()
    df['首次封板时间'] = df['首次封板时间'].apply(lambda s:str(s)[-6:-4]+':'+str(s)[-4:-2])
    df['最后封板时间'] = df['最后封板时间'].apply(lambda s:str(s)[-6:-4]+':'+str(s)[-4:-2])
    df['最新价'] = df['最新价'] / 1000
   
    # 将object类型转为数值型
    ignore_cols = ['代码','名称','最新价','首次封板时间','最后封板时间','涨停统计','所属行业',]
    df = trans_num(df, ignore_cols)
    df[['成交额(百万)','流通市值(百万)','总市值(百万)','封板资金(百万)']]=(df[['成交额(百万)',
        '流通市值(百万)','总市值(百万)','封板资金(百万)']]/1000000)
    return df.round(3)

#跌停股池

def stock_dt_pool(date = None):
    """
    获取东方财富网跌停股池
    http://quote.eastmoney.com/ztb/detail#type=dtgc
    date: 交易日
    """
    if date is None:
        date=trade_date
    url = 'http://push2ex.eastmoney.com/getTopicDTPool'
    params = {
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'dpt': 'wz.ztzt',
        'Pageindex': '0',
        'pagesize': '10000',
        'sort': 'fund:asc',
        'date': date,
        '_': '1621590489736',
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    if data_json['data'] is None:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json['data']['pool'])
  
    old_cols=['代码','_','名称','最新价','涨跌幅','成交额(百万)','流通市值(百万)',
        '总市值(百万)','动态市盈率','换手率','封板资金(百万)','最后封板时间','板上成交额',
        '连续跌停','开板次数','所属行业',]
    temp_df.columns = old_cols
    new_cols=['代码','名称','涨跌幅','最新价','换手率','最后封板时间',
        '连续跌停','开板次数','所属行业','成交额(百万)','封板资金(百万)','流通市值(百万)',
        '总市值(百万)']
    df = temp_df[new_cols].copy()
    df['最新价'] = df['最新价'] / 1000
    df['最后封板时间'] = df['最后封板时间'].apply(lambda s:str(s)[-6:-4]+':'+str(s)[-4:-2])
    # 将object类型转为数值型
    ignore_cols = ['代码','名称','最新价','最后封板时间','所属行业',]
    df = trans_num(df, ignore_cols)
    df[['成交额(百万)','流通市值(百万)','总市值(百万)','封板资金(百万)']]=(df[['成交额(百万)',
        '流通市值(百万)','总市值(百万)','封板资金(百万)']]/1000000)
    return df.round(3)

def stock_strong_pool(date= None) :
    """
    获取东方财富网强势股池
    date：日期
    """
    if date is None:
        date=trade_date
    url = 'http://push2ex.eastmoney.com/getTopicQSPool'
    params = {
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'dpt': 'wz.ztzt',
        'Pageindex': '0',
        'pagesize': '170',
        'sort': 'zdp:desc',
        'date': date,
        '_': '1621590489736',
    }
    res = requests.get(url, params=params)
    data_json = res.json()
    if data_json['data'] is None:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json['data']['pool'])
   
    old_cols=['代码','_','名称','最新价','涨停价','_','涨跌幅',
        '成交额(百万)','流通市值(百万)','总市值(百万)', '换手率','是否新高','入选理由',
        '量比','涨速','涨停统计','所属行业',]
    temp_df.columns = old_cols
    temp_df['涨停统计'] = temp_df['涨停统计'].apply(lambda x: dict(x)['days']).astype(str) + "/" + temp_df['涨停统计'].apply(lambda x: dict(x)['ct']).astype(str)
    new_cols=['代码','名称','涨跌幅','最新价','涨停价','换手率','涨速','是否新高','量比',
              '涨停统计','入选理由','所属行业','成交额(百万)','流通市值(百万)','总市值(百万)', ]
    df = temp_df[new_cols].copy()
    df[['最新价','涨停价']] = df[['最新价','涨停价']] / 1000
    df[['成交额(百万)','流通市值(百万)','总市值(百万)']]=(df[['成交额(百万)',
        '流通市值(百万)','总市值(百万)']]/1000000)
    rr={1: '60日新高', 2: '近期多次涨停', 3: '60日新高且近期多次涨停'}
    df['入选理由']=df['入选理由'].apply(lambda s: rr[s])
    return df.round(2)

#############################################################################

url0='http://data.10jqka.com.cn/rank/'


def fetch_ths_data(url):
    res = requests.get(url, headers=ths_header())
    soup = BeautifulSoup(res.text, "lxml")
    try:
        total_page = soup.find(
            "span", attrs={"class": "page_info"}
        ).text.split("/")[1]
    except:
        total_page = 1
    df = pd.DataFrame()
    for page in tqdm(range(1, int(total_page) + 1), leave=False):
        r = requests.get(url, headers=ths_header())
        temp_df = pd.read_html(r.text)[0]
        df = pd.concat([df, temp_df], ignore_index=True)
    return df


def ths_break_price(flag= "cxg",n=1):
    """
    获取同花顺技术选股-创新高个股
    flag: 'cxg'：创新高，'cxd'：创新低
    n=1,2,3,4,分别对应：历史新高（低）、一年新高（低）、半年新高（低）、创月新高（低）
    """
    page=1
    url = url0+f"{flag}/board/{n}/field/stockcode/order/asc/page/{page}/ajax/1/free/1/"
    df=fetch_ths_data(url)
        
    df.columns = ["序号","股票代码","股票简称","涨跌幅","换手率","最新价",
        "前期点位","前期点位日期",]
    
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df["涨跌幅"] = df["涨跌幅"].str.strip("%")
    df["换手率"] = df["换手率"].str.strip("%")
    df["前期点位日期"] = pd.to_datetime(df["前期点位日期"]).dt.date
    # 将object类型转为数值型
    ignore_cols = ["股票代码","股票简称","前期点位日期"]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]


def ths_up_down(flag='lxsz'):
    """
    同花顺技术选股连续上涨
    flag:'lxsz':连续上涨;'lxxd':连续下跌
    """
    page=1
    url = url0+f"{flag}/field/lxts/order/desc/page/{page}/ajax/1/free/1/"
    df=fetch_ths_data(url)
    
    df.columns = ["序号", "股票代码","股票简称","收盘价","最高价",
        "最低价","连涨天数","连续涨跌幅","累计换手率","所属行业",]
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df["连续涨跌幅"] = df["连续涨跌幅"].str.strip("%")
    df["累计换手率"] = df["累计换手率"].str.strip("%")
    # 将object类型转为数值型
    ignore_cols = ["股票代码","股票简称","所属行业"]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]

#同花顺技术选股持续放量（缩量）

def ths_vol_change(flag='cxfl'):
    """
    同花顺技术选股持续放量（缩量）
    flag='cxfl':持续放量;'cxsl':持续缩量
    """
    page=1
    url =url0+ f"{flag}/field/count/order/desc/ajax/1/free/1/page/{page}/free/1/"
    df=fetch_ths_data(url)
        
    df.columns = ["序号","股票代码","股票简称","涨跌幅","最新价","成交量",
        "基准日成交量","天数","阶段涨跌幅","所属行业",]
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df["涨跌幅"] = df["涨跌幅"].astype(str).str.strip("%")
    df["阶段涨跌幅"] = df["阶段涨跌幅"].astype(str).str.strip("%")
    ignore_cols = ["股票代码","股票简称","所属行业"]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]


def ths_break_ma(flag='xstp',n=20):
    """
    同花顺技术选股-向上（下）突破
    flag='xstp':向上突破；'xxtp':向下突破
    n:可选5、10、20、30、60、90、250、500，表示突破n日均线
    """
    page=1
    url =url0+ f"{flag}/board/{n}/order/asc/ajax/1/free/1/page/{page}/free/1/"
    df=fetch_ths_data(url)
        
    df.columns = ["序号","股票代码","股票简称","最新价","成交额","成交量(万)",
        "涨跌幅","换手率",]
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df[["涨跌幅","换手率"]] = df[["涨跌幅","换手率"]].apply(lambda s:s.astype(str).str.strip("%"))
    df['成交量(万)']=df['成交量(万)'].astype(str).str.strip("万")
    del df['成交额']
    ignore_cols = ["股票代码","股票简称"]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]

def ths_price_vol(flag='ljqs'):
    """
    返回同花顺技术选股-量价齐升（齐跌）
    flag: 'ljqs："量价齐升，'ljqd："量价齐跌
    """
    page=1
    url = url0+f"{flag}/field/count/order/desc/ajax/1/free/1/page/{page}/free/1/"
    df=fetch_ths_data(url)
        
    df.columns = ["序号","股票代码","股票简称","最新价",
        "天数","阶段涨幅","累计换手率","所属行业",]
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df["阶段涨幅"] = df["阶段涨幅"].astype(str).str.strip("%")
    df["累计换手率"] = df["累计换手率"].astype(str).str.strip("%")
    ignore_cols = ["股票代码","股票简称","所属行业",]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]

def ths_xzjp():
    """
    同花顺技术选股-险资举牌
    """
    url = "http://data.10jqka.com.cn/ajax/xzjp/field/DECLAREDATE/order/desc/ajax/1/free/1/"
    df=fetch_ths_data(url)
        
    df.columns = ["序号","举牌公告日","股票代码","股票简称","现价","涨跌幅",
        "举牌方","增持数量(万)","交易均价","增持数量占总股本比例","变动后持股总数(万)",
        "变动后持股比例","历史数据",]
    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    df["涨跌幅"] = df["涨跌幅"].astype(str).str.zfill(6)
    df["增持数量占总股本比例"] = df["增持数量占总股本比例"].astype(str).str.strip("%")
    df["增持数量(万)"] = df["增持数量(万)"].astype(str).str.strip("万").astype(float)
    df["变动后持股总数(万)"] = df["变动后持股总数(万)"].apply(lambda s: float(s.strip("亿"))*10000 
                                if s.endswith('亿') else float(s.strip("万")) )
    df["变动后持股比例"] = df["变动后持股比例"].astype(str).str.strip("%")
    
    df["举牌公告日"] = pd.to_datetime(df["举牌公告日"]).dt.date
    del df["历史数据"]
    ignore_cols = ["股票代码","股票简称","举牌方","举牌公告日"]
    df = trans_num(df, ignore_cols)
    return df.iloc[:,1:]
