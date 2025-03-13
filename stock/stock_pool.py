# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 17:39:06 2019

@author: Jinyi Zhang
"""

import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from qstock.data.money import stock_money


#正常显示画图时出现的中文和负号
from pylab import mpl
mpl.rcParams['font.sans-serif']=['SimHei']
mpl.rcParams['axes.unicode_minus']=False

#计算数据在系列时间周期内的收益率
def ret_date(data,w_list=[1,5,20,60,120]):
    data=data.fillna(method='ffill')
    df=pd.DataFrame()
    for w in w_list:
        df[str(w)+'日收益率%']=(((data/data.shift(w)-1)*100)
                            .round(2)
                            .T
                            .iloc[:,-1])
    return df

#计算某期间动量排名
def ret_rank(data,w_list=[1,5,20,60,120],c=4):
    #c为w_list里第几个
    rets=ret_date(data,w_list)
    col=rets.columns[c]
    rank_ret=rets.sort_values(col,ascending=False)
    return rank_ret

#同花顺概念动量排名
def ret_top(ths_rets,n=10):
    ths_top=pd.DataFrame()
    for c in ths_rets.columns:
        ths_top[c]=ths_rets.sort_values(c,ascending=False)[:n].index
    return ths_top

#获取同花顺概念指数动量排名名称列表
def ret_top_list(ths_top):
    alist=ths_top.values.tolist()
    words=' '.join([' '.join(s) for s in alist])
    word_list=words.split(' ')
    w_set=set(word_list)
    w_data=[]
    for w in w_set:
       w_data.append([w,word_list.count(w)/len(word_list)])
    return w_data

#运用在同花顺板块概念选股
def date_ret(data,w_list=[1,5,20,60,120]):
    data=data.fillna(method='ffill')
    df=pd.DataFrame()
    for w in w_list:
        df[str(w)+'日收益率%']=(((data/data.shift(w)-1)*100)
                            .round(2)
                            .T
                            .iloc[:,-1])
    return df

#计算某期间动量排名
def stock_rets_rank(data,w_list=[1,5,20,60,120],c=4):
    #c为w_list里第几个
    rets=date_ret(data,w_list)
    col=rets.columns[c]
    rank_ret=rets.sort_values(col,ascending=False)
    return rank_ret

#同花顺概念动量排名
def ths_top(ths_rets,n=10):
    ths_top=pd.DataFrame()
    for c in ths_rets.columns:
        ths_top[c]=ths_rets.sort_values(c,ascending=False)[:n].index
    return ths_top

#获取同花顺概念指数动量排名名称列表
def ths_top_list(ths_top):
    alist=ths_top.values.tolist()
    words=' '.join([' '.join(s) for s in alist])
    word_list=words.split(' ')
    w_set=set(word_list)
    w_data=[]
    for w in w_set:
       w_data.append([w,word_list.count(w)/len(word_list)])
    return w_data

#rps选股
class RPS(object):
    def __init__(self,data,w_list=[5,20,60,120,250]):
        self.data=data.fillna(method='ffill')
        self.w_list=w_list
        
    def cal_rps(self,ser):
        df=pd.DataFrame(ser.sort_values(ascending=False))
        df['n']=range(1,len(df)+1)
        df['rps']=(1-df['n']/len(df))*100
        return df.rps
    
    def all_rps(self,w):
        ret=(self.data/self.data.shift(w)-1).iloc[w:].fillna(0)
        rps=ret.T.apply(self.cal_rps)
        return rps.T

    def date_rps(self):
        print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
        pass
        
    def plot_stock_rps(self,stock,n=120):
        print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
        pass
        
#量价选股（参考欧奈尔的带柄茶杯形态）
#筛选价格和成交量突破N日阈值的个股
def find_price_vol_stock(data,n=120,rr=0.5):
    print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
    pass

#MM趋势选股（Mark Minervini’s Trend Template）
#参考文章https://zhuanlan.zhihu.com/p/165379657
#股票价格高于150天均线和200天均线
#1、150日均线高于200日均线
#2、200日均线上升至少1个月
#3、50日均线高于150日均线和200日均线
#4、股票价格高于50日均线
#5、股票价格比52周低点高30%
#6、股票价格在52周高点的25%以内
#7、相对强弱指数(RS)大于等于70，这里的相对强弱指的是股票与大盘对比，RS = 股票1年收益率 / 基准指数1年收益率
#这里将第七条RS改为欧奈尔的相对强弱指标，与RPS选股结合选择大于90值的股票

def MM_trend(close,sma=50,mma=150,lma=200):
    """close: 股票收盘价
    """
    print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
    pass

def tscode(code):
    return code+'.SH'if code.startswith('6') else code+'.SZ'

#资金流选股
def moneyflow_stock(codes,w_list=[3,5,10,20,60]):
    print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
    pass


#计算营收和利润增长得分
def cal_yoy(y,a):
    '''y是总资产报酬率时a=5
    y是营业收入增长率时a=10
    y是净资产收益率是a=15
    y是营业利润增长率a=20
    '''
    try:
        return 5+ min(round(y-a),5) if y>=a else 5+ max(round(y-a),-5)
    except:
        return 0
    
def cal_exp(y,a):
    '''y是毛利率或期间费用率a=0.5
     y是存货周转率a=2
     y是每股经营性现金流a=4
    '''
    try:
       return 5+min(round(y)/a,5) if y>0 else max(round(y)/a,-5)+5
    except:
       return 0
        
def cal_roa(y):
    #总资产报酬率打分
    try:
        return min(round((y-5)/0.5),10) if y>=5 else max(round(y-5),0)
    except:
        return 0
    
def cal_pepb(y,a,b):
    '''y是市净率时，a=3,b=0.4
      y是动态市盈率相对盈利增长率时，a=1,b=0.1
    '''
    try:
        return 5-max(round((y-a)/b),-5) if y<=a else 5-min(round((y-a)/b),5)
    except:
        return 0

#财务指标打分系统
##获取个股财务指标（指定某几个指标）+pe市盈率+pb市净率
fields='ts_code,ann_date,end_date,tr_yoy,op_yoy,\
         grossprofit_margin,expense_of_sales,inv_turn,eps,\
         ocfps,roe_yearly,roa2_yearly,netprofit_yoy,update_flag'
#update_flag财务数据是否更新过
def indicator_score(tudata,code,pepb=True,fields=fields):
    df=tudata.get_stock_indicator(stock=code,pepb=pepb,fields=fields)
    data=df.copy()
    '''(1)营业收入增长率打分'''
    data['营收得分']=data['tr_yoy'].apply(lambda y:cal_yoy(y,a=10))
    '''(2)营业利润增长率打分'''
    data['利润得分']=data['op_yoy'].apply(lambda y:cal_yoy(y,a=20))
    '''(3)毛利率打分'''
    #计算最近季度毛利率-前三季度平均毛利率
    data['gpm']=data['grossprofit_margin']-data['grossprofit_margin'].rolling(3).mean()
    data['毛利得分']=data['gpm'].apply(lambda y:cal_exp(y,a=0.5))
    '''(4)期间费用率打分'''
    #最近季度期间费用率-前三季度平均期间费用率
    data['exp']=data['expense_of_sales']-data['expense_of_sales'].rolling(3).mean()
    data['费用得分']=data['exp'].apply(lambda y:cal_exp(y,a=0.5))
    '''(5)周转率打分'''
    #（最近季度存货周转率-前三季度平均存货周转率）/前三季度平均存货周转率*100
    data['inv']=(data['inv_turn']-data['inv_turn'].rolling(3).mean())*100/data['inv_turn'].rolling(3).mean()
    data['周转得分']=data['inv'].apply(lambda y:cal_exp(y,a=2))
    '''(6)每股经营现金流打分'''
    #（最近三季度每股经营性现金流之和-最近三季度每股收益之和）/最近三季度每股收益之和*100
    data['ocf']=(data['ocfps'].rolling(3).sum()-data['eps'].rolling(3).sum())*100/data['eps'].rolling(3).sum()
    data['现金得分']=data['ocf'].apply(lambda y:cal_exp(y,a=4))
    '''(7)净资产收益率打分'''
    data['净资产得分']=data['roe_yearly'].apply(lambda y:cal_yoy(y,a=15))
    '''(8)总资产收益率打分'''
    data['总资产得分']=data['roa2_yearly'].apply(cal_roa)
    '''(9)市净率打分'''
    data['市净率得分']=data['pb'].apply(lambda y:cal_pepb(y,a=3,b=0.4))
    '''(10)动态市盈率相对盈利增长率打分'''
    #动态市盈率相对盈利增长率
    data['peg']=data['pe_ttm']/data['netprofit_yoy'].rolling(3).mean()
    data['市盈率得分']=data['peg'].apply(lambda y:cal_pepb(y,a=1,b=0.1))
    #计算总得分
    cols=['营收得分','利润得分','费用得分','周转得分','现金得分','净资产得分','总资产得分']
    data['总分']=data[cols].sum(axis=1)
    return data[cols+['总分']]

#计算所有股票财务指标总分
def all_indicator_score(tudata,name_code_dict):
    
    df=pd.DataFrame()
    for name,code in tqdm(name_code_dict.items()):
        try:
            d1=indicator_score(tudata,code)['总分']
            d1=pd.DataFrame(d1).rename(columns={'总分':name})
            df=pd.concat([df,d1],axis=1)
            
        except:
            continue
    return df
    

def indicator_score_rank(df,n1=10,n2=10):
    '''df:所有股票财务指标总分
    通过all_indicator_score(tudata,name_code_dict)获取
    '''
    result=df.T.sort_values(df.index[-1],ascending=False).iloc[:n1,-n1:]
    return result