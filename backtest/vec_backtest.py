# -*- coding: utf-8 -*-
"""
Created on Sun Oct  9 09:03:15 2022

@author: Jinyi Zhang
"""
import pandas as pd  
import numpy as np
import matplotlib.pyplot as plt
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
from datetime import timedelta
#关掉pandas的warnings
pd.options.mode.chained_assignment = None
from qstock.data.trade import get_data

#获取数据
def data_feed(code,index='hs300',start='20000101',end='',fqt=2):
    #获取个股数据
    df=get_data(code,start=start,end=end,fqt=fqt).fillna(method='ffill')
    #指数数据,作为参照指标
    df['index']=get_data(index,start,end).close.pct_change().fillna(0)
    #计算收益率
    df['rets']=df.close.pct_change().fillna(0)
    #计算买入持有和对比指数的累计收益率
    df['rets_line']=(df.rets+1.0).cumprod()
    df['index_line']=(df['index']+1.0).cumprod()
    return df

#有交易策略时
def trade_indicators(df):
    if 'capital_ret' not in df.columns:
        return
    # 计算资金曲线
    df['capital'] = (df['capital_ret'] + 1).cumprod()
    df=df.reset_index()
    # 记录买入或者加仓时的日期和初始资产
    df.loc[df['position'] > df['position'].shift(1), 'start_date'] = df['date']
    df.loc[df['position'] > df['position'].shift(1), 'start_capital'] = df['capital'].shift(1)
    df.loc[df['position'] > df['position'].shift(1), 'start_stock'] = df['close'].shift(1)
    # 记录卖出时的日期和当天的资产
    df.loc[df['position'] < df['position'].shift(1), 'end_date'] = df['date']
    df.loc[df['position'] < df['position'].shift(1), 'end_capital'] = df['capital']
    df.loc[df['position'] < df['position'].shift(1), 'end_stock'] = df['close']
    # 将买卖当天的信息合并成一个dataframe
    df_temp = df[df['start_date'].notnull() | df['end_date'].notnull()]

    df_temp['end_date'] = df_temp['end_date'].shift(-1)
    df_temp['end_capital'] = df_temp['end_capital'].shift(-1)
    df_temp['end_stock'] = df_temp['end_stock'].shift(-1)

    # 构建账户交易情况dataframe：'hold_time'持有天数，
    #'trade_return'该次交易盈亏,'stock_return'同期股票涨跌幅
    trade = df_temp.loc[df_temp['end_date'].notnull(), ['start_date', 'start_capital', 'start_stock',
                                                       'end_date', 'end_capital', 'end_stock']]
    trade['hold_time'] = (trade['end_date'] - trade['start_date']).dt.days
    trade['trade_return'] = trade['end_capital'] / trade['start_capital'] - 1
    trade['stock_return'] = trade['end_stock'] / trade['start_stock'] - 1

    trade_num = len(trade)  # 计算交易次数
    max_holdtime = trade['hold_time'].max()  # 计算最长持有天数
    average_change = trade['trade_return'].mean()  # 计算每次平均涨幅
    max_gain = trade['trade_return'].max()  # 计算单笔最大盈利
    max_loss = trade['trade_return'].min()  # 计算单笔最大亏损
    total_years = (trade['end_date'].iloc[-1] - trade['start_date'].iloc[0]).days / 365
    trade_per_year = trade_num / total_years  # 计算年均买卖次数

    # 计算连续盈利亏损的次数
    trade.loc[trade['trade_return'] > 0, 'gain'] = 1
    trade.loc[trade['trade_return'] < 0, 'gain'] = 0
    trade['gain'].fillna(method='ffill', inplace=True)
    # 根据gain这一列计算连续盈利亏损的次数
    rtn_list = list(trade['gain'])
    successive_gain_list = []
    num = 1
    for i in range(len(rtn_list)):
        if i == 0:
            successive_gain_list.append(num)
        else:
            if (rtn_list[i] == rtn_list[i - 1] == 1) or (rtn_list[i] == rtn_list[i - 1] == 0):
                num += 1
            else:
                num = 1
            successive_gain_list.append(num)
    # 将计算结果赋给新的一列'successive_gain'
    trade['successive_gain'] = successive_gain_list
    # 分别在盈利和亏损的两个dataframe里按照'successive_gain'的值排序并取最大值
    max_successive_gain = trade[trade['gain'] == 1].sort_values(by='successive_gain', \
                        ascending=False)['successive_gain'].iloc[0]
    max_successive_loss = trade[trade['gain'] == 0].sort_values(by='successive_gain', \
                        ascending=False)['successive_gain'].iloc[0]
    
    #  输出账户交易各项指标
    print ('\n==============每笔交易收益率及同期股票涨跌幅===============')
    print (trade[['start_date', 'end_date', 'trade_return', 'stock_return']])
    print ('\n====================账户交易的各项指标=====================')
    print ('交易次数为：%d   最长持有天数为：%d' % (trade_num, max_holdtime))
    print ('每次平均涨幅为：%f' % average_change)
    print ('单次最大盈利为：%f  单次最大亏损为：%f' % (max_gain, max_loss))
    print ('年均买卖次数为：%f' % trade_per_year)
    print ('最大连续盈利次数为：%d  最大连续亏损次数为：%d' % (max_successive_gain, max_successive_loss))
    return trade

def trade_performance(df,plot=True):
    if 'capital_ret' in df.columns:
        df1 = df.loc[:,['index','rets', 'capital_ret']]
        name_dict={'index':'基准指数','rets':'买入持有','capital_ret':'交易策略'}
    else:
        df1 = df.loc[:,['index','rets']]
        name_dict={'index':'基准指数','rets':'买入持有'}
    #df1.loc[df.index[0], ['index','rets']] = 0

    #计算收益率
    #累计收益率
    acc_ret=(df1+1).cumprod()
    #计算总收益率
    total_ret=acc_ret.iloc[-1]-1
    #年化收益率，假设一年250个交易日
    annual_ret=pow(1+total_ret,250/len(df1))-1
    #最大回撤
    md=((acc_ret.cummax()-acc_ret)/acc_ret.cummax()).max()
    exReturn=df1-0.03/250
    #计算夏普比率
    sharper_atio=np.sqrt(len(exReturn))*exReturn.mean()/df1.std()
    #计算CAPM里的alpha和beta系数
    beta0=df1[['rets','index']].cov().iat[0,1]/df['index'].var()
    alpha0=(annual_ret['rets']-annual_ret['index']*beta0)
    if 'capital_ret' in df1.columns:
        beta1=df1[['capital_ret','index']].cov().iat[0,1]/df['index'].var()
        alpha1=(annual_ret['capital_ret']-annual_ret['index']*beta1)
        alpha=[np.nan,alpha0,alpha1,]
        beta=[np.nan,beta0,beta1,]
    else:
        alpha=[np.nan,alpha0]
        beta=[np.nan,beta0]
    # 计算每一年(月,周)股票,资金曲线的收益
    year_ret = df1.resample('A').apply(lambda x: (x + 1.0).prod() - 1.0)
    month_ret = df1.resample('M').apply(lambda x: (x + 1.0).prod() - 1.0)
    week_ret = df1.resample('W').apply(lambda x: (x + 1.0).prod() - 1.0)
    #去掉缺失值
    year_ret.fillna(0,inplace=True)
    month_ret.fillna(0,inplace=True)
    week_ret.fillna(0,inplace=True)
    #计算胜率
    year_win_rate = year_ret.apply(lambda s:len(s[s>0])/len(s[s!=0]))
    month_win_rate =month_ret.apply(lambda s:len(s[s>0])/len(s[s!=0]))
    week_win_rate = week_ret.apply(lambda s:len(s[s>0])/len(s[s!=0]))
    result=pd.DataFrame()
    result['总收益率']=total_ret
    result['年化收益率']=annual_ret
    result['最大回撤']=md
    result['夏普比率']=sharper_atio
    result['Alpha']=alpha
    result['Beta']=beta
    result['年胜率']=year_win_rate
    result['月胜率']=month_win_rate
    result['周胜率']=week_win_rate
    result=result.T.rename(columns=name_dict)
    if plot:
        acc_ret=acc_ret.rename(columns=name_dict)
        acc_ret.plot(figsize=(15,7))
        plt.title('策略累计净值',size=15)
        plt.xlabel('')
        ax=plt.gca()
        ax.spines['right'].set_color('none')
        ax.spines['top'].set_color('none')
        plt.show()
    return result

def start_backtest(code,index='hs300',start='20000101',end='20220930',fqt=2,strategy=None):
    if not isinstance(code,str):
        trade_indicators(code.copy())
        return trade_performance(code.copy())
    print(f'回测标的：{code}')
    if end in ['',None]:
        end='至今'
    print(f'回测期间：{start}—{end}')
    d0=data_feed(code,index,start=start,end=end,fqt=fqt)
    if strategy is not None:
        df=strategy(d0)
        trade_indicators(df)
    else:
        df=d0
    return trade_performance(df)

def MR_Strategy(df,lookback=20,buy_threshold=-1.5,sell_threshold=1.5,cost=0.0):
    '''输入参数：
    df为数据表: 包含open,close,low,high,vol，标的收益率rets，指数收益率数据hs300
    lookback为均值回归策略参数，设置统计区间长度，默认20天
    buy_threshold:买入参数，均值向下偏离标准差的倍数，默认-1.5
    sell_threshold:卖出参数，均值向上偏离标准差的倍数，默认1.5
    cost为手续费+滑点价差，可以根据需要进行设置，默认为0.0
    '''
    #计算均值回归策略的score值
    ret_lb=df.rets.rolling(lookback).mean()
    std_lb=df.rets.rolling(lookback).std()
    df['score']=(df.rets-ret_lb)/std_lb
    df.fillna(0,inplace=True)
    #设计买卖信号，为尽量贴近实际，加入涨跌停不能买卖的限制
    #当score值小于-1.5且第二天开盘没有涨停发出买入信号设置为1
    df.loc[(df.score<buy_threshold) &(df['open'] < df['close'].shift(1) * 1.097), 'signal'] = 1
    #当score值大于1.5且第二天开盘没有跌停发出卖入信号设置为0
    df.loc[(df.score>sell_threshold) &(df['open'] > df['close'].shift(1) * 0.903), 'signal'] = 0
    df['position']=df['signal'].shift(1)
    df['position'].fillna(method='ffill',inplace=True)
    df['position'].fillna(0,inplace=True)
    #根据交易信号和仓位计算策略的每日收益率
    df.loc[df.index[0], 'capital_ret'] = 0
    #今天开盘新买入的position在今天的涨幅(扣除手续费)
    df.loc[df['position'] > df['position'].shift(1), 'capital_ret'] = \
                         (df['close'] / df['open']-1) * (1- cost) 
    #卖出同理
    df.loc[df['position'] < df['position'].shift(1), 'capital_ret'] = \
                   (df['open'] / df['close'].shift(1)-1) * (1-cost) 
    # 当仓位不变时,当天的capital是当天的change * position
    df.loc[df['position'] == df['position'].shift(1), 'capital_ret'] = \
                        df['rets'] * df['position']
    #计算策略累计收益率
    df['capital_line']=(df.capital_ret+1.0).cumprod()
    return df

def North_Strategy(data,window=252,stdev_n=1.5,cost=0.00):
    '''输入参数：
    data:包含北向资金数据
    window:移动窗口
    stdev_n:几倍标准差
    cost:手续费
    '''
    # 中轨
    df=data.copy().fillna(0)
    df['mid'] = df['北向资金'].rolling(window).mean()
    stdev = df['北向资金'].rolling(window).std()
    # 上下轨
    df['upper'] = df['mid'] + stdev_n * stdev
    df['lower'] = df['mid'] - stdev_n * stdev
    df['ret']=df.close/df.close.shift(1)-1
    df.fillna(0,inplace=True)
   
    #设计买卖信号
    #当日北向资金突破上轨线发出买入信号设置为1
    df.loc[df['北向资金']>df.upper, 'signal'] = 1
    #当日北向资金跌破下轨线发出卖出信号设置为0
    df.loc[df['北向资金']<df.lower, 'signal'] = 0
    df['position']=df['signal'].shift(1)
    df['position'].fillna(method='ffill',inplace=True)
    df['position'].fillna(0,inplace=True)
    #根据交易信号和仓位计算策略的每日收益率
    df.loc[df.index[0], 'capital_ret'] = 0
    #今天开盘新买入的position在今天的涨幅(扣除手续费)
    df.loc[df['position'] > df['position'].shift(1), 'capital_ret'] = \
                         (df.close/ df.open-1) * (1- cost) 
    #卖出同理
    df.loc[df['position'] < df['position'].shift(1), 'capital_ret'] = \
                   (df.open / df.close.shift(1)-1) * (1-cost) 
    # 当仓位不变时,当天的capital是当天的change * position
    df.loc[df['position'] == df['position'].shift(1), 'capital_ret'] = \
                        df['ret'] * df['position']
    #计算策略累计收益率
    df['capital_line']=(df.capital_ret+1.0).cumprod()
    return df

#海龟交易法则指数择时简单版回测
def TT_strategy(data,n1=20,n2=10):
    df=data.copy()
    #最近N1个交易日最高价
    df['H_N1']=df.high.rolling(n1).max()
    #最近N2个交易日最低价
    df['L_N2']=df.low.rolling(n2).max()
    #当日收盘价>昨天最近N1个交易日最高点时发出信号设置为1
    buy_index=df[df.close>df['H_N1'].shift(1)].index
    df.loc[buy_index,'signal']=1
    #将当日收盘价<昨天最近N2个交易日的最低点时收盘信号设置为0
    sell_index=df[df.close<df['L_N2'].shift(1)].index
    df.loc[sell_index,'signal']=0
    df['position']=df['signal'].shift(1)
    df['position'].fillna(method='ffill',inplace=True)
    d=df[df['position']==1].index[0]-timedelta(days=1)
    df1=df.loc[d:].copy()
    df1['position'][0]=0
    #当仓位为1时，买入持仓，当仓位为0时，空仓，计算资金净值
    df1['capital_ret']=df1.rets.values*df1['position'].values
    #计算策略累计收益率
    df1['capital_line']=(df1.capital_ret+1.0).cumprod()
    return df1







