#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：python_project
@File    ：data_plot.py
@IDE     ：PyCharm
@Author  ：Jinyi Zhang
@Date    ：2022/9/29 21:00
"""
# 先引入后面可能用到的包（package）
import pandas as pd
import matplotlib.pyplot as plt
from pylab import mpl
import seaborn as sns

sns.set_theme(style='darkgrid')  # 图形主题
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

# 对plotly的封装
import plotly.express as px
import plotly
#plotly.offline.init_notebook_mode(connected=True)
import warnings

from qstock.data.money import stock_money

warnings.simplefilter(action='ignore', category=FutureWarning)

###############################################################################
# 常用图表

# 画线图line
def line(data=None, x=None, y=None, title=None, color=None, line_group=None, 
         facet_col=None, legend=True,notebook=True):
    """画折线图
    data为series或dataframe的时候，可以不输入x和y
    """
    fig = px.line(data_frame=data, x=x, y=y, title=title, \
                  color=color, line_group=line_group, facet_col=facet_col)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 画散点图scatter
def scatter(data=None, x=None, y=None, title=None, color=None, size=None,
            trend=None, legend=True, marginal_x=None, marginal_y=None,
            notebook=True):
    '''color根据不同类型显示不同颜色
    size根据值大小显示散点图的大小
    trend='ols'添加回归拟合线
    marginal_x='violin',添加小提琴图
    marginal_y= 'box'，添加箱线图
    '''
    fig = px.scatter(data_frame=data, x=x, y=y, title=title, \
                     color=color, size=size, trendline=trend,
                     marginal_x=marginal_x, marginal_y=marginal_y)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 画饼图pie
def pie(data=None, x=None, y=None, color=None, title=None, 
        legend=False, hole=None,notebook=True):
    '''data为dataframe数据，value
    hole数值0-1，显示中间空心
    legend=True显示图例，默认不显示
    '''
    fig = px.pie(data_frame=data, names=x, values=y, color=color, title=title, hole=hole)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 画柱状图
def bar(data=None, x=None, y=None, title=None, color=None, legend=False,
        orientation=None, log_x=False, log_y=False, barmode='group',
        notebook=True):
    '''orientation='h'表示横向柱状图,log_x和log_y为True表示使用对数坐标
    barmode='group'表示对比条形图,默认。为'relative'
    如qs.bar(dd['总市值'],log_x=True,orientation='h')
    '''

    fig = px.bar(data_frame=data, x=x, y=y, color=color, title=title,
                 orientation=orientation, log_x=log_x, log_y=log_y, barmode=barmode)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 箱线图
def box(data=None, x=None, y=None, title=None, color=None, legend=False,
        orientation=None, log_x=False, log_y=False, boxmode=None,
        notebook=True):
    fig = px.box(data_frame=data, x=x, y=y, color=color, title=title,
                 orientation=orientation, log_x=log_x, log_y=log_y, boxmode=boxmode)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 小提琴图（Violin）
def violin(data=None, x=None, y=None, title=None, color=None, legend=False,
           orientation=None, log_x=False, log_y=False, box=False, points=None
           ,notebook=True):
    fig = px.violin(data_frame=data, x=x, y=y, color=color, title=title,
                    orientation=orientation, log_x=log_x, log_y=log_y, box=box,
                    points=points)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# 直方图histogram
def hist(data=None, x=None, y=None, title=None, color=None, legend=False,
         orientation=None, log_x=False, log_y=False, barmode='group',
         histnorm=None,notebook=True):
    '''histnorm={1:'percent',2:'probability',3:'density',4:'probability density'}
    '''
    hists = {1: 'percent', 2: 'probability', 3: 'density', 4: 'probability density'}
    if histnorm is not None:
        histnorm = hists[histnorm]
    fig = px.histogram(data_frame=data, x=x, y=y, color=color, title=title,
                       orientation=orientation, log_x=log_x, log_y=log_y,
                       barmode=barmode, histnorm=histnorm)
    fig.update_layout(showlegend=legend)
    fig.update_layout(
        title={
            'text': title,  # 标题名称
            'y': 0.9,  # 位置，坐标轴的长度看做1
            'x': 0.5,
            'xanchor': 'center',  # 相对位置
            'yanchor': 'top'})
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


# seaborn统计分布图
def hist_kde(data=None, x=None, y=None, kde=True, stat='count', 
             figsize=(15, 7)):
    '''
    直方图默认统计的是观测数，可以进行统计变化，设置stat参数。
    stat可选参数：count:观测数(默认)
    frequency:频数;density：密度;probability:概率
    kde=True表示添加核密度曲线
    '''
    plt.figure(figsize=figsize)
    sns.histplot(data=data, x=x, y=y, kde=kde, stat=stat)
    plt.show()


# 画矩形树图
def treemap(data, label, weight, value, color=['Green', 'Red', "#8b0000"],
            notebook=True):
    '''data：dataframe数据格式
    label：需要展示的列名或标签名，必须是list,如[px.Constant('股票'), '行业',  '股票名称']
    其中，'行业',  '股票名称'必须是data里的存在的列名
    weight:data里存在的列，用来显示权重
    value:data里存在的列，用来显示数值
    color：设置展示的颜色
    '''
    fig = px.treemap(data, path=label, values=weight, color=value, color_continuous_scale=color)
    fig.data[0].texttemplate = "%{label}<br>%{customdata}%"
    fig.update_traces(textposition="middle center",
                      selector=dict(type='treemap'))
    fig.update_layout(margin=dict(t=30, l=10, r=10, b=10))
    fig.update(layout_coloraxis_showscale=False)
    if notebook:
        return fig.show()
    else:
        return plotly.offline.plot(fig)


#####################################################################################
###金融量化特色可视化


# 北向资金可视化
def plot_north_money(north_data, w_list=[14, 20, 30, 60, 120]):
    '''north_data为北向资金数据'''
    df = (north_data[['north_money', 'south_money']] / 100).dropna().copy()
    for w in w_list:
        df[str(w) + '日累计'] = df['north_money'].rolling(w).sum()
    cols = [str(w) + '日累计' for w in w_list]
    dd = round(df['north_money'][-1], 2)
    date = df[cols].index[-1].strftime('%Y%m%d')
    sig = '流入' if dd > 0 else '流出'
    content = f'{date[:4]}年{date[4:6]}月{date[6:]}日,北向资金{sig}{abs(dd)}亿元'
    # print(content)
    dic = dict(df[cols].iloc[-1].round(2))

    for k, v in dic.items():
        temp_sig = '流入' if v > 0 else '流出'
        temp = f'{k}{temp_sig}{abs(v)}亿元'
        content += '，近' + temp
        # print(temp)
    # 最近250日周期内可视化
    df[cols][-250:].plot(figsize=(15, 20), subplots=True, title='北向资金流向（亿元）');
    plt.xlabel('');
    return content


# 个股资金流向可视化
def plot_stock_money(stock, w_list=[3, 5, 10, 20, 60]):
    '''stock可以为股票简称或代码，如晓程科技或300139'''
    # 获取个股资金流向数据
    df = stock_money(stock,w_list)
    df.index = pd.to_datetime(df['日期'])
    df = df.sort_index().dropna()
    for w in w_list:
        df[str(w) + '日累计'] = df['主力净流入'].rolling(w).sum()
    cols = [(str(w) + '日累计') for w in w_list]
    # 单位万元
    dd = round(df['主力净流入'][-1] / 10000, 2)
    date = df[cols].index[-1].strftime('%Y%m%d')
    sig = '流入' if dd > 0 else '流出'
    content = f'{date[:4]}年{date[4:6]}月{date[6:]}日,{stock}资金{sig}{abs(dd)}万元'
    print(content)
    # 单位转为万元
    result = df[cols] / 10000
    dic = dict(result.iloc[-1].round(2))

    for k, v in dic.items():
        temp_sig = '流入' if v > 0 else '流出'
        temp = f'{k}{temp_sig}{abs(v)}万元'
        content += '，近' + temp
        print(temp)

    # 可视化
    (result).plot(figsize=(15, 20), subplots=True, title=f'{stock}资金流向（万元）');
    plt.xlabel('')
    return


# ichimoku云图
def plot_ichimoku(df, t=9, k=26, l=30, s=52):
    '''df为dataframe数据，包含'open','high','low','close','volume‘列，索引为时间格式
    t:Tenkan-sen：转折线计算周期，默认9个交易日
    k：Kijun-sen：基准线计算周期，默认26
    l:Chikou Span：滞后跨度或延迟线计算周期，默认30天
    s:Senkou Span B：计算前导跨度B或先行上线B，默认52个交易日
    '''
    data = df.copy()

    # 计算转换线
    high_1 = data['high'].rolling(t).max()
    low_1 = data['low'].rolling(t).min()
    # Tenkan-sen：转换线
    data['conversion_line'] = (high_1 + low_1) / 2
    # 计算基准线
    high_2 = data['high'].rolling(k).max()
    low_2 = data['low'].rolling(k).min()
    data['base_line'] = (high_2 + low_2) / 2
    # 计算前导跨度A
    data['lead_span_A'] = ((data.conversion_line + data.base_line) / 2).shift(l)
    # 计算前导跨度A
    high_3 = data['high'].rolling(s).max()
    low_3 = data['high'].rolling(s).min()
    data['lead_span_B'] = ((high_3 + low_3) / 2).shift(s)
    # 滞后跨度
    data['lagging_span'] = data['close'].shift(-l)
    # 删除缺失值
    data.dropna(inplace=True)
    fig, ax = plt.subplots(1, 1, sharex=True, figsize=(15, 7))
    ax.plot(data.index, data['close'], linewidth=2, label='收盘价')
    ax.plot(data.index, data['lead_span_A'], label='前导跨度A', color='k')
    ax.plot(data.index, data['lead_span_B'], label='前导跨度B', color='y')
    ax.fill_between(data.index, data['lead_span_A'], data['lead_span_B'],
                    where=data['lead_span_A'] >= data['lead_span_B'], color='lightcoral')
    ax.fill_between(data.index, data['lead_span_A'], data['lead_span_B'],
                    where=data['lead_span_A'] < data['lead_span_B'], color='lightgreen')
    plt.legend(loc=0)
    plt.grid()
    plt.show()
