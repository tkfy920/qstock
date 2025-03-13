#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：python_project 
@File    ：chart_plot.py
@IDE     ：PyCharm 
@Author  ：Jinyi Zhang 
@Date    ：2022/9/29 21:05 
'''

import numpy as np
import pandas as pd

# 导入pyecharts
from pyecharts.charts import (Kline, Bar, Line, Pie, HeatMap, Map, WordCloud,
                              Calendar, Boxplot, Grid)
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode

# 文本处理
import jieba
import jieba.analyse
import re

# 计算MACD
def MACD(close, short=12, long=26, signal=9):
    ema12 = pd.DataFrame.ewm(close, span=short).mean()
    ema26 = pd.DataFrame.ewm(close, span=long).mean()
    # dif组成的线叫MACD线
    dif = ema12 - ema26
    # dea组成的线叫signal线
    dea = pd.DataFrame.ewm(dif, span=signal).mean()
    # dif与dea的差得到柱状
    hist = 2 * (dif - dea)
    dif = np.round(dif, 3)
    dea = np.round(dea, 3)
    hist = np.round(hist, 3)
    return dif, dea, hist

#计算修正K线数据
def Heikin_Ashi(df):
    df=df.copy()
    #计算修正版K线
    df['ha_close']=(df.close+df.open+df.high+df.low)/4.0
    ha_open=np.zeros(df.shape[0])
    ha_open[0]=df.open[0]
    for i in range(1,df.shape[0]):
        ha_open[i]=(ha_open[i-1]+df['ha_close'][i-1])/2
    df.insert(1,'ha_open',ha_open)
    df['ha_high']=df[['high','ha_open','ha_close']].max(axis=1)
    df['ha_low']=df[['low','ha_open','ha_close']].min(axis=1)
    old_cols=['ha_open','ha_high','ha_low','ha_close','volume']
    df=df[old_cols]
    new_cols=['open','high','low','close','volume']
    df=df.rename(columns=dict(zip(old_cols,new_cols)))
    return df

#修正K线图
#平均K线图（Heikin-Ashi）
def HA_kline(df):
    data=Heikin_Ashi(df)
    return kline(data,title='股票平均K线图（Heikin-Ashi）')

# 股票K线图
def kline(df, mas=5, mal=20, notebook=True,title="股票K线图"):
    # 计算技术指标
    data = df.dropna().copy(deep=True)
    data['mas'] = data.close.rolling(mas).mean()
    data['mal'] = data.close.rolling(mal).mean()
    data['macd'], data['macdsignal'], data['macdhist'] = MACD(data.close)
    attr = list(data.index.strftime('%Y%m%d'))
    try:
        vol = data["volume"].tolist()
    except:
        vol = data['vol'].tolist()
    kline = (Kline()  # K线图
        .add_xaxis(xaxis_data=attr)
        .add_yaxis(series_name="klines",
                   y_axis=data[["open", "close", "low", "high"]].values.tolist(),
                   itemstyle_opts=opts.ItemStyleOpts(
                       color="red", color0="green",  # 设置K线两种颜色
                       border_color="red", border_color0="green", ),
                   markpoint_opts=opts.MarkPointOpts(data=[  # 添加标记符
                       opts.MarkPointItem(type_='max', name='最大值'),
                       opts.MarkPointItem(type_='min', name='最小值'), ]),
                   markline_opts=opts.MarkLineOpts(  # 添加辅助线
                       data=[opts.MarkLineItem(type_="average", value_dim="close")], ))
        .set_global_opts(
        legend_opts=opts.LegendOpts(is_show=True, pos_top=1, pos_left='center'),
        datazoom_opts=[  # 控制三个组合图
            opts.DataZoomOpts(is_show=False, type_="inside", xaxis_index=[0, 0], range_end=100, ),
            opts.DataZoomOpts(is_show=False, xaxis_index=[0, 1], range_end=100),
            opts.DataZoomOpts(is_show=True, xaxis_index=[0, 2], pos_top="95%", range_end=100, ), ],
        title_opts=opts.TitleOpts(title=title, pos_left="0"),
        yaxis_opts=opts.AxisOpts(is_scale=True,
                                 splitarea_opts=opts.SplitAreaOpts(  # 分隔区域配置
                                     is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)), )))
    # 成交量柱状图
    bar = (Bar()
        .add_xaxis(xaxis_data=attr)
        .add_yaxis(series_name="volume",
                   y_axis=vol,
                   label_opts=opts.LabelOpts(is_show=False),
                   itemstyle_opts=opts.ItemStyleOpts(color=JsCode("""
                    function(params) {
                        var colorList;
                        if (barData[params.dataIndex][1] > barData[params.dataIndex][0]) {
                            colorList = 'red';} else {
                            colorList = 'green';}
                        return colorList;}""")), )
        .set_global_opts(
        legend_opts=opts.LegendOpts(is_show=False), ))
    # 添加均线
    line = (Line()
            .add_xaxis(xaxis_data=attr)
            .add_yaxis(series_name=f"{mas}日均线", y_axis=data["mas"].tolist(),
                       label_opts=opts.LabelOpts(is_show=False),
                       is_symbol_show=False,
                       linestyle_opts=opts.LineStyleOpts(width=1.5),
                       itemstyle_opts=opts.ItemStyleOpts(color="black"), )

            .add_yaxis(series_name=f"{mal}日均线", y_axis=data["mal"].tolist(),
                       label_opts=opts.LabelOpts(is_show=False),
                       is_symbol_show=False,
                       linestyle_opts=opts.LineStyleOpts(width=1.5),
                       itemstyle_opts=opts.ItemStyleOpts(color="blue"), ))

    ##MACD柱状图
    bar_2 = (Bar()
        .add_xaxis(xaxis_data=attr)
        .add_yaxis(series_name="MACD",
                   y_axis=data["macdhist"].values.tolist(),
                   label_opts=opts.LabelOpts(is_show=False),
                   itemstyle_opts=opts.ItemStyleOpts(color=JsCode("""
                            function(params) {
                                var colorList;
                                if (params.data >= 0) {
                                  colorList = 'red';} else {
                                  colorList = 'green';}
                                return colorList;}""")), )
        .set_global_opts(  # 不显示X轴
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False), ),
        legend_opts=opts.LegendOpts(is_show=True, pos_top=1, pos_right=20), ))

    line_2 = (Line()  # 添加MACD折线
              .add_xaxis(xaxis_data=attr)
              .add_yaxis(series_name="DIFF",
                         y_axis=data["macd"],
                         label_opts=opts.LabelOpts(is_show=False),
                         is_symbol_show=False,
                         linestyle_opts=opts.LineStyleOpts(width=1.5),
                         itemstyle_opts=opts.ItemStyleOpts(color="red"), )
              .add_yaxis(series_name="DEA",
                         y_axis=data["macdsignal"],
                         label_opts=opts.LabelOpts(is_show=False),
                         is_symbol_show=False,
                         linestyle_opts=opts.LineStyleOpts(width=1.5),
                         itemstyle_opts=opts.ItemStyleOpts(color="green"), )
              .set_global_opts(legend_opts=opts.LegendOpts(is_show=False)))

    # 最下面的柱状图和折线图,使用overlap层叠多图
    overlap_bar_line = bar_2.overlap(line_2)
    # 将三个图组合
    grid = Grid(init_opts=opts.InitOpts(width="100%", height="600px"))
    # 自定义成交量颜色，收盘大于开盘红色，否则绿色
    grid.add_js_funcs("var barData={}".format(data[["open", "close"]].values.tolist()))
    overlap_kline_line = kline.overlap(line)
    grid.add(overlap_kline_line,  # k线,
             grid_opts=opts.GridOpts(pos_left="5%", pos_right="3%", height="45%"), )
    grid.add(bar,  # 成交量
             grid_opts=opts.GridOpts(pos_left="5%", pos_right="3%", pos_top="60%", height="15%"), )
    grid.add(overlap_bar_line,
             grid_opts=opts.GridOpts(pos_left="5%", pos_right="3%", pos_top="80%", height="15%"), )
    if notebook:
        return grid.render_notebook()
    else:
        return grid.render('kline.html')


# 股价折线图+不同颜色显示分位数点位
def stock_line(data=None, x=None, y=None, notebook=True, title=None):
    '''data数据dataframe或series格式'''
    # 不同点位设置不同颜色
    if data is None:
        x = x
        y = y
        des = pd.Series(y).describe()
    else:
        if x is None:
            try:
                x = data.index.strftime('%Y%m%d').tolist()
            except:
                x = data.index
        else:
            x = data[x].values.tolist()

        if y is None:
            y = data.values.tolist()
            des = data.describe()
        else:
            des = data[y].describe()
            y = data[y].values.tolist()

    v1, v2, v3 = np.ceil(des['25%']), np.ceil(des['50%']), np.ceil(des['75%'])
    pieces = [{"min": v3, "color": "red"},
              {"min": v2, "max": v3, "color": "blue"},
              {"min": v1, "max": v2, "color": "black"},
              {"max": v1, "color": "green"}, ]
    # 价格折线图（不同分位点不同颜色）

    g = (
        Line({'width': '100%', 'height': '480px'})  # 设置画布大小，px像素
            .add_xaxis(xaxis_data=x)  # x数据
            .add_yaxis(
            series_name="",  # 序列名称
            y_axis=y,  # 添加y数据
            is_smooth=True,  # 平滑曲线d
            is_symbol_show=False,  # 不显示折线的小圆圈
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2),  # 线宽
            markpoint_opts=opts.MarkPointOpts(data=[  # 添加标记符
                opts.MarkPointItem(type_='max', name='最大值'),
                opts.MarkPointItem(type_='min', name='最小值'), ], symbol_size=[100, 30]),
            markline_opts=opts.MarkLineOpts(  # 添加均值辅助性
                data=[opts.MarkLineItem(type_="average")], ))
            .set_global_opts(  # 全局参数设置
            title_opts=opts.TitleOpts(title=title, pos_left='center'),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            datazoom_opts=opts.DataZoomOpts(),
            visualmap_opts=opts.VisualMapOpts(  # 视觉映射配置
                orient="horizontal", split_number=4,
                pos_left='center', is_piecewise=True,
                pieces=pieces, ), )
    )
    # 默认使用jupyter notebook显示图形
    if notebook:
        return g.render_notebook()
    else:
        return g.render('stockline.html')


# 画线图
def chart_line(data=None, x=None, y=None, title=None, notebook=True, zoom=False):
    '''x,y均为list，或者x为dataframe'''
    if data is None:
        x = x;
        y = y
    else:
        if isinstance(data, pd.Series):
            x = list(data.index)
            y = data.values.tolist()

        if isinstance(data, pd.DataFrame):
            x = data[x].values.tolist()
            y = data[y].values.tolist()

    g = (Line()
         .add_xaxis(x)
         .add_yaxis(
        series_name="",  # 序列名称
        y_axis=y,
        is_smooth=True,  # 平滑曲线d
        is_symbol_show=False,  # 不显示折线的小圆圈
        label_opts=opts.LabelOpts(is_show=False),
        linestyle_opts=opts.LineStyleOpts(width=2), )
         # 添加全局配置项
         .set_global_opts(title_opts=opts.TitleOpts(title=title),
                          datazoom_opts=opts.DataZoomOpts(is_show=zoom),  # 区域缩放配置项
                          ))
    g.width = "100%"  # 设置画布比例
    # 默认使用jupyter notebook显示图形
    if notebook:
        return g.render_notebook()
    else:
        return g.render('line.html')


# 画柱状图
def chart_bar(data=None, x=None, y=None, title=None, notebook=True, zoom=False):
    '''x,y均为list，或者x为dataframe'''
    if data is None:
        x = x;
        y = y
    else:
        if isinstance(data, pd.Series):
            x = list(data.index)
            y = data.values.tolist()

        if isinstance(data, pd.DataFrame):
            x = data[x].values.tolist()
            y = data[y].values.tolist()

    g = (Bar()
         .add_xaxis(x)
         .add_yaxis("", y)
         # 添加全局配置项
         .set_global_opts(title_opts=opts.TitleOpts(title=title),
                          datazoom_opts=opts.DataZoomOpts(is_show=zoom),  # 区域缩放配置项
                          ))
    g.width = "100%"  # 设置画布比例
    # 默认使用jupyter notebook显示图形
    if notebook:
        return g.render_notebook()
    else:
        return g.render('bar.html')


# 画纵向柱状图
def chart_inv_bar(data=None, x=None, y=None, title=None, notebook=True, zoom=False):
    '''x,y均为list，或者x为dataframe'''
    # 自定义柱状图颜色，使用Javascript代码
    if data is None:
        x = x;
        y = y
    else:
        if isinstance(data, pd.Series):
            x = list(data.index)
            y = data.values.tolist()

        if isinstance(data, pd.DataFrame):
            x = data[x].values.tolist()
            y = data[y].values.tolist()

    color_function = """
        function (params) {
            if (params.value < 0 ) {
                return 'green';} 
                else if (params.value > 0 && params.value < 20) {
                return 'blue';}
            return 'red';}
        """
    g = (Bar()
         .add_xaxis(x)
         .add_yaxis("", y,
                    itemstyle_opts=opts.ItemStyleOpts(color=JsCode(color_function)))
         .reversal_axis()
         .set_series_opts(label_opts=opts.LabelOpts(position="right"))
         .set_global_opts(title_opts=opts.TitleOpts(title=title, pos_left='center'),
                          datazoom_opts=opts.DataZoomOpts(is_show=zoom),  # 区域缩放配置项
                          xaxis_opts=opts.AxisOpts(is_show=False)))

    g.width = "100%"
    if notebook:
        return g.render_notebook()
    else:
        return g.render('bar.html')


# 饼图
def chart_pie(data=None, x=None, y=None, data_pair=None, title=None, notebook=True):
    if data_pair is not None:
        data_pair = data_pair
    else:
        if data is None:
            x = x;
            y = y
        else:
            if isinstance(data, pd.Series):
                x = list(data.index)
                y = data.values.tolist()

            if isinstance(data, pd.DataFrame):
                x = data[x].values.tolist()
                y = data[y].values.tolist()
        data_pair = [list(i) for i in zip(x, y)]

    g = (Pie()
        .add("", data_pair)
        .set_global_opts(
        title_opts=opts.TitleOpts(title=title),
        legend_opts=opts.LegendOpts(is_show=False), )
        .set_series_opts(tooltip_opts=opts.TooltipOpts(
        trigger="item", formatter="{b}:{c} ({d}%)"
    ),
        label_opts=opts.LabelOpts(formatter="{b}:{c}({d}%)")))
    g.width = "100%"
    if notebook:
        return g.render_notebook()
    else:
        return g.render('pie.html')


# 热力图
def chart_heatmap(x, y, v, title=None, notebook=True):
    g = (HeatMap()
        .add_xaxis(x)
        .add_yaxis("", y, v,
                   label_opts=opts.LabelOpts(is_show=True, position="inside"), )
        .set_global_opts(
        title_opts=opts.TitleOpts(title=title),
        visualmap_opts=opts.VisualMapOpts(is_show=False)))
    if notebook:
        return g.render_notebook()
    else:
        return g.render('heatmap.html')


# 热力图改变颜色
def chart_heatmap_color(x, y, value, v1=0.0, v2=1.0, v3=3.0, title=None, notebook=True):
    pieces = [{"min": v3, "color": "red"},
              {"min": v2, "max": v3, "color": "blue"},
              {"min": v1, "max": v2, "color": "black"},
              {"max": v1, "color": "green"}, ]
    g = (HeatMap()
        .add_xaxis(x)
        .add_yaxis("", y, value,
                   label_opts=opts.LabelOpts(is_show=True, position="inside"), )
        .set_global_opts(
        title_opts=opts.TitleOpts(title="上证综指月收益率(%)"),
        visualmap_opts=opts.VisualMapOpts(  # 视觉映射配置
            orient="horizontal", split_number=4,
            pos_left='center', is_piecewise=True,
            pieces=pieces, )))
    if notebook:
        return g.render_notebook()
    else:
        return g.render('heatmap2.html')


# 画地图
def chart_map(data=None, x=None, y=None, data_pair=None, title=None, notebook=True):
    if data_pair is not None:
        data_pair = data_pair
    else:
        if data is None:
            x = x;
            y = y
        else:
            if isinstance(data, pd.Series):
                x = list(data.index)
                y = data.values.tolist()

            if isinstance(data, pd.DataFrame):
                x = data[x].values.tolist()
                y = data[y].values.tolist()
        data_pair = [list(i) for i in zip(x, y)]

    max_ = pd.DataFrame(data_pair).iloc[:, 1].max()
    g = (Map()
        .add("", data_pair, "china", is_map_symbol_show=False)
        .set_global_opts(
        title_opts=opts.TitleOpts(title=title, pos_left='center'),
        visualmap_opts=opts.VisualMapOpts(max_=int(max_ / 100 + 1) * 100,
                                          orient="horizontal", pos_left='center', )))
    g.width = "100%"
    if notebook:
        return g.render_notebook()
    else:
        return g.render('map.html')

# 文本分词
def cut_word(data, stopword='stopwords.txt'):
    sentence = ''.join(list(data))
    text = jieba.lcut(''.join(re.findall('[\u4e00-\u9fa5]', sentence)), cut_all=False)
    try:
        stopwords = [i.strip() for i in open(stopword).readlines()]
        for i in range(len(text) - 1, -1, -1):
            if text[i] in stopwords:
                del text[i]
    except:
        pass
    return text


# 词云数据格式
def cloud_data(news_list, stopword='stopwords.txt'):
    words = ' '.join(cut_word(news_list, stopword))
    tags = jieba.analyse.extract_tags(words, topK=200, withWeight=True)
    tf = dict((a[0], a[1]) for a in tags)
    data = [[k, v] for k, v in tf.items()]
    return data


# 画中文词云图
def chart_wordcloud(data, title=None, notebook=True):
    g = (WordCloud()
        .add(series_name="热点分析", data_pair=data, word_size_range=[6, 66])
        .set_global_opts(
        title_opts=opts.TitleOpts(
            title=title, title_textstyle_opts=opts.TextStyleOpts(font_size=23)),
        tooltip_opts=opts.TooltipOpts(is_show=True), ))

    g.width = "100%"
    if notebook:
        return g.render_notebook()
    else:
        return g.render('wordcloud.html')


# 日历图
def chart_calendar(data, title=None, notebook=True):
    start = data.index[0].strftime('%Y-%m-%d')
    end = data.index[0].strftime('%Y-%m-%d')
    g = (Calendar(init_opts=opts.InitOpts(width="900px", height="250px"))
        .add("",
             data,  # 添加数据
             calendar_opts=opts.CalendarOpts(
                 range_=[start, end],
                 daylabel_opts=opts.CalendarDayLabelOpts(name_map="cn"),
                 monthlabel_opts=opts.CalendarMonthLabelOpts(name_map="cn"), ), )
        .set_global_opts(
        title_opts=opts.TitleOpts(title=title, pos_left='center'),
        visualmap_opts=opts.VisualMapOpts(is_show=False,
                                          max_=5.5, min_=-5,
                                          orient="horizontal",
                                          is_piecewise=False, )))
    if notebook:
        return g.render_notebook()
    else:
        return g.render('calendar.html')


# 箱线图
def chart_box(df, title=None, notebook=True):
    v = df.T.values
    x = list(df.columns)
    g = Boxplot()
    g.add_xaxis(x)
    g.add_yaxis("", g.prepare_data(v))
    g.set_global_opts(title_opts=opts.TitleOpts(title=title))
    if notebook:
        return g.render_notebook()
    else:
        return g.render('box.html')



