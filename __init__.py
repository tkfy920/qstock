#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  ：Jinyi Zhang 
@Date    ：2022/9/29 20:20 
'''
#数据模块
#股票、债券、期货、基金等交易行情数据
from qstock.data.trade import *

#新闻数据
from qstock.data.news import *
#股票基本面数据
from qstock.data.fundamental import *
##行业、概念板块数据
from qstock.data.industry import *
#资金流向数据
from qstock.data.money import *
#宏观经济数据
from qstock.data.macro import *

#可视化模块
from qstock.plot.data_plot import *
from qstock.plot.chart_plot import *

#选股模块
from qstock.stock.stock_pool import *
from qstock.stock.ths_em_pool import *

#回测模块
from qstock.backtest.vec_backtest import *
from qstock.backtest.turtle import *
from qstock.backtest.bt_backtest import *

#问财数据
import pywencai

def wencai(query,query_type='stock'):
    '''获取结果'''
    result=pywencai.get(query=query,query_type=query_type)
    return result
    
from qstock import wencai