# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 10:19:19 2023

@author: tkfy9
"""

import backtrader as bt
import qstock as qs
import warnings

# 忽略UserWarning（缺失zipline.assets ）
warnings.filterwarnings("ignore",  message="Module \"zipline.assets\"  not found")
import pyfolio as pf

def bt_result(code='sh',start='2000-01-01', end=None,
              strategy=None,startcash=10000000.0,commission=0.00):
    '''code:回测代码或简称列表（list）或数据的字典格式
       start，end为回测起始日期，格式为'2023-08-04'
       strategy:策略名称
       startcash：初始资金
       commission：手续费
    '''
    print('抱歉，该接口仅限知识星球会员使用，详情可添加个人微信sky2blue2，获取8折优惠')
    pass