"""
测试用文件
"""
import pandas as pd
import numpy as np
import tushare as ts

ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()

if __name__ == "__main__":
    # df = ts_pro.concept(src='ts')
    # df.to_csv('.\\data_csv\\概念列表.csv', encoding="utf-8")

    # df = ts_pro.index_weight(index_code='399007.SZ')
    # trade_date = int(df.head(1)['trade_date'])
    # df = df[df.trade_date == trade_date]['con_code']
    # df.to_csv('.\\data_csv\\沪深300成分399007.SZ.csv', encoding="utf-8")

    #df = ts_pro.moneyflow_hsgt(start_date='20200225', end_date='20200226')
    from st_board import Hsgt
    Hsgt.get_moneyflow()





