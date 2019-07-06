import tushare as ts
import pandas as pd
ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()
data = ts_pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')