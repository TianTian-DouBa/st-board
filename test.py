import tushare as ts
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl
from st_board import Stock

ts_code = '00001.SZ'
df_fq = Stock.load_adj_factor(ts_code)
df_stock = Stock.load_stock_daily(ts_code)
#df_cls = 
