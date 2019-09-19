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

dates = pd.date_range('20190821',periods=6)
df = pd.DataFrame(np.random.randn(6,4),index=dates,columns=['a','b','c','d'])
dates1 = pd.date_range('20190821',periods=3)
df1 = pd.DataFrame(np.random.randn(3,2),index=dates1,columns=['a','c'])
df.loc[:,'e']=df1['a']
combined = df
#frames=[df,df1]
#combined=pd.concat(frames)

print('-----------------df-------------------')
print(df)
print('-----------------df1-------------------')
print(df1)
print('-----------------combined-------------------')
print(combined)





