import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print

aa = pd.DataFrame([['A0','B0','C0'],
                   ['A1','B1','C1'],
                   ['A2','B2','C2']],columns=['A','B','C'])

bb = pd.DataFrame([['A3','B3','C3'],
                   ['A4','B4','C4'],
                   ['A5','B5','C5']],columns=['A','B','C'])


print('-----------aa----------------')
print()
print(aa)
print()
print('-----------bb----------------')
print(bb)
print()
print('-----------concat----------------')
frames = [aa,bb]
result = pd.concat(frames)
print(result)
print()
print('-----------concat axis=1 合并的轴----------------')
frames = [aa,bb]
result = pd.concat(frames,axis=1)
print(result)
print()
print('-----------concat ignore_index=True----------------')
frames = [aa,bb]
result = pd.concat(frames,ignore_index=True)
print(result)
