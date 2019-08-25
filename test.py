import tushare as ts
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl

df_al = pd.DataFrame(columns=['ts_code','valid','selected','type','stype1','stype2'])
