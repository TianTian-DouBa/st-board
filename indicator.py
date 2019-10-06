import tushare as ts
import pandas as pd
import numpy as np
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print

SUBTYPE = {'D':'day',
           'W':'week',
           'M':'month'}

SOURCE = {'close_hfq':'收盘后复权',
          }

class Indicator():
    """
    指标的基本类
    """

    def __init__(self,ts_code,reload=False,fill=True,update_csv=True):
        """
        ts_code:<str> e.g. '000001.SH'
        reload:<bool> True: igonre the csv, generate the df from the begining
        fill:<bool> True: according to the source date fill the df up to date
        update_csv:<bool> True: update the csv; False: keep the original csv as it is
        """
        self.ts_code = ts_code
        self.idt_df = None #df存放指标的结果数据 index; result
        self.idt_type = None #指标的类型，如MA
        self.source = None #数据源，如'close_hfq'收盘后复权
        print("[L15] 未完")
    
    def load_idt_df(self):
        """
        将历史指标的数据载入，并补全到和source同时间
        """
        print("[L20] 未完")

    def calc_res(self):
        """
        计算idt_df要补完的数据
        """
        print('[Note] Indicator.calc_res() 需要分别在各指标类中重构')
        
class Ma(Indicator):
    """
    移动平均线
    """
    def __new__(cls,ts_code,period,source='close_hfq',reload=False,fill=True,update_csv=True,subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Ma> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code,subtype]
            add_log(20, '[fn]Ma.__new__() ts_code:{0[0]}; subtype:{0[1]} not valid', log_args)
            return
        period = int(period)
        obj = super().__new__(cls)
        return obj

    def __init__(self,ts_code,period,source='close_hfq',reload=False,fill=True,update_csv=True,subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self,ts_code=ts_code,reload=reload,fill=fill,update_csv=update_csv)
        self.idt_type = 'MA'
        self.period = period
        print("[L40] 补period类型异常")
        self.file_name = 'idt_' + subtype + '_' + subtype + str(period) + '_' + '.csv'

if __name__ == '__main__':
    it1 = Indicator('000001.SZ')
    print(type(it1))
    ma1 = Ma(ts_code='000002.SZ',period=10)
    print(type(ma1))
