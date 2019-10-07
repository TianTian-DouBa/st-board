import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
from st_board import Raw_Data, SUBTYPE, SOURCE
from st_board import load_source_df
from st_board import sub_path, sub_idt


class Indicator():
    """
    指标的基本类
    """
    def __new__(cls,ts_code,reload=False,fill=True,update_csv=True):
        """
        检验ts_code的有效性
        """
        if raw_data.valid_ts_code(ts_code):
            obj = super().__new__(cls)
            return obj
        else:
            log_args = [ts_code]
            add_log(10, '[fn]Indicator.__new__() ts_code "{0[0]}" invalid, instance not created', log_args)
            return

    def __init__(self,ts_code,reload=False,fill=True,update_csv=True):
        """
        ts_code:<str> e.g. '000001.SH'
        reload:<bool> True: igonre the csv, generate the df from the begining
        fill:<bool> True: according to the source date fill the df up to date
        update_csv:<bool> True: update the csv; False: keep the original csv as it is
        """
        self.ts_code = ts_code
        self.df_idt = None #df存放指标的结果数据 index; result
        self.idt_type = None #指标的类型，如MA
        self.source = None #数据源，如'close_hfq'收盘后复权
        print("[L15] 未完")

    def load_sources(self,nrows=None):
        """
        调用st_board.load_source_df()来准备计算用原数据，并计算calc_res用的end_date_str;
        ts_code:<str> e.g. '000001.SH'
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        retrun:<df> trade_date(index); close; high..., None if failed
        """
        df_source = load_source_df(ts_code=self.ts_code,source=self.source)
        print('[L40] 是否需要end_date_str待考虑')
        return df_source
    
    def load_idt(self,nrows=None):
        """
        将历史指标的数据载入，并补全到和source同时间
        """

        print("[L20] 未完")
    
    def calc_idt(self):
        """
        计算补完df_idt数据
        """
        print('[Note] Indicator.calc_idt() 需要分别在各指标类中重构')

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        print('[Note] Indicator._calc_res() 需要分别在各指标类中重构')
        
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
            add_log(10, '[fn]Ma.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        period = int(period)
        obj = super().__new__(cls, ts_code=ts_code)
        return obj

    def __init__(self,ts_code,period,source='close_hfq',reload=False,fill=True,update_csv=True,subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self,ts_code=ts_code,reload=reload,fill=fill,update_csv=update_csv)
        self.idt_type = 'MA'
        self.period = period
        print("[L97] 补period类型异常")
        self.file_name = 'idt_' + ts_code + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        print('[L111] 未完')

if __name__ == '__main__':
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)
    #------------------test---------------------
    ma1 = Ma(ts_code='000002.SZ',period=10)
    print(dir(ma1))

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
