import tushare as ts
import pandas as pd
import numpy as np
from st_common import raw_data
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
#from st_common import Raw_Data, raw_data_init, SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
from st_board import load_source_df
import st_board

class Indicator():
    """
    指标的基本类
    """
    def __new__(cls,ts_code,reload=False,fill=True,update_csv=True):
        """
        检验ts_code的有效性
        """
        global raw_data
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
        self.df_idt = None #<df>存放指标的结果数据
        self.idt_type = None #<str>指标的类型，如'MA'
        self.source = None #<str>数据源，如'close_hfq'收盘后复权
        self.file_name = None
        self.file_path = None
        print("[L40] 未完")

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
        将历史指标的数据载入;
        """
        file_path = self.file_path #用到的file_path需要在具体的继承子类中定义
        try:
            df_idt = pd.read_csv(file_path,dtype={'trade_date':str},index_col='trade_date',nrows=nrows)
        except FileNotFoundError:
            log_args = [file_path]
            add_log(30, '[fn]Indicator.load_idt() file "{0[0]}" not exist', log_args)
            return None
        self.df_idt = df_idt
        return self.df_idt
    
    def calc_idt(self):
        """
        调用self._calc_res()补完df_idt数据
        """
        df_idt = self._calc_res()
        if isinstance(df_idt, pd.DataFrame):
            if st_board.valid_file_path(self.file_path):
                df_idt.to_csv(self.file_path)
            else:
                log_args = [ts_code, file_path]
                add_log(40,"[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, invalid", log_args)
            self.df_idt = df_idt
        elif df_idt is None:
            log_args = [ts_code]
            add_log(30,"[fn]Indicator.calc_idt() ts_code:{0[0]}, return None to .df_idt", log_args)
            self.df_idt = None
        else:
            log_args = [ts_code, type(df_idt)]
            add_log(10,"[fn]Indicator.calc_idt() ts_code:{0[0]}, type(df_idt):{0[1]}, unknown problem", log_args)
            self.df_idt = None


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
        #print("[L97] 补period类型异常")
        self.source = 'close_hfq'
        self.file_name = 'idt_' + ts_code + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
        self.file_path = sub_path + sub_idt + '\\' + self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period=self.period
        if isinstance(df_idt, pd.DataFrame):
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str) #idt head position in df_source
            except KeyError:
                log_args = [ts_code]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [ts_code]
                add_log(40, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[:idt_head_in_source + period - 1],inplace=True)
                values = []
                rvs_rslt = []
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [ts_code,source]
                    add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                    return
                for idx in reversed(df_source.index):
                    values.append(df_source[source_column_name][idx])
                    if len(values) > period:
                        del values[0]
                    if len(values) == period:
                        rvs_rslt.append(np.average(values))
        else: #.csv file not exist
            try:
                source_column_name = SOURCE_TO_COLUMN[self.source]
            except KeyError:
                log_args = [ts_code,source]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                return
            values = []
            rvs_rslt = []
            for idx in reversed(df_source.index):
                values.append(df_source[source_column_name][idx])
                if len(values) > period:
                    del values[0]
                if len(values) == period:
                    rvs_rslt.append(np.average(values))
        iter_rslt = reversed(rvs_rslt)
        rslt = list(iter_rslt)
        index_source = df_source.index[:len(df_source)-period+1]
        idt_column_name = 'MA' + str(period)
        data = {idt_column_name:rslt}
        df_idt_append = pd.DataFrame(data,index=index_source)
        return df_idt_append

if __name__ == '__main__':
    start_time = datetime.now()
    #raw_data_init()
    global raw_data
    print("[L185]: raw_data:{}".format(raw_data))
    #------------------test---------------------
    ma10 = Ma(ts_code='000002.SZ',period=10)
    ma10.calc_idt()
    print(ma10.df_idt)

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
