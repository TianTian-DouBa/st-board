import tushare as ts
import pandas as pd
import numpy as np
from st_common import raw_data
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
#from st_common import Raw_Data, raw_data_init, SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
#from st_board import load_source_df, Stock, Index
import st_board
import weakref

class Indicator():
    """
    指标的基本类
    """
    def __new__(cls,ts_code,par_asset,reload=False,update_csv=True):
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

    def __init__(self,ts_code,par_asset,reload=False,update_csv=True):
        """
        ts_code:<str> e.g. '000001.SH'
        reload:<bool> True: igonre the csv, generate the df from the begining
        update_csv:<bool> True: update the csv; False: keep the original csv as it is
        """
        self.ts_code = ts_code
        self.df_idt = None #<df>存放指标的结果数据
        self.idt_type = None #<str>指标的类型，如'MA'
        self.source = None #<str>数据源，如'close_hfq'收盘后复权
        self.file_name = None
        self.file_path = None
        self.par_asset = par_asset #<Asset>父asset对象

    def load_sources(self,nrows=None):
        """
        调用st_board.load_source_df()来准备计算用原数据
        ts_code:<str> e.g. '000001.SH'
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        retrun:<df> trade_date(index); close; high..., None if failed
        """
        from st_board import load_source_df
        df_source = load_source_df(ts_code=self.ts_code,source=self.source)
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
        df_append = self._calc_res()
        if isinstance(df_append, pd.DataFrame):
            if isinstance(self.df_idt, pd.DataFrame):
                _frames = [df_append,self.df_idt]
                df_idt = pd.concat(_frames,sort=False)
            else:
                df_idt = df_append
            if st_board.valid_file_path(self.file_path):
                df_idt.to_csv(self.file_path)
                log_args = [self.ts_code, self.file_path]
                add_log(40,"[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, saved", log_args)
            else:
                log_args = [self.ts_code, self.file_path]
                add_log(20,"[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, invalid", log_args)
            self.df_idt = df_idt
            log_args = [self.ts_code, self.file_name[:-4], len(df_idt)]
            add_log(40,"[fn]Indicator.calc_idt() ts_code:{0[0]}, {0[1]} updated; items:{0[2]}", log_args)
        elif df_append is None:
            pass #keep self.df_idt as it is
        else:
            log_args = [self.ts_code, type(self.df_idt)]
            add_log(10,"[fn]Indicator.calc_idt() ts_code:{0[0]}, type(df_append):{0[1]}, unknown problem", log_args)
            pass #keep self.df_idt as it is


    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        print('[Note] Indicator._calc_res() 需要分别在各指标类中重构')
        return False #清除VS_CODE报的问题
        
class Ma(Indicator):
    """
    移动平均线
    """
    def __new__(cls,ts_code,par_asset,period,source='close_hfq',reload=False,update_csv=True,subtype='D'):
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
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset)
        return obj

    def __init__(self,ts_code,par_asset,period,source='close_hfq',reload=False,update_csv=True,subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self,ts_code=ts_code,par_asset=par_asset,reload=reload,update_csv=update_csv)
        self.idt_type = 'MA'
        self.period = period
        #print("[L97] 补period类型异常")
        self.source = source
        self.file_name = 'idt_' + ts_code + '_' + self.source + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
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
                log_args = [self.ts_code]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[idt_head_in_source + period - 1:],inplace=True)
                values = []
                rvs_rslt = []
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [self.ts_code,self.source]
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
                log_args = [self.ts_code,self.source]
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

class Ema(Indicator):
    """
    指数移动平均线
    """
    def __new__(cls,ts_code,par_asset,period,source='close_hfq',reload=False,update_csv=True,subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Ema> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code,subtype]
            add_log(10, '[fn]Ema.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        period = int(period)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset)
        return obj
    
    def __init__(self,ts_code,par_asset,period,source='close_hfq',reload=False,update_csv=True,subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self,ts_code=ts_code,par_asset=par_asset,reload=reload,update_csv=update_csv)
        self.idt_type = 'EMA'
        self.period = period
        self.source = source
        self.file_name = 'idt_' + ts_code + '_' + self.source + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
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
                log_args = [self.ts_code]
                add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Ema._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[idt_head_in_source + 1:],inplace=True)
                rvs_rslt = []
                i = 0
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [self.ts_code,self.source]
                    add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                    return
                for idx in reversed(df_source.index):
                    if i==0:
                        idt_column_name = 'EMA' + str(period)
                        past_ema = df_idt[idt_column_name][idx]
                        rvs_rslt.append(df_idt[idt_column_name][idx])
                        i += 1
                    else:
                        # Y=[2*X+(N-1)*Y’]/(N+1)
                        today_ema = (2 * df_source[source_column_name][idx] + (period - 1) * past_ema) / (period + 1)
                        past_ema = today_ema
                        rvs_rslt.append(today_ema)
        else: #.csv file not exist
            try:
                source_column_name = SOURCE_TO_COLUMN[self.source]
            except KeyError:
                log_args = [self.ts_code,self.source]
                add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                return
            rvs_rslt = []
            i = 0
            for idx in reversed(df_source.index):
                if i==0:
                    past_ema = df_source[source_column_name][idx]
                    rvs_rslt.append(df_source[source_column_name][idx])
                    i += 1
                else:
                    # Y=[2*X+(N-1)*Y’]/(N+1)
                    today_ema = (2 * df_source[source_column_name][idx] + (period - 1) * past_ema) / (period + 1)
                    past_ema = today_ema
                    rvs_rslt.append(today_ema)
        del rvs_rslt[0]
        iter_rslt = reversed(rvs_rslt)
        rslt = list(iter_rslt)
        index_source = df_source.index[:-1]
        idt_column_name = 'EMA' + str(period)
        data = {idt_column_name:rslt}
        df_idt_append = pd.DataFrame(data,index=index_source)
        return df_idt_append

class Macd(Indicator):
    """
    MACD
    """
    def __new__(cls,ts_code,par_asset,long_n=26,short_n=12,dea_n=9,source='close_hfq',reload=False,update_csv=True,subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Macd> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code,subtype]
            add_log(10, '[fn]Macd.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        long_n = int(long_n)
        short_n = int(short_n)
        dea_n = int(dea_n)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset)
        return obj

    def __init__(self,ts_code,par_asset,long_n=26,short_n=12,dea_n=9,source='close_hfq',reload=False,update_csv=True,subtype='D'):
        """
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self,ts_code=ts_code,par_asset=par_asset,reload=reload,update_csv=update_csv)
        self.idt_type = 'MACD'
        self.long_n = long_n
        self.short_n = short_n
        self.dea_n = dea_n
        self.source = source
        self.file_name = 'idt_' + ts_code + '_' + self.source + '_' + self.idt_type + '_' + subtype + str(long_n) + '_' + str(short_n) + '_' + str(dea_n) + '.csv'
        self.file_path = sub_path + sub_idt + '\\' + self.file_name
        self.subtype = subtype

if __name__ == '__main__':
    start_time = datetime.now()
    #raw_data_init()
    from st_board import Stock, Index
    global raw_data
    #------------------test---------------------
    stock1 = Stock(ts_code='000002.SZ')
    stock1.add_indicator('ma10',Ma,period=10)
    stock1.ma10.calc_idt()
    print(stock1.ma10.df_idt)

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
