import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print

sub_path = r".\data_csv"
STATUS_WORD = {0:'-bad-',
               1:'-good-',
               3:'-uncertain-'}
ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()

def valid_date_str_fmt(date_str):
    """验证date_str形式格式是否正确
    date_str:<str> e.g. '20190723' YYYYMMDD
    return:<bool> True=valid
    """
    if isinstance(date_str,str):
        if len(date_str) == 8:
            return True
    return False

def today_str():
    """
    return: <str> today in 'YYYYMMDD' e.g.'20190712'
    """
    dt = datetime.now()
    today_str = dt.strftime("%Y%m%d")
    return today_str

def date_to_date_str(dt):
    """将datetime转化为data_str
    dt:<datetime>
    return:<str> e.g. '20190723'
    """
    return dt.strftime("%Y%m%d")

def date_str_to_date(date_str):
    """将date_str转化为datetime
    date_str:<str> e.g. '20190723'
    return:<datetime>
    """
    date = datetime.strptime(date_str,"%Y%m%d")
    return date

def sgmt_daily_index_download(ts_code,start_date_str,end_date_str,size):
    """分段下载数据
    ts_code: <str> 对象的tushare代码 e.g. '399001.SZ'
    start_date_str: <str> 开始时间字符串 YYYYMMDD '19930723'
    end_date_str: <str> 结束时间字符串 YYYYMMDD '20190804'
    size: <int> 每个分段的大小 1 to xxxx
    retrun: <df> if success, None if fail
    """
    df = None
    start_date = date_str_to_date(start_date_str)
    end_date = date_str_to_date(end_date_str)
    duration = (end_date-start_date).days
    _start_str = start_date_str
    while duration > size:
        _end_time = date_str_to_date(_start_str) + timedelta(size)
        _end_str = date_to_date_str(_end_time)
        _df = ts_pro.index_daily(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
        if not isinstance(df,pd.DataFrame):
            df = _df
        else:
            _frames = [_df,df]
            df=pd.concat(_frames,ignore_index=True)
        _start_time = _end_time + timedelta(1)
        _start_str = date_to_date_str(_start_time)
        duration = duration - size
    else:
        _end_str = end_date_str
        _df = ts_pro.index_daily(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
        if not isinstance(df,pd.DataFrame):
            df = _df
        else:
            _frames = [_df,df]
            df=pd.concat(_frames,ignore_index=True)
    return df

def get_stock_list(return_df = True):
    """获取TuShare股票列表保存到stock_list.csv文件,按需反馈DataFram
    retrun_df:<bool> 是返回DataFrame数据，否返回None
    """
    file_name = "stock_list.csv"
    df = ts_pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,exchange,curr_type,list_date,delist_date')
    df.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
    if return_df != True:
        df = None
    return df

def load_stock_list():
    """从保存的stock_list.csv文件中,读入返回DataFram"""
    file_name = "stock_list.csv"
    try:
        df = pd.read_csv(sub_path + '\\' + file_name)
    except FileNotFoundError:
        add_log(20, '[fn]load_stock_list(). file not found')
        df = None
    return df

def get_daily_basic(return_df = True):
    """获取TuShare最近交易日的每日指标，保存到daily_basic.csv文件,按需反馈DataFram
    retrun_df:<bool> 是返回DataFrame数据，否返回None
    """
    file_name = "daily_basic.csv"
    _trade_date = '20190712'
    df = ts_pro.daily_basic(ts_code='', trade_date=_trade_date, fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,free_share,total_mv,circ_mv')
    df.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
    if return_df != True:
        df = None
    return df

# def que_index_daily(ts_code,start_date=None,end_date=None):
#     """通过Tushare index_daily接口获取指标日线数据
#     ts_code: <str> '399001.SZ'
#     start_date: <str> '20190628', 省略为股票上市日期
#     end_date: <str> '20190719', 省略为当日最近的交易日
#     <未完成，待删除>
#     """
#     if start_date == None:
#         start_date = '20190628'
#     if end_date == None:
#         end_date = '20190719'
#     df = ts_pro.index_daily(ts_code=ts_code,start_date=start_date,end_date=end_date)
#     return df

class Raw_Data():
    """    """
    def __init__(self, pull=False):
        self.trade_calendar = Trade_Calendar(pull)
        self.index = Index(pull) #指数相关数据
        self.stock_list = None
    
    def valid_ts_code(self, ts_code):
        """验证在raw_data内ts_code是否有效,
        包含index,
        return: <bool> True=valid
        """
        #--------------------Index---------------
        try:
            name = self.index.idx_ts_code.loc[ts_code]['name']
        except KeyError:
            log_args = [ts_code]
            add_log(30, '[fn]Raw_Data.valid_ts_code(). ts_code "{0[0]}" invalid', log_args)
            return False
        if isinstance(name,str):
            if len(name) > 0:
                return True
        return False

class Trade_Calendar():
    """    """
    def __init__(self, pull=False):
        self.df = None #交易日数据
        if pull == True:
            self.get()
        else:
            self.load()
    
    def load(self):
        """load trade_calendar.csv文件，读入self.df
        """
        file_name = "trade_calendar.csv"
        try:
            self.df = pd.read_csv(sub_path + '\\' + file_name)
        except FileNotFoundError:
            add_log(20, '[fn]Trade_Calendar.load(). file not found')
            self.df = None
        return
    
    def get(self):
        """获取TuShare的交易日历数据,保存到trade_calendar.csv文件；
        日历会更新到当年的年底"""
        file_name = "trade_calendar.csv"
        self.df = ts_pro.trade_cal(fields='cal_date,is_open,pretrade_date')
        self.df.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        return
    
    def valid(self):
        """to valid trade_calendar
        return: True or False"""
        if isinstance(self.df,pd.DataFrame):
            return True
        else:
            return False
    
    def last_trade_day(self, dt_str = None):
        """查询指定日最近的交易日，返回日期字符串
        dt_str: <string> in 'YYYYMMDD' e.g.'20190721'
                None 代表当日
        return: <string> in 'YYYYMMDD' e.g.'20190719'
        """
        if not self.valid():
            add_log(20, '[fn]Trade_Calendar.last_trade_day() df not valid')
            return None
        if dt_str == None:
            dt = datetime.now()
            dt_str = dt.strftime("%Y%m%d")
        if isinstance(dt_str,str) and (len(dt_str) == 8):
            tdf = self.df.set_index(['cal_date'])
            try:
                is_open = tdf.loc[dt_str]['is_open']
                if is_open == 1:
                    return dt_str
                elif is_open == 0:
                    pretrade_date = tdf.loc[dt_str]['pretrade_date']
                    return pretrade_date
                else:
                    return None
            except KeyError:
                 log_args = [dt_str]
                 add_log(20, '[fn]Trade_Calendar.last_trade_day() dt_str "{0[0]}" incorrect format', log_args)
                 return None
        else:
            log_args = [dt_str]
            add_log(20, '[fn]Trade_Calendar.last_trade_day() dt_str "{0[0]}" incorrect format', log_args)
            return None

class Index():
    """指数相关，包括行业板块指数等"""
    def __init__(self, pull=False):
        self.index_basic_df = None
        self.idx_ts_code = None #<df> ts_code indexed
        self.valid = {'index_basic_sse':STATUS_WORD[3], #上交所
                      'index_basic_szse':STATUS_WORD[3], #深交所
                      'index_basic_sw':STATUS_WORD[3]} #申万
        self._sse = None
        self._szse = None
        self._sw = None
        if pull == True:
            self.get_index_basic()
        else:
            self.load_index_basic()
        self._idx_ts_code()
    
    def get_index_basic(self):
        """从ts_pro获取指数的基本信息列表
        待续：获取数据失败时，self.valid对应项的设-bad-处理
        """
        #上交所指数
        file_name = "index_basic_sse.csv"
        self._sse = ts_pro.index_basic(market='SSE')
        self.valid['index_basic_sse']=STATUS_WORD[1]
        self._sse.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #深交所指数
        file_name = "index_basic_szse.csv"
        self._szse = ts_pro.index_basic(market='SZSE')
        self.valid['index_basic_szse']=STATUS_WORD[1]
        self._szse.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #申万指数
        file_name = "index_basic_sw.csv"
        self._sw = ts_pro.index_basic(market='SW')
        self.valid['index_basic_sw']=STATUS_WORD[1]
        self._sw.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        self._update_index_basic_df()
        return
    
    def load_index_basic(self):
        """load index_basic.csv文件，读入self.index_basic_df
        """
        #上交所指数
        file_name = "index_basic_sse.csv"
        try:
            self._sse = pd.read_csv(sub_path + '\\' + file_name,dtype = {'base_date':str, 'list_date':str})
            self.valid['index_basic_sse']=STATUS_WORD[1]
        except FileNotFoundError:
            log_args = [file_name]
            add_log(20, '[fn]Index.load_index_basic(). file "{0[0]}" not found', log_args)
            self._sse = None
        #深交所指数
        file_name = "index_basic_szse.csv"
        try:
            self._szse = pd.read_csv(sub_path + '\\' + file_name,dtype = {'base_date':str, 'list_date':str})
            self.valid['index_basic_szse']=STATUS_WORD[1]
        except FileNotFoundError:
            log_args = [file_name]
            add_log(20, '[fn]Index.load_index_basic(). file "{0[0]}" not found', log_args)
            self._szse = None
        #申万指数
        file_name = "index_basic_sw.csv"
        try:
            self._sw = pd.read_csv(sub_path + '\\' + file_name,dtype = {'base_date':str, 'list_date':str})
            self.valid['index_basic_sw']=STATUS_WORD[1]
        except FileNotFoundError:
            log_args = [file_name]
            add_log(20, '[fn]Index.load_index_basic(). file "{0[0]}" not found', log_args)
            self._sw = None
        self._update_index_basic_df()
        return
    
    def que_list_date(self, ts_code):
        """查询上市时间list_date
        return:<str> e.g. '19930503'
        ts_code:<str> e.g. '000001.SH'
        """
        #try:
        result = self.idx_ts_code.loc[ts_code]['list_date']
        return result
    
    def get_index_daily(self, ts_code, reload = False):
        """通过ts_pro.index_daily下载指数的日线数据到daily_data\<ts_code>.csv文件
        ts_code: <str> '399001.SZ'
        reload: <bool> True=重头开始下载
        retrun: <df> if success, None if fail
        """
        QUE_LIMIT = 100 #每次查询返回的条目限制
        sub_path_2nd = r"\daily_data"
        if raw_data.valid_ts_code(ts_code):
            if reload == True: #重头开始下载
                start_date_str = self.que_list_date(ts_code)
                end_date_str = today_str()
                #to be continued
                df = sgmt_daily_index_download(ts_code,start_date_str,end_date_str,QUE_LIMIT)
                if isinstance(df, pd.DataFrame):
                    file_name = 'd_' + ts_code + '.csv'
                    df.to_csv(sub_path + sub_path_2nd + '\\' + file_name)
                    return df
                else:
                    log_args = [ts_code,df]
                    add_log(20, '[fn]Index.get_index_daily() failed to get DataFrame. ts_code: "{0[0]}" df: ', log_args)
                    return None
            else: #reload != True 读入文件，看最后条目的日期，继续下载数据
                df = self.load_index_daily(ts_code)
                if isinstance(df, pd.DataFrame):
                    last_date_str = df.iloc[0]['trade_date']
                    last_date = date_str_to_date(last_date_str)
                    today_str_ = today_str()
                    today = date_str_to_date(today_str_) #只保留日期，忽略时间差别
                    start_date = last_date + timedelta(1)
                    _start_str = date_to_date_str(start_date)
                    _end_str = today_str_
                    if last_date < today:
                        _df = sgmt_daily_index_download(ts_code,_start_str,_end_str,QUE_LIMIT)
                        #_df = que_index_daily(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
                        _frames = [_df,df]
                        df=pd.concat(_frames,ignore_index=True)
                        file_name = 'd_' + ts_code + '.csv'
                        df.to_csv(sub_path + sub_path_2nd + '\\' + file_name)
                        return df
                else:
                    log_args = [ts_code]
                    add_log(20, '[fn]Index.get_index_daily() ts_code "{0[0]}" load csv fail', log_args)
                    return
                
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.get_index_daily() ts_code "{0[0]}" invalid', log_args)
            return
        
    def load_index_daily(self, ts_code):
        """从文件读入指数日线数据
        return: <df>
        """
        sub_path_2nd = r"\daily_data"
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            result = pd.read_csv(sub_path + sub_path_2nd + '\\' + file_name,dtype={'trade_date':str},usecols=['ts_code','trade_date','close','open','high','low','pre_close','change','pct_chg','vol','amount'],index_col=False)
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_index_daily() ts_code "{0[0]}" invalid', log_args)
            return

    def _idx_ts_code(self):
        """以self.index_basic_df为基础，以ts_code字段创建index"""
        self.idx_ts_code = self.index_basic_df.set_index('ts_code')

    def _update_index_basic_df(self):
        """将self._sse等内部<df>合并读入self.index_basic_df"""
        _frames = []
        if self.valid['index_basic_sse'] == STATUS_WORD[1]: #good
            _frames.append(self._sse)
        if self.valid['index_basic_szse'] == STATUS_WORD[1]: #good
            _frames.append(self._szse)
        if self.valid['index_basic_sw'] == STATUS_WORD[1]: #good
            _frames.append(self._sw)
        if len(_frames) > 0:
            self.index_basic_df = pd.concat(_frames, ignore_index=True)


if __name__ == "__main__":
    #df = get_stock_list()
    #df = load_stock_list()
    #df = get_daily_basic()
    #cl = get_trade_calendar()
    #last_trad_day_str()
    raw_data = Raw_Data(pull=False)
    #c = raw_data.trade_calendar
    index = raw_data.index
    #zs = que_index_daily(ts_code="000009.SH",start_date="20031231")
    ttt = index.get_index_daily('399003.SZ',reload=False)
