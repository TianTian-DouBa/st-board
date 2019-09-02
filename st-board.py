import tushare as ts
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters() #否则Warning

sub_path = r".\data_csv"
sub_path_2nd_daily = r"\daily_data" #日线数据
sub_path_config = r"\config" #配置文件
sub_path_al = r"\assets_lists" #资产列表
sub_path_result = r".\plot_result" #分析模板运行结果

STATUS_WORD = {0:'-bad-',
               1:'-good-',
               3:'-uncertain-'}
DOWNLOAD_WORD = {0:'-success-',
                 1:'-fail-',
                 3:'-unknow-'}
DEFAULT_OPEN_DATE_STR = "19901219" #中国股市开始时间？
ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()

class Index():
    """指数相关，包括行业板块指数等"""
    def __init__(self, pull=False):
        """
        pull: True=get from Tushare; False=load from file
        """
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
    
    @staticmethod
    def get_sw_index_classify(return_df = True):
        """从ts_pro获取申万行业指数的分类
        """
        #一级行业列表
        file_name = "index_sw_L1_list.csv"
        df_l1 = ts_pro.index_classify(level='L1', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l1.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #二级行业列表
        file_name = "index_sw_L2_list.csv"
        df_l2 = ts_pro.index_classify(level='L2', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l2.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #三级行业列表
        file_name = "index_sw_L3_list.csv"
        df_l3 = ts_pro.index_classify(level='L3', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l3.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        if return_df != True:
            return None
        return (df_l1, df_l2, df_l3)

    def que_list_date(self, ts_code):
        """查询上市时间list_date
        return:<str> e.g. '19930503'
        ts_code:<str> e.g. '000001.SH'
        """
        #try:
        try:
            result = self.idx_ts_code.loc[ts_code]['list_date']
        except KeyError:
            log_args = [ts_code]
            add_log(20, '[fn]:Index.que_list_date() ts_code: "{0[0]}" was not found in Index.idx_ts_code. use DEFAULT_OPEN_DATE_STR instead', log_args)
            result = DEFAULT_OPEN_DATE_STR
            return result
        if valid_date_str_fmt(result):
            return result
        else:
            result = self.idx_ts_code.loc[ts_code]['base_date']
            if valid_date_str_fmt(result):
                log_args = [ts_code, result]
                add_log(40, '[fn]:Index.que_list_date() ts_code: "{0[0]}" used "base_date" {0[1]} instead "list_date".', log_args)
                return result
            else:
                result = DEFAULT_OPEN_DATE_STR
                log_args = [ts_code, result]
                add_log(40, '[fn]:Index.que_list_date() ts_code: "{0[0]}" used "DEFAULT_OPEN_DATE_STR" {0[1]} instead "list_date".', log_args)
                return result
    
    # def get_index_daily(self, ts_code, handler_s, reload = False):
    #     r"""通过调用sgmt_daily_index_download下载指数的日线数据到daily_data\<ts_code>.csv文件
    #     ts_code: <str> '399001.SZ'
    #     handler_s: <str> in HANDLER e.g. 'index', 'sw_daily'
    #     reload: <bool> True=重头开始下载
    #     retrun: <df> if success, None if fail
    #     """
    #     global sub_path_2nd_daily
    #     READER = {'index':self.load_index_daily,
    #               'sw_daily':self.load_sw_daily}
    #     QUE_LIMIT = 8000 #每次查询返回的条目限制
    #     try:
    #         handler = HANDLER[handler_s] #debug try catch wrong handler
    #     except KeyError:
    #         log_args = [ts_code, handler_s]
    #         add_log(20, '[fn]get_index_daily(). ts_code: "{0[0]}" handler_s: "{0[1]}" incorrect',log_args)
    #         return
    #     if raw_data.valid_ts_code(ts_code):
    #         if reload == True: #重头开始下载
    #             start_date_str = self.que_list_date(ts_code)
    #             end_date_str = today_str()
    #             df = sgmt_daily_index_download(ts_code,start_date_str,end_date_str,QUE_LIMIT,handler)
    #             if isinstance(df, pd.DataFrame):
    #                 file_name = 'd_' + ts_code + '.csv'
    #                 df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
    #                 return df
    #             else:
    #                 log_args = [ts_code,df]
    #                 add_log(20, '[fn]Index.get_index_daily() failed to get DataFrame. ts_code: "{0[0]}" df: ', log_args)
    #                 return None
    #         else: #reload != True 读入文件，看最后条目的日期，继续下载数据
    #             reader = READER[handler_s]
    #             df = reader(ts_code)
    #             if isinstance(df, pd.DataFrame):
    #                 try:
    #                     last_date_str = df.iloc[0]['trade_date']
    #                 except IndexError:
    #                     last_date_str = self.que_list_date(ts_code)
    #                 last_date = date_str_to_date(last_date_str)
    #                 today_str_ = today_str()
    #                 today = date_str_to_date(today_str_) #只保留日期，忽略时间差别
    #                 start_date = last_date + timedelta(1)
    #                 _start_str = date_to_date_str(start_date)
    #                 _end_str = today_str_
    #                 if last_date < today:
    #                     _df = sgmt_daily_index_download(ts_code,_start_str,_end_str,QUE_LIMIT,handler)
    #                     _frames = [_df,df]
    #                     df=pd.concat(_frames,ignore_index=True)
    #                     file_name = 'd_' + ts_code + '.csv'
    #                     df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
    #                     if logable(40):
    #                         number_of_items = len(df)
    #                         log_args = [ts_code, number_of_items]
    #                         add_log(40,"[fn]Index.get_index_daily() ts_code: {0[0]}, total items: {0[1]}", log_args)
    #                     return df
    #             else:
    #                 log_args = [ts_code]
    #                 add_log(20, '[fn]Index.get_index_daily() ts_code "{0[0]}" load csv fail', log_args)
    #                 return                
    #     else:
    #         log_args = [ts_code]
    #         add_log(20, '[fn]Index.get_index_daily() ts_code "{0[0]}" invalid', log_args)
    #         return

    @staticmethod    
    def load_index_daily(ts_code,nrows=None):
        """从文件读入指数日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        sub_path_2nd_daily = r"\daily_data"
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            result = pd.read_csv(sub_path + sub_path_2nd_daily + '\\' + file_name,dtype={'trade_date':str},usecols=['ts_code','trade_date','close','open','high','low','pre_close','change','pct_chg','vol','amount'],index_col=False,nrows=nrows)
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_index_daily() ts_code "{0[0]}" invalid', log_args)
            return
    
    @staticmethod
    def load_sw_daily(ts_code, nrows=None):
        """从文件读入指数日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        sub_path_2nd_daily = r"\daily_data"
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            try:
                result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['ts_code','trade_date','name','open','low','high','close','change','pct_change','vol','amount','pe','pb'],index_col=False,nrows=nrows)
            except FileNotFoundError:
                log_args = [file_path]
                add_log(20, '[fn]Index.load_sw_daily() "{0[0]}" not exist', log_args)
                return
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_sw_daily() ts_code "{0[0]}" invalid', log_args)
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

class Stock():
    """股票类的资产"""

    def __init__(self, pull=False):
        self.basic = None
        if pull == True:
            self.get_stock_basic()
        else:
            self.load_stock_basic()

    def get_stock_basic(self):
        """
        从ts_pro.stock_basic获取个股的基本信息列表
        return:<df> is success, None if fail
        """
        file_name = 'stock_basic.csv'
        file_path = sub_path + '\\' + file_name
        self.basic = ts_pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        if isinstance(self.basic, pd.DataFrame):
            if len(self.basic) > 10:
                self.basic.set_index('ts_code',inplace=True)
                self.basic.to_csv(file_path,encoding='utf-8')
                log_args = [file_path]
                add_log(40, '[fn]:Stock.get_stock_basic() file "{0[0]}" saved', log_args)
                return self.basic
        log_args = [file_path]
        add_log(20, '[fn]:Stock.get_stock_basic() failed to save file "{0[0]}"', log_args)
        return
    
    def load_stock_basic(self):
        """
        从stock_basic.csv文件读入个股的基本信息列表
        return:<df> is success, None if fail
        """
        file_name = "stock_basic.csv"
        file_path = sub_path + '\\' + file_name
        try:
            self.basic = pd.read_csv(file_path,dtype={'symbol':str,'list_date':str,'delist_date':str},index_col='ts_code')
            return self.basic
        except FileNotFoundError:
            log_args = [file_path]
            add_log(20, '[fn]Stock.get_stock_basic()e. file "{0[0]}" not found', log_args)
            return



#LOADER读入.csv数据的接口
LOADER = {'index_sse':Index.load_index_daily,
          'index_szse':Index.load_index_daily,
          'index_sw':Index.load_sw_daily}
#GETTER从Tushare下载数据的接口
GETTER = {'index_sse':ts_pro.index_daily,
          'index_szse':ts_pro.index_daily,
          'index_sw':ts_pro.sw_daily}

# 'handler_s':handler
# HANDLER = {'index':ts_pro.index_daily,
#           'sw_daily':ts_pro.sw_daily}

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

def download_data(ts_code,category,reload=False):
    r"""通过调用sgmt_daily_index_download下载资产数据到数据文e.g. daily_data\d_<ts_code>.csv文件
        ts_code: <str> '399001.SZ'
        category: <str> in READER e.g. 'Index.load_index_daily'
        reload: <bool> True=重头开始下载
        retrun: <df> if success, None if fail
    """
    QUE_LIMIT = 8000 #每次查询返回的条目限制,假定所有loader都相同
    try:
        loader = LOADER[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]download_data(). ts_code: "{0[0]}" category: "{0[1]}" incorrect',log_args)
        return
    if raw_data.valid_ts_code(ts_code):
        if reload == True: #重头开始下载
            start_date_str = raw_data.index.que_list_date(ts_code)
            end_date_str = today_str()
            df = sgmt_download(ts_code,start_date_str,end_date_str,QUE_LIMIT,category)
            if isinstance(df, pd.DataFrame):
                file_name = 'd_' + ts_code + '.csv'
                df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                if logable(40):
                        number_of_items = len(df)
                        log_args = [ts_code, number_of_items]
                        add_log(40,"[fn]download_data() ts_code: {0[0]}, total items: {0[1]}", log_args)
                return df
            else:
                log_args = [ts_code,df]
                add_log(20, '[fn]download_data() fail to get DataFrame from Tushare. ts_code: "{0[0]}" df: ', log_args)
                return None
        else: #reload != True 读入文件，看最后条目的日期，继续下载数据
            loader = LOADER[category]
            df = loader(ts_code)
            if isinstance(df, pd.DataFrame):
                try:
                    last_date_str = df.iloc[0]['trade_date'] #注意是否所有类型都有'trade_date'字段
                except IndexError:
                    last_date_str = raw_data.index.que_list_date(ts_code)
                last_date = date_str_to_date(last_date_str)
                today_str_ = today_str()
                today = date_str_to_date(today_str_) #只保留日期，忽略时间差别
                start_date = last_date + timedelta(1)
                _start_str = date_to_date_str(start_date)
                _end_str = today_str_
                if last_date < today:
                    _df = sgmt_download(ts_code,_start_str,_end_str,QUE_LIMIT,category)
                    _frames = [_df,df]
                    df=pd.concat(_frames,ignore_index=True)
                    file_name = 'd_' + ts_code + '.csv'
                    df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                    if logable(40):
                        number_of_items = len(df)
                        log_args = [ts_code, number_of_items]
                        add_log(40,"[fn]download_data() ts_code: {0[0]}, total items: {0[1]}", log_args)
                    return df
            else:
                log_args = [ts_code]
                add_log(20, '[fn]download_data() ts_code "{0[0]}" load csv fail', log_args)
                return                
    else:
        log_args = [ts_code]
        add_log(20, '[fn]download_data(). ts_code "{0[0]}" invalid', log_args)
        return

def sgmt_download(ts_code,start_date_str,end_date_str,size,category):
    """通过TuShare API分段下载数据
    ts_code: <str> 对象的tushare代码 e.g. '399001.SZ'
    start_date_str: <str> 开始时间字符串 YYYYMMDD '19930723'
    end_date_str: <str> 结束时间字符串 YYYYMMDD '20190804'
    size: <int> 每个分段的大小 1 to xxxx
    category: <str> listed in HANDLER e.g. ts_pro.index_daily, ts_pro.sw_daily
    retrun: <df> if success, None if fail
    """
    TRY_TIMES = 20
    SLEEP_TIME = 20
    try:
        getter = GETTER[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]sgmt_download(). ts_code: "{0[0]}" category: "{0[1]}" incorrect',log_args)
        return
    df = None
    start_date = date_str_to_date(start_date_str)
    end_date = date_str_to_date(end_date_str)
    duration = (end_date-start_date).days
    _start_str = start_date_str
    while duration > size:
        _end_time = date_str_to_date(_start_str) + timedelta(size)
        _end_str = date_to_date_str(_end_time)
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
            except Exception as e: #ConnectTimeout, 每分钟200:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
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
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
            except Exception as e: #ConnectTimeout:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
        if not isinstance(df,pd.DataFrame):
            df = _df
        else:
            _frames = [_df,df]
            df=pd.concat(_frames,ignore_index=True)
    return df

# def sgmt_daily_index_download(ts_code,start_date_str,end_date_str,size,handler):
#     """通过TuShare API分段下载数据
#     ts_code: <str> 对象的tushare代码 e.g. '399001.SZ'
#     start_date_str: <str> 开始时间字符串 YYYYMMDD '19930723'
#     end_date_str: <str> 结束时间字符串 YYYYMMDD '20190804'
#     size: <int> 每个分段的大小 1 to xxxx
#     handler: <tushare API> listed in HANDLER e.g. ts_pro.index_daily, ts_pro.sw_daily
#     retrun: <df> if success, None if fail
#     """
#     TRY_TIMES = 20
#     SLEEP_TIME = 20
#     df = None
#     start_date = date_str_to_date(start_date_str)
#     end_date = date_str_to_date(end_date_str)
#     duration = (end_date-start_date).days
#     _start_str = start_date_str
#     while duration > size:
#         _end_time = date_str_to_date(_start_str) + timedelta(size)
#         _end_str = date_to_date_str(_end_time)
#         _try = 0
#         while _try < TRY_TIMES:
#             try:
#                 _df = handler(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
#                 #--------debug-------------
#                 #print("[debug]L78: ts_code:{} , start_date:{}, end_date:{}".format(ts_code, _start_str, _end_str))
#                 #len__df = len(_df)
#                 #print("[debug]L81: len__df:{}".format(len__df))
#             except Exception as e: #ConnectTimeout, 每分钟200:
#                 time.sleep(SLEEP_TIME)
#                 _try += 1
#                 log_args = [ts_code, _try, e.__class__.__name__, e]
#                 add_log(40, '[fn]sgmt_daily_index_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
#                 add_log(40, '[fn]sgmt_daily_index_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
#                 continue
#             break
#         if not isinstance(df,pd.DataFrame):
#             df = _df
#         else:
#             _frames = [_df,df]
#             df=pd.concat(_frames,ignore_index=True)
#         _start_time = _end_time + timedelta(1)
#         _start_str = date_to_date_str(_start_time)
#         duration = duration - size
#     else:
#         _end_str = end_date_str
#         _try = 0
#         while _try < TRY_TIMES:
#             try:
#                 _df = handler(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
#                 #--------debug-------------
#                 #print("[debug]L103: ts_code:{} , start_date:{}, end_date:{}, handler:{}".format(ts_code, _start_str, _end_str, handler))
#                 #len__df = len(_df)
#                 #print("[debug]L105: len__df:{}".format(len__df))
#             except Exception as e: #ConnectTimeout:
#                 time.sleep(SLEEP_TIME)
#                 _try += 1
#                 log_args = [ts_code, _try, e.__class__.__name__, e]
#                 add_log(40, '[fn]sgmt_daily_index_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
#                 add_log(40, '[fn]sgmt_daily_index_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
#                 continue
#             break
#         if not isinstance(df,pd.DataFrame):
#             df = _df
#         else:
#             _frames = [_df,df]
#             df=pd.concat(_frames,ignore_index=True)
#     return df

def bulk_download(download_file, reload=False):
    r"""根据资产列表文件，批量下载数据到csv文件
    download_file:<str> path for al file e.g. r'.\data_csv\assets_lists\al_<download_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    file_path = None
    if isinstance(download_file,str):
        if len(download_file)>0:        
            file_name = 'al_' + download_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path == None:
        log_args = [download_file]
        add_log(10, '[fn]bulk_download(). invalid download_file: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_download(). file "{0[0]}" not found',log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_download(). df_al loaded -sucess, items:"{0[0]}"',log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            name,_type,stype1,stype2 = raw_data.all_assets_list.loc[ts_code][['name','type','stype1','stype2']]
            category = None #资产的类别，传给下游[fn]处理
            #--------------申万指数---------------
            if _type == 'index' and stype1=='SW':
                category = 'index_sw'
            #--------------上证指数---------------
            if _type == 'index' and stype1=='SSE':
                category = 'index_sse'
            #--------------深圳指数---------------
            if _type == 'index' and stype1=='SZSE':
                category = 'index_szse'
            #--------------其它类型(未完成)----------
            if category == None:
                log_args = [ts_code]
                add_log(20, '[fn]bulk_download(). No matched category for "{0[0]}"',log_args)
                continue
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            if reload==True or (not os.path.exists(file_path)):
                _reload = True
            else:
                _reload = False
            download_data(ts_code, category, _reload)

# def bulk_download_1(download_file, reload=False):
#     r"""根据下载列表配置文件，批量下载数据到csv文件
#     download_file:<str> path for configure file e.g. r'.\data_csv\download_cnfg.csv'
#     reload:<bool> True重新下载完整文件
#     return:<df> of download configure file is success; None if failed
#     """
#     try:
#         df_cnfg = pd.read_csv(download_file, index_col=False)
#     except FileNotFoundError:
#         log_args = [download_file]
#         add_log(10, '[fn]bulk_download(). file "{0[0]}" not found',log_args)
#         df_cnfg = None
#         return None
#     df_cnfg['status']=DOWNLOAD_WORD[3] #<str>'-unknow-',增加一列存放数据下载的状态
#     for _, row in df_cnfg.iterrows():
#         #print("ts_code: " + ts_code + "    type: " + _type)
#         if row['selected'] == 'x' or row['selected'] == 'X':
#             #print("debug#124: {}".format(row))
#             ts_code, handler_s = row['ts_code'], row['handler']
#             #print("debug#131 ts_code='{}' handler_s='{}'".format(ts_code,handler_s))
#             file_name = 'd_' + ts_code + '.csv'
#             file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
#             if reload==True or (not os.path.exists(file_path)):
#                 _reload = True
#             else:
#                 _reload = False
#             df_daily = raw_data.index.get_index_daily(ts_code, handler_s, _reload)
#             if isinstance(df_daily,pd.DataFrame):
#                 row['status'] = DOWNLOAD_WORD[0] #'-success-'
#             else:
#                 row['status'] = DOWNLOAD_WORD[1] #'-fail-'
#             # #----指数类型----
#             # if handler == 'index':
#             #     file_name = 'd_' + ts_code + '.csv'
#             #     file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
#             #     if reload==True or (not os.path.exists(file_path)):
#             #         _reload = True
#             #     else:
#             #         _reload = False
#             #     df_daily = raw_data.index.get_index_daily(ts_code, _reload)
#             #     if isinstance(df_daily,pd.DataFrame):
#             #         row['status'] = DOWNLOAD_WORD[0] #'-success-'
#             #     else:
#             #         row['status'] = DOWNLOAD_WORD[1] #'-fail-'
#             # #----申万指数类型----
#             # if handler == 'sw_daily':
#             #     print("其它类型待继续 line-114")
#             log_args = [ts_code, row['status']]
#             add_log(40, '[fn]bulk_download() ts_code: "{0[0]}"  status: "{0[1]}"', log_args)
#     return df_cnfg

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
    """存放其它模块计算时所要用的公共数据的基础模块，需要实例化填充数据后使用"""
    def __init__(self, pull=False):
        """
        pull: True=get from Tushare; False=load from file
        """
        self.trade_calendar = Trade_Calendar(pull)
        self.index = Index(pull) #指数相关数据
        self.stock = Stock(pull) #个股相关数据
        self.all_assets_list = All_Assets_List.load_all_assets_list()
        #self.stock_list = None
    
    def valid_ts_code(self, ts_code):
        """验证在raw_data内ts_code是否有效,
        return: <bool> True=valid
        """
        #--------------------Index---------------
        try:
            name = self.all_assets_list.loc[ts_code]['name']
            #print("[debug L249] name:{}".format(name))
        except KeyError:
            log_args = [ts_code]
            add_log(30, '[fn]Raw_Data.valid_ts_code(). ts_code "{0[0]}" invalid', log_args)
            return None
        if isinstance(name,str):
            if len(name) > 0:
                return True
        return None

    def valid_all_assets_list(self):
        """验证self.all_assets_list的有效性
        return: True=valid
        """
        if isinstance(self.all_assets_list, pd.DataFrame):
            if len(self.all_assets_list) > 10:
                return True
        return None

class Plot_Utility():
    """存放绘图出报告用的公用工具"""
    @staticmethod
    def gen_al(al_name=None, ts_code=None,valid='T',selected='T',type_=None,stype1=None,stype2=None):
        r"""生成资产列表文件.\assets_lists\al_<name>.csv
        al_name: default=None只返回df不生成文件；<str> 填充文件名的<name>部分
        ts_code: <str> default=None 全部，[未完成] 考虑批量传入
        valid：<str> default='T' 筛选出值为'T'的，None=全部
        selected：<str> default='T' 筛选出值为'T'的，None=全部
        type: <str> default=None 全部， 其它<str>按此筛选
        stype1: <str> default=None 全部， 其它<str>按此筛选
        stype2: <str> default=None 全部， 其它<str>按此筛选
        return: <df> or None
        """
        if raw_data.valid_all_assets_list:
            _aal = raw_data.all_assets_list #all=all_assets_list
            if ts_code == None:
                al = _aal
            else: #[未完成] 考虑批量传入
                print("[not complete L285] 考虑批量传入")
                return
            #-------valid-----------
            if valid=='T':
                al = al[(al.valid=='T')]
            elif isinstance(valid,str):
                al = al[(al.valid==valid)]
            #-------selected-----------
            if selected=='T':
                al = al[(al.selected=='T')]
            elif isinstance(selected,str):
                al = al[(al.valid==selected)]
            #-------type-----------
            if isinstance(type_,str):
                al = al[(al.type==type_)]
            #-------stype1-----------
            if isinstance(stype1,str):
                al = al[(al.stype1==stype1)]
            #-------stype2-----------
            if isinstance(stype2,str):
                al = al[(al.stype2==stype2)]
            
            al = al['selected']
        else:
            add_log(10, '[fn]Plot_Utility.gen_al(). raw_data.all_assets_list is not valid')
            return
        if isinstance(al_name,str):
            if len(al_name) > 0:
                file_name = 'al_' + al_name + '.csv'
                file_path = sub_path + sub_path_al + '\\' + file_name
                al.to_csv(file_path,encoding='utf-8',header=True) #haeder=True是Series.to_csv的处理，否则Warning
                log_args = [al_name]
                add_log(40, '[fn]Plot_Utility.gen_al(). "al_{0[0]}.csv" generated', log_args)
            else:
                log_args = [al_name]
                add_log(20, '[fn]Plot_Utility.gen_al(). al_name invalid. "al_{0[0]}.csv" file not generated', log_args)
        return al

class Plot_Assets_Racing():
    """资产竞速图表：不同资产从同一基准起跑，一定时间内的价格表现
    """
    def __init__(self,al_file,period=30):
        """
        al_file: <str> 资产表的文件名e.g.'al_SW_Index_L1.csv'
        period: <int> 比较的周期
        """
        al_path = sub_path + sub_path_al + '\\' + al_file
        try:
            al_df = pd.read_csv(al_path)
            #print("[debug L335] al_df:{}".format(al_df))
        except FileNotFoundError:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). file "{0[0]}" not found',log_args)
            return
        al_df.set_index('ts_code',inplace=True)
        #print("[debug L341] al_df:{}".format(al_df))
        self.al = al_df[al_df=='T'].index #index of ts_code
        if len(self.al) == 0:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). no item in "{0[0]}"',log_args)
            return
        fig = plt.figure()
        ax = fig.add_subplot(111)
        _aal = raw_data.all_assets_list
        self.raw_data = pd.DataFrame(columns=['ts_code','name','base_close','last_chg','df'])
        self.raw_data.set_index('ts_code',inplace=True)
        for ts_code in self.al:
            #print("[debug L349] ts_code:{}".format(ts_code))
            name,_type, stype1, stype2 = _aal.loc[ts_code][['name','type','stype1','stype2']]
            handler = None
            #--------------Index.load_sw_daily---------------
            if stype1=='SW':
                handler = Index.load_sw_daily
            #--------------handler = ts_pro.index_daily(未完成)----------
            if handler == None:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing.__init__(). No matched handler for "{0[0]}"',log_args)
                continue
            df = handler(ts_code=ts_code, nrows=period)
            #print("[debug L364] df:{}".format(df))
            if isinstance(df,pd.DataFrame):
                log_args = [ts_code]
                add_log(40, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -success-', log_args)
            else:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -fail-', log_args)
                continue
            df = df[['trade_date','close']]
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
            base_close, = df.tail(1)['close'].values
            df['base_chg_pct']=(df['close']/base_close-1)*100
            last_chg, = df.head(1)['base_chg_pct'].values
            row = pd.Series({'ts_code':ts_code,'name':name,'base_close':base_close,'last_chg':last_chg,'df':df},name=ts_code)
            #print("[L383] row:{}".format(row))
            self.raw_data=self.raw_data.append(row)
            #self.raw_data.loc[ts_code]=[name,base_close,last_chg,df]
        #print("[L383] self.raw_data:{}".format(self.raw_data))
        self.raw_data.sort_values(by='last_chg',inplace=True,ascending=False)
        result = self.raw_data[['name','last_chg']]
        print(result)
        file_name = str('资产竞速{}周期涨幅比较_{}'.format(period,al_file[3:-4])) + '_' + today_str() + '.csv'
        file_path = sub_path_result + r'\Plot_Assets_Racing' + '\\' + file_name
        result.to_csv(file_path,encoding="utf-8")
        log_args = [file_path]
        add_log(40, '[fn]:Plot_Assets_Racing() result saved to file "{}"', log_args)
        for ts_code, pen in self.raw_data.iterrows():
            name, last_chg, df = pen['name'],pen['last_chg'],pen['df']
            last_chg = str(round(last_chg,2))
            label = last_chg + '%  ' + name #  + '\n' + ts_code
            ax.plot(df.index,df['base_chg_pct'],label=label,lw=1)
        plt.legend(handles=ax.lines)
        plt.grid(True)
        mpl.rcParams['font.sans-serif'] = ['FangSong'] # 指定默认字体
        mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
        plt.xticks(df.index,rotation='vertical')
        plt.title('资产竞速{}周期涨幅比较 - {}'.format(period,al_file[3:-4]))
        plt.ylabel('收盘%')
        plt.subplots_adjust(left=0.03, bottom=0.11, right=0.85, top=0.97, wspace=0, hspace=0)
        plt.legend(bbox_to_anchor=(1, 1),bbox_transform=plt.gcf().transFigure)
        plt.show()

class All_Assets_List():
    """处理全资产列表"""

    @staticmethod
    def load_all_assets_list():
        """从all_asstes_list.csv中读取全代码列表
        """
        file_name = "all_assets_list.csv"
        file_path = sub_path + sub_path_config + '\\' + file_name
        try:
            df = pd.read_csv(file_path,index_col='ts_code')
        except FileNotFoundError:
            log_args = [file_path]
            add_log(10, '[fn]load_all_assets_list. "{0[0]}" not found', log_args)
            df = None
        return df

    @staticmethod
    def rebuild_all_assets_list(que_from_ts = False):
        """重头开始构建全资产列表
        que_from_ts: <bool> F：从文件读 T:从tushare 接口读
        """
        file_name = "all_assets_list_rebuild.csv" #不同名避免误操作
        file_path_al = sub_path + sub_path_config + '\\' + file_name
        df_al = pd.DataFrame(columns=['ts_code','valid','selected','name','type','stype1','stype2'])
        df_al = df_al.set_index('ts_code')
        #--------------SW 指数---------------
        if que_from_ts == True:
            df_l1,df_l2,df_l3 = Index.get_sw_index_classify()
            df_l1=df_l1[['index_code','industry_name']]
            df_l2=df_l2[['index_code','industry_name']]
            df_l3=df_l3[['index_code','industry_name']]
        else:     
            file_path_sw_l1 = sub_path + '\\' + 'index_sw_L1_list.csv'
            file_path_sw_l2 = sub_path + '\\' + 'index_sw_L2_list.csv'
            file_path_sw_l3 = sub_path + '\\' + 'index_sw_L3_list.csv'
            try:
                _file_path = file_path_sw_l1
                df_l1 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
                _file_path = file_path_sw_l2
                df_l2 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
                _file_path = file_path_sw_l3
                df_l3 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
            except FileNotFoundError:
                log_args = [_file_path]
                add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
                return
        df_l1.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l1['valid'] = 'T'
        df_l1['selected'] = 'T'
        df_l1['type'] = 'index'
        df_l1['stype1'] = 'SW'
        df_l1['stype2'] = 'L1'
        df_l1.set_index('ts_code',inplace=True)
        df_l2.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l2['valid'] = 'T'
        df_l2['selected'] = 'T'
        df_l2['type'] = 'index'
        df_l2['stype1'] = 'SW'
        df_l2['stype2'] = 'L2'
        df_l2.set_index('ts_code',inplace=True)
        df_l3.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l3['valid'] = 'T'
        df_l3['selected'] = 'T'
        df_l3['type'] = 'index'
        df_l3['stype1'] = 'SW'
        df_l3['stype2'] = 'L3'
        df_l3.set_index('ts_code',inplace=True)
        _frame = [df_al,df_l1,df_l2,df_l3]
        df_al = pd.concat(_frame,sort=False)

        #--------------上交所指数---------------
        if que_from_ts == True:
            raw_data.index.get_index_basic()
        _file_path = sub_path + '\\' + 'index_basic_sse.csv'        
        try:
            df_sse = pd.read_csv(_file_path,usecols=['ts_code','name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df_sse['valid'] = 'T'
        df_sse['selected'] = 'T'
        df_sse['type'] = 'index'
        df_sse['stype1'] = 'SSE'
        df_sse['stype2'] = ''
        df_sse.set_index('ts_code',inplace=True)
        _file_path = sub_path + '\\' + 'index_basic_szse.csv'        
        try:
            df_szse = pd.read_csv(_file_path,usecols=['ts_code','name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df_szse['valid'] = 'T'
        df_szse['selected'] = 'T'
        df_szse['type'] = 'index'
        df_szse['stype1'] = 'SZSE'
        df_szse['stype2'] = ''
        df_szse.set_index('ts_code',inplace=True)
        _frame = [df_al,df_sse,df_szse]
        df_al = pd.concat(_frame,sort=False)
        #--------------个股---------------
        if que_from_ts == True:
            raw_data.stock.get_stock_basic()
        file_name = 'stock_basic.csv'
        _file_path = sub_path + '\\' + file_name    
        try:
            df = pd.read_csv(_file_path,usecols=['ts_code','name'],index_col='ts_code')
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df['valid'] = 'T'
        df['selected'] = 'T'
        df['type'] = 'stock'
        #df['stype1'] = ''
        #df['stype2'] = ''
        df.loc[df.index.str.startswith('600'),'stype1'] = 'SHZB' #上海主板
        df.loc[df.index.str.startswith('601'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('602'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('603'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('688'),'stype1'] = 'KCB' #科创板
        df.loc[df.index.str.startswith('000'),'stype1'] = 'SZZB' #深圳主板
        df.loc[df.index.str.startswith('001'),'stype1'] = 'SZZB'
        df.loc[df.index.str.startswith('002'),'stype1'] = 'ZXB' #中小板
        df.loc[df.index.str.startswith('003'),'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('004'),'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('300'),'stype1'] = 'CYB' #创业板
        df['stype2'] = ''
        _frame = [df_al,df]
        df_al = pd.concat(_frame,sort=False)
        #--------------结尾---------------
        df_al.to_csv(file_path_al,encoding="utf-8")    
        return

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
    #ttt = index.get_index_daily('399003.SZ',reload=False)
    download_path = r"download_all"
    #bulk_download(download_path) #批量下载数据
    #ttt = ts_pro.index_daily(ts_code='801001.SI',start_date='20190601',end_date='20190731')
    #ttt = ts_pro.sw_daily(ts_code='950085.SH',start_date='20190601',end_date='20190731')
    #Plot.try_plot()
    # #------------------------资产赛跑-----------------------
    # num_samples=30
    # df = Index.load_sw_daily('801003.SI',num_samples)
    # df = df[['trade_date','close']]
    # df['trade_date'] = pd.to_datetime(df['trade_date'])
    # df.set_index('trade_date', inplace=True)
    # base_close, = df.tail(1)['close'].values
    # df['base_chg_pct']=(df['close']/base_close-1)*100
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # ax.plot(df.index,df['base_chg_pct'],label='801003.SI\n指数A')
    
    # df1 = Index.load_sw_daily('850341.SI',30)
    # df1 = df1[['trade_date','close']]
    # df1['trade_date'] = pd.to_datetime(df1['trade_date'])
    # df1.set_index('trade_date', inplace=True)
    # base_close, = df1.tail(1)['close'].values
    # df1['base_chg_pct']=(df1['close']/base_close-1)*100
    # ax.plot(df1.index,df1['base_chg_pct'],label='850341.SI\n指数B')
    # plt.legend(handles=ax.lines)
    # plt.grid(True)
    # mpl.rcParams['font.sans-serif'] = ['FangSong'] # 指定默认字体
    # mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
    # plt.xticks(df.index,rotation='vertical')
    # plt.title('申万板块指标{}日涨幅比较'.format(num_samples))
    # plt.ylabel('收盘%')
    # plt.legend(bbox_to_anchor=(1, 1),bbox_transform=plt.gcf().transFigure)
    # plt.show()
    # #------------------------下载申万三级行业分类-----------------------
    # df_l1,df_l2,df_l3 = Index.get_sw_index_classify()
    # al = All_Assets_List.load_all_assets_list()
    # All_Assets_List.rebuild_all_assets_list()
    # #------------------------生成al文件-----------------------
    #al_l1 = Plot_Utility.gen_al(al_name='SW_Index_L1',stype1='SW',stype2='L1') #申万一级行业指数
    # al_l2 = Plot_Utility.gen_al(al_name='SW_Index_L2',stype1='SW',stype2='L2') #申万二级行业指数
    # al_l3 = Plot_Utility.gen_al(al_name='SW_Index_L3',stype1='SW',stype2='L3') #申万二级行业指数
    # al_download = Plot_Utility.gen_al(al_name='download_all',selected=None)#全部valid='T'的资产
    # #-------------------Plot_Assets_Racing资产竞速-----------------------
    #plot_ar = Plot_Assets_Racing('al_SW_Index_L3.csv',period=5)
    # #-------------------Stock Class-----------------------
    #stock = Stock(pull=True)
    #stock = Stock()


