import tushare as ts
import pandas as pd
import numpy as np

sub_path = r".\data_csv"
sub_path_2nd_daily = r"\daily_data" #日线数据
sub_path_config = r"\config" #配置文件
sub_path_al = r"\assets_lists" #资产列表
sub_path_result = r".\plot_result" #分析模板运行结果
sub_idt = r"\idt_data" #存放指标的结果，下按idt_type不同再分目录

SUBTYPE = {'D':'day',
           'W':'week',
           'M':'month'}

SOURCE = {'close_hfq':'收盘后复权',
          }

SOURCE_TO_COLUMN = {'close_hfq':'close',
                    }

STATUS_WORD = {0:'-bad-',
               1:'-good-',
               3:'-uncertain-'}
DOWNLOAD_WORD = {0:'-success-',
                 1:'-fail-',
                 3:'-unknow-'}
DEFAULT_OPEN_DATE_STR = "19901219" #中国股市开始时间？
ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
      
class Raw_Data():
    """存放其它模块计算时所要用的公共数据的基础模块，需要实例化填充数据后使用"""
    def __init__(self, pull=False):
        """
        pull: True=get from Tushare; False=load from file
        """
        self.trade_calendar = Trade_Calendar(pull)
        self.index = Index_Basic(pull) #指数相关数据
        self.stock = Stock_Basic(pull) #个股相关数据
        self.load_all_assets_list()
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
            return
        if isinstance(name,str):
            if len(name) > 0:
                return True
        return

    def valid_all_assets_list(self):
        """验证self.all_assets_list的有效性
        return: True=valid
        """
        if isinstance(self.all_assets_list, pd.DataFrame):
            if len(self.all_assets_list) > 10:
                return True
        return
    
    def load_all_assets_list(self):
        """
        从all_asstes_list.csv中读取全代码列表
        """
        file_name = "all_assets_list.csv"
        file_path = sub_path + sub_path_config + '\\' + file_name
        try:
            df = pd.read_csv(file_path,index_col='ts_code')
        except FileNotFoundError:
            log_args = [file_path]
            add_log(10, '[fn]Raw_Data.load_all_assets_list. "{0[0]}" not found', log_args)
            df = None
        self.all_assets_list = df

class Index_Basic():
    """
    指数相关，包括行业板块指数等
    """
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
        """
        从ts_pro获取指数的基本信息列表
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
        """
        load index_basic.csv文件，读入self.index_basic_df
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
        """
        查询上市时间list_date
        return:<str> e.g. '19930503'
        ts_code:<str> e.g. '000001.SH'
        """
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
    
    def que_base_date(self, ts_code):
        """
        查询base_date
        return:<str> e.g. '19930503'
        ts_code:<str> e.g. '000001.SH'
        """
        try:
            result = self.idx_ts_code.loc[ts_code]['base_date']
        except KeyError:
            log_args = [ts_code]
            add_log(20, '[fn]:Index.que_base_date() ts_code: "{0[0]}" was not found in Index.idx_ts_code. use DEFAULT_OPEN_DATE_STR instead', log_args)
            result = DEFAULT_OPEN_DATE_STR
            return result
        if valid_date_str_fmt(result):
            return result
        else:
            result = DEFAULT_OPEN_DATE_STR
            log_args = [ts_code, result]
            add_log(40, '[fn]:Index.que_base_date() ts_code: "{0[0]}" used "DEFAULT_OPEN_DATE_STR" {0[1]} instead "base_date".', log_args)
            return result

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
            self.index_basic_df = pd.concat(_frames, ignore_index=True, sort=False)

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

class Stock_Basic():
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

    def que_list_date(self, ts_code):
        """查询上市时间list_date
        return:<str> e.g. '19930503'
        ts_code:<str> e.g. '000001.SH'
        """
        try:
            result = self.basic.loc[ts_code]['list_date']
        except KeyError: 
            log_args = [ts_code]
            add_log(20, '[fn]:Stock.que_list_date() ts_code: "{0[0]}" was not found in Stock.base. use DEFAULT_OPEN_DATE_STR instead', log_args)
            result = DEFAULT_OPEN_DATE_STR
            return result
        if valid_date_str_fmt(result):
            return result
        else:
            result = DEFAULT_OPEN_DATE_STR
            log_args = [ts_code, result]
            add_log(40, '[fn]:Stock.que_list_date() ts_code: "{0[0]}" used "DEFAULT_OPEN_DATE_STR" {0[1]} instead "list_date".', log_args)
            return result


global raw_data
raw_data = Raw_Data(pull=False)
print("raw_data:{}".format(repr(raw_data)))