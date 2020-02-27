import tushare as ts
import pandas as pd
import numpy as np
from XF_LOG_MANAGE import add_log, logable, log_print
from datetime import datetime, timedelta

ts_pro = ts.pro_api()

sub_path = r".\data_csv"
sub_path_2nd_daily = r"\daily_data"  # 日线数据
sub_path_config = r"\config"  # 配置文件
sub_path_al = r"\assets_lists"  # 资产列表
sub_path_result = r".\plot_result"  # 分析模板运行结果
sub_idt = r"\idt_data"  # 存放指标的结果，下按idt_type不同再分目录
sub_analysis = r"\analysis"  # 分析数据

SUBTYPE = {'D': 'day',
           'W': 'week',
           'M': 'month'}

SOURCE = {'close': '收盘数据，能复权则后复权',  # 修改的是，原来'close_hfq'
          }

SOURCE_TO_COLUMN = {'close': 'close',  # 修改的是，原来'close_hfq'
                    }

STATUS_WORD = {0: '-bad-',
               1: '-good-',
               3: '-uncertain-'}
DOWNLOAD_WORD = {0: '-success-',
                 1: '-fail-',
                 3: '-unknown-'}
DEFAULT_OPEN_DATE_STR = "19901219"  # 中国股市开始时间？
# 用于dashboard head不同参数的显示格式
FORMAT_HEAD = {"ts_code": "{:<12}",
               "trade_date": "{:^10}",
               "in_date": "{:^14}",
               "by_date": "{:^14}",
               "by_price": "{:>10}",
               "in_price": "{:>10}",
               "earn": "{:^10}",
               "earn_pct": "{:^12}",
               "stay_days": "{:<10}",
               "cond_desc": "{:^30}",
               "cond_result": "{:<6}",
               "cond_p1_value": "{:^15}",
               "cond_p2_value": "{:^15}",
               "cond_p1_date": "{:<14}",
               "cond_p2_date": "{:<14}",
               }
# 用于dashboard record不同参数的显示格式
FORMAT_FIELDS = {"ts_code": "{:<12}",
                 "trade_date": "{:^14}",
                 "in_date": "{:^14}",
                 "by_date": "{:^14}",
                 "by_price": "{:10.2f}",
                 "in_price": "{:10.2f}",
                 "earn": "{:10.2f}",
                 "earn_pct": "{:12.2%}",
                 "stay_days": "{:>8}",
                 "cond_desc": "{:<30}",
                 "cond_result": "{:<6}",
                 "cond_p1_value": "{:14.2f}",
                 "cond_p2_value": "{:14.2f}",
                 "cond_p1_date": "{:^14}",
                 "cond_p2_date": "{:^14}",
                 }

# Condition Special idt_type
CND_SPC_TYPES = {'const',  # 常量
                 'stay_days',  # 在pool中交易日数
                 'earn_pct',  # 盈利%
                 'max_by_pct',  # pool中历史最高by_price对应的earn pct
                 'min_by_pct',  # pool中历史最低by_price对应的loss pct
                 'earn_return',  # 从max_by回落一定比例触发
                 'dymc_return_lmt',  # 动态dynamic earn return limit, 根据max_by_pct计算，用于earn_return的动态设定
                 }

# 特殊的ts_code
SPC_TS_CODE = {'hsgt_flow',  # 沪深港通数据起点
               }

ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')


class Raw_Data:
    """存放其它模块计算时所要用的公共数据的基础模块，需要实例化填充数据后使用"""
    def __init__(self, pull=False):
        """
        pull: True=get from Tushare; False=load from file
        """
        self.trade_calendar = None  # 交易日日历
        self.init_trade_calendar(pull)
        self.all_assets_list = None  # 全资产代码列表
        self.load_all_assets_list()
        self.index = Index_Basic(pull)  # 指数相关数据
        self.stock = Stock_Basic(pull)  # 个股相关数据

        # self.stock_list = None
    
    def init_trade_calendar(self, pull=False):
        """
        初始化.trade_calendar
        """
        if pull is True:
            self.get_trade_calendar()
        else:
            self.load_trade_calendar()

    def valid_trade_date(self, date_str):
        """
        验证date_str是否是交易日
        date_str:<str> e.g. '20190723' YYYYMMDD
        return:<bool> True=是交易日
                      None=不是
        """
        is_open = self.trade_calendar.loc[int(date_str)]['is_open']
        # print('[L40] is_open: {}'.format(is_open))
        # 看具体什么except
        if is_open == 1:
            return True

    def load_trade_calendar(self):
        """
        load trade_calendar.csv文件，读入self.trade_calendar
        """
        file_name = "trade_calendar.csv"
        file_path = sub_path + '\\' + file_name
        try:
            self.trade_calendar = pd.read_csv(file_path, dtype={'cal_date': str, 'pretrade_date': str}, index_col='cal_date')
        except FileNotFoundError:
            log_args = [file_path]
            add_log(20, '[fn]Raw_Data.load_trade_calendar(). file "{0[0]}" not found', log_args)
            self.trade_calendar = None

    def get_trade_calendar(self):
        """
        获取TuShare的交易日历数据,保存到trade_calendar.csv文件；
        日历会更新到当年的年底
        """
        file_name = "trade_calendar.csv"
        file_path = sub_path + '\\' + file_name
        self.trade_calendar = ts_pro.trade_cal(fields='cal_date,is_open,pretrade_date')
        self.trade_calendar.set_index('cal_date', inplace=True)
        self.trade_calendar.to_csv(file_path, encoding="utf-8")

    def valid_trade_calendar(self):
        """
        将来考虑增加按最新日期验证有效性
        return: True if valid, None if invalid
        """
        if isinstance(self.trade_calendar, pd.DataFrame):
            if len(self.trade_calendar) > 2:
                return True

    def last_trade_day(self, dt_str=None):
        """
        查询指定日当日或之前最近的交易日，返回日期字符串
        dt_str: <string> in 'YYYYMMDD' e.g.'20190721'
                None 代表当日
        return: <string> in 'YYYYMMDD' e.g.'20190719'
        """
        if self.valid_trade_calendar() is None:
            add_log(20, '[fn]Raw_Data.last_trade_day() trade_calendar invalid')
            return
        if dt_str is None:
            dt = datetime.now()
            dt_str = dt.strftime("%Y%m%d")
        if isinstance(dt_str, str) and (len(dt_str) == 8):
            # tdf = self.trade_calendar.set_index(['cal_date'])
            tdf = self.trade_calendar
            try:
                is_open = tdf.loc[int(dt_str)]['is_open']
                if is_open == 1:
                    return dt_str
                elif is_open == 0:
                    pretrade_date = tdf.loc[int(dt_str)]['pretrade_date']
                    return str(pretrade_date)
                else:
                    return None
            except KeyError:
                log_args = [dt_str]
                add_log(10, '[fn]Raw_Data.last_trade_day() dt_str "{0[0]}" not in stock_basic.csv. check date_str and pull Raw_Data', log_args)
                return
        else:
            log_args = [dt_str]
            add_log(20, '[fn]Raw_Data.last_trade_day() dt_str "{0[0]}" incorrect format', log_args)
            return None

    def next_trade_day(self, dt_str, next_n=1):
        """
        查询之后第n个交易日
        dt_str: <string> in 'YYYYMMDD' e.g.'20190721'
        next_n: <int>
        return: <string> in 'YYYYMMDD' e.g.'20190719'
        """
        SUSPENDED_DAYS_LIM = 100
        if self.valid_trade_calendar() is None:
            add_log(20, '[fn]Raw_Data.next_trade_day() trade_calendar invalid')
            return
        if next_n < 1:
            log_args = [next_n]
            add_log(20, '[fn]Raw_Data.next_trade_day() next_n:{0[0]} invalid'.format(log_args))
            return
        if isinstance(dt_str, str) and (len(dt_str) == 8):
            tdf = self.trade_calendar
            try:
                pos = tdf.index.get_loc(int(dt_str))
                for _ in range(int(next_n)):
                    for i in range(1, SUSPENDED_DAYS_LIM):  # 估计最长休市天数不超过x天
                        if tdf.iloc[pos + i]['is_open'] == 1:
                            pos = pos + i
                            break
                    else:
                        add_log(20, '[fn]Raw_Data.next_trade_day() suspended days exceed limit')
                        return
            except KeyError:
                log_args = [dt_str]
                add_log(10, '[fn]Raw_Data.next_trade_day() dt_str "{0[0]}" not in stock_basic.csv. check date_str and pull Raw_Data', log_args)
                return
            next_trade_date = tdf.iloc[pos].name  # tdf.iloc[pos]是Series，所以用.name
            # print('[L167] next_trade_date: {}'.format())
            return str(next_trade_date)
        else:
            log_args = [dt_str]
            add_log(20, '[fn]Raw_Data.next_trade_day() dt_str "{0[0]}" incorrect format', log_args)
            return

    def previous_trade_day(self, dt_str, pre_n=1):
        """
        查询不含当日的前一个交易日
        dt_str: <string> in 'YYYYMMDD' e.g.'20190721'
        pre_n: <int>
        return: <string> in 'YYYYMMDD' e.g.'20190719'
        """
        SUSPENDED_DAYS_LIM = 100
        if self.valid_trade_calendar() is None:
            add_log(20, '[fn]Raw_Data.previous_trade_day() trade_calendar invalid')
            return
        if pre_n < 1:
            log_args = [pre_n]
            add_log(20, '[fn]Raw_Data.previous_trade_day() pre_n:{0[0]} invalid'.format(log_args))
            return
        if isinstance(dt_str, str) and (len(dt_str) == 8):
            tdf = self.trade_calendar
            try:
                pos = tdf.index.get_loc(int(dt_str))
                for _ in range(int(pre_n)):
                    for i in range(1, SUSPENDED_DAYS_LIM):  # 估计最长休市天数不超过x天
                        if tdf.iloc[pos - i]['is_open'] == 1:
                            pos = pos - i
                            break
                    else:
                        add_log(20, '[fn]Raw_Data.previous_trade_day() exceed span')
                        return
            except KeyError:
                log_args = [dt_str]
                add_log(10, '[fn]Raw_Data.previous_trade_day() dt_str "{0[0]}" not in stock_basic.csv. check date_str and pull Raw_Data', log_args)
                return
            previous_trade_date = tdf.iloc[pos].name  # 是Series，所以用.name
            # print('[L167] previous_trade_date: {}'.format())
            return str(previous_trade_date)
        else:
            log_args = [dt_str]
            add_log(20, '[fn]Raw_Data.previous_trade_day() dt_str "{0[0]}" incorrect format', log_args)
            return

    def in_calendar(self, date_str):
        """
        查看日期是否在calendar中
        """
        try:
            self.trade_calendar.index.get_loc(int(date_str))
            return True
        except Exception as e:
            return

    def len_trade_days(self, start_day, end_day=None):
        """
        查询两个日期间所包括的交易日数，start_day算第1天，end_day也算1天
        具体except处理待完善
        start_day: <str> e.g. '20191231'
        end_day: None = today_str() 今天的字符串
                 <str> e.g. '20191231'
        """
        from st_board import today_str, valid_date_str_fmt
        if end_day is None:
            end_day = today_str()

        if self.in_calendar(start_day) is not True:
            log_args = [start_day]
            add_log(20, '[fn]Raw_Data.len_trade_days() start_day:{0[0]} not in calendar', log_args)
            return

        if self.in_calendar(end_day) is not True:
            log_args = [end_day]
            add_log(20, '[fn]Raw_Data.len_trade_days() end_day:{0[0]} not in calendar', log_args)
            return

        try:
            int_start = int(start_day)
            int_end = int(end_day)
        except ValueError:
            log_args = [start_day, end_day]
            add_log(20, '[fn]Raw_Data.len_trade_days() start_day:{0[0]} or end_day:{0[1]} invalid', log_args)
            return

        try:
            df = self.trade_calendar[self.trade_calendar.index.to_series().between(int_start, int_end)]
            rslt = df['is_open'].value_counts()[1]
        except IndexError:
            log_args = [start_day, end_day]
            add_log(30, '[fn]Raw_Data.len_trade_days() start_day:{0[0]} or end_day:{0[1]} invalid or mis-order', log_args)
            return
        except KeyError:
            rslt = 0  # 之间无交易日
        return rslt

    def valid_ts_code(self, ts_code):
        """验证在raw_data内ts_code是否有效,
        return: <bool> True=valid
        """
        if ts_code in SPC_TS_CODE:
            return True
        # --------------------Index---------------
        try:
            name = self.all_assets_list.loc[ts_code]['name']
            # print("[debug L249] name:{}".format(name))
        except KeyError:
            log_args = [ts_code]
            add_log(30, '[fn]Raw_Data.valid_ts_code(). ts_code "{0[0]}" invalid', log_args)
            return
        if isinstance(name, str):
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
        从all_assets_list.csv中读取全代码列表
        """
        file_name = "all_assets_list.csv"
        file_path = sub_path + sub_path_config + '\\' + file_name
        try:
            df = pd.read_csv(file_path, index_col='ts_code')
        except FileNotFoundError:
            log_args = [file_path]
            add_log(10, '[fn]Raw_Data.load_all_assets_list. "{0[0]}" not found', log_args)
            df = None
        self.all_assets_list = df


class Index_Basic:
    """
    指数相关，包括行业板块指数等
    """
    def __init__(self, pull=False):
        """
        pull: True=get from Tushare; False=load from file
        """
        self.index_basic_df = None
        self.idx_ts_code = None  # <df> ts_code indexed
        self.valid = {'index_basic_sse': STATUS_WORD[3],  # 上交所
                      'index_basic_szse': STATUS_WORD[3],  # 深交所
                      'index_basic_sw': STATUS_WORD[3]}  # 申万
        self._sse = None
        self._szse = None
        self._sw = None
        if pull is True:
            self.get_index_basic()
        else:
            self.load_index_basic()
        self._idx_ts_code()
    
    def get_index_basic(self):
        """
        从ts_pro获取指数的基本信息列表
        无用-待续：获取数据失败时，self.valid对应项的设-bad-处理
        return: 上交指数个数，深交指数个数， 申万指数个数
        """
        # 上交所指数
        file_name = "index_basic_sse.csv"
        self._sse = ts_pro.index_basic(market='SSE')
        self.valid['index_basic_sse'] = STATUS_WORD[1]  # 无用，考虑取消
        self._sse.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        n_sse = len(self._sse)
        # 深交所指数
        file_name = "index_basic_szse.csv"
        self._szse = ts_pro.index_basic(market='SZSE')
        self.valid['index_basic_szse'] = STATUS_WORD[1]  # 无用，考虑取消
        self._szse.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        n_szse = len(self._szse)
        # 申万指数
        file_name = "index_basic_sw.csv"
        self._sw = ts_pro.index_basic(market='SW')
        self.valid['index_basic_sw'] = STATUS_WORD[1]  # 无用，考虑取消
        self._sw.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        n_sw = len(self._sw)
        self._update_index_basic_df()
        return n_sse, n_szse, n_sw
    
    def load_index_basic(self):
        """
        load index_basic.csv文件，读入self.index_basic_df
        """
        # 上交所指数
        file_name = "index_basic_sse.csv"
        try:
            self._sse = pd.read_csv(sub_path + '\\' + file_name, dtype={'base_date': str, 'list_date': str})
            self.valid['index_basic_sse'] = STATUS_WORD[1]
        except FileNotFoundError:
            log_args = [file_name]
            add_log(20, '[fn]Index.load_index_basic(). file "{0[0]}" not found', log_args)
            self._sse = None
        # 深交所指数
        file_name = "index_basic_szse.csv"
        try:
            self._szse = pd.read_csv(sub_path + '\\' + file_name, dtype={'base_date': str, 'list_date': str})
            self.valid['index_basic_szse'] = STATUS_WORD[1]
        except FileNotFoundError:
            log_args = [file_name]
            add_log(20, '[fn]Index.load_index_basic(). file "{0[0]}" not found', log_args)
            self._szse = None
        # 申万指数
        file_name = "index_basic_sw.csv"
        try:
            self._sw = pd.read_csv(sub_path + '\\' + file_name, dtype={'base_date': str, 'list_date': str})
            self.valid['index_basic_sw'] = STATUS_WORD[1]
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
        from st_board import valid_date_str_fmt
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
        from st_board import valid_date_str_fmt
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
        if self.valid['index_basic_sse'] == STATUS_WORD[1]:  # good
            _frames.append(self._sse)
        if self.valid['index_basic_szse'] == STATUS_WORD[1]:   # good
            _frames.append(self._szse)
        if self.valid['index_basic_sw'] == STATUS_WORD[1]:  # good
            _frames.append(self._sw)
        if len(_frames) > 0:
            self.index_basic_df = pd.concat(_frames, ignore_index=True, sort=False)


class Stock_Basic:
    """股票类的资产"""

    def __init__(self, pull=False):
        self.basic = None
        if pull is True:
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
                self.basic.set_index('ts_code', inplace=True)
                self.basic.to_csv(file_path, encoding='utf-8')
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
        from st_board import valid_date_str_fmt
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