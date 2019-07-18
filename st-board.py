import tushare as ts
import pandas as pd
from datetime import datetime
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print

sub_path = r".\data_csv"
ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()

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

def day_str(mode = 'today'):
    """
    未完待续
    mode: 'today' return today string
          'last_trade_day' return last trade day
    return: <str> in 'YYYYMMDD' e.g.'20190712'"""
    dt = datetime.now()
    today_str = dt.strftime("%Y%m%d")
    if mode.lower() == 'today':
        return today_str
    if mode.lower() == 'last_trade_day':
        #未完待续
        #last_trade_day_str = query_last_trade_day(today_str)
        pass
    








class Static_Model():
    """    """
    def __init__(self, pull=False):
        self.trade_calendar = Trade_Calendar(pull)
        self.stock_list = None

        


    
       

class Trade_Calendar():
    """    """
    def __init__(self, pull=False):
        self.df = None
        if pull == True:
            self.get()
        else:
            self.load()
    
    def load(self):
        """load trade_calendar.csv文件，读入statics_model.trade_calendar
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
    static_model = Static_Model(pull=True)
    b = static_model.trade_calendar
    b.valid()
    b.df
