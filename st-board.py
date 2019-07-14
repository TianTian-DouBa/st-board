import tushare as ts
import pandas as pd
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

if __name__ == "__main__":
    #df = get_stock_list()
    #df = load_stock_list()
    df = get_daily_basic()