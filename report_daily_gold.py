"""
黄金投资日报
尚未完场

目前进度：
通过AU9999的收盘价预测518880.SH黄金etf的净值，显示净值和限价的溢价
"""

from st_common import raw_data
from st_common import sub_path_rpt
from st_board import Asset
import xlsxwriter as xlw
from XF_LOG_MANAGE import add_log
import pandas as pd
from xlw_common import *
from pathlib import PurePath
from spider.spider_common import update_gold_df, load_daily_df

def report():
    """
    报告: 每日黄金市场简报
    """
    etr_col = {'unit_net',}
    etf = Asset('518880.SH', load_daily=etr_col)
    df_etf = etf.daily_data
    df_etf.loc[:, 'premium_rate'] = (df_etf.unit_net - df_etf.close) / df_etf.close  # 计算基金公布的溢价率

    update_gold_df('AU9999.SH')  # 更新数据
    df_au = load_daily_df('AU9999.SH')

    sr_net = df_etf['unit_net'].head(50)
    sr_auc = df_au['close'].head(50)  # 收盘价
    sr_r = (sr_net / sr_auc).head(20)  # 预测用的净值和现货收盘价的比值

    # 公布的etf最新净值
    latest_net = sr_net.head(1)
    latest_net_date, = sr_net.head(1).index

    print('[L33]---------sr_r预测比值---------')
    print(sr_r)
    print()
    latest_etf_close, = df_etf.head(1).close  # etf 518880 收盘
    latest_etf_date, = df_etf.head(1).index  # etf 518880 日期
    print('518880.SH 收盘： {}  {}'.format(latest_etf_close, latest_etf_date))
    net_auc = sr_r.agg(['median'])['median']  # 518880净值与au9999收盘价的比率进20交易日的中位数
    print('预测用比值x10000: {:.2f}'.format(net_auc * 10000))
    latest_auc, = sr_auc.head(1)
    latest_auc_date, = sr_auc.head(1).index
    print('AU9999收盘: {:.2f}  {}'.format(latest_auc, latest_auc_date))
    fcst_net = latest_auc * net_auc  # 根据AU9999收盘价及近几日的net_auc比值推断出518880的净值

    print('-------------------')
    print('推测518880.SH净值： {:.3f}  {}'.format(fcst_net, latest_auc_date))
    # 当时基金已经公布净值的情况下，打印出来与预测比较
    if latest_net_date == latest_auc_date:
        print('官方518880.SH净值： {:.3f}  {}'.format(float(latest_net), latest_net_date))
    if latest_etf_date == latest_auc_date:
        fcst_premium_rate = (latest_etf_close - fcst_net) / fcst_net
        print('518880.SH收盘较预测净值溢价: {:.2%}  {}'.format(fcst_premium_rate, latest_auc_date))


if __name__ == '__main__':
    report()
    pass