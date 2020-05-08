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
    etr_col = {'unit_net', 'pct_chg'}
    etf_518880 = Asset('518880.SH', load_daily=etr_col)
    etf_518800 = Asset('518800.SH', load_daily=etr_col)
    df_518880 = etf_518880.daily_data
    df_518800 = etf_518800.daily_data
    # df_518880.loc[:, 'premium_rate'] = (df_518880.unit_net - df_518880.close) / df_518880.close  # 计算基金公布的溢价率

    update_gold_df('AU9999.SH')  # 更新数据
    df_au = load_daily_df('AU9999.SH')

    # 计算预测用系数
    sr_auc = df_au['close'].head(50)  # 收盘价

    sr_net_518880 = df_518880['unit_net'].head(50)
    sr_r_518880 = (sr_net_518880 / sr_auc).head(20)  # 预测用的净值和现货收盘价的比值

    sr_net_518800 = df_518800['unit_net'].head(50)
    sr_r_518800 = (sr_net_518800 / sr_auc).head(20)  # 预测用的净值和现货收盘价的比值

    # 公布的etf最新净值
    latest_net_518880 = sr_net_518880.head(1)
    latest_net_date_518880, = sr_net_518880.head(1).index
    latest_pre_net_518880 = sr_net_518880.values[1]  # 前一个净值
    latest_pre_net_date_518880 = sr_net_518880.index[1]  # 前一个净值日期
    # print('[L42] 官方518880.SH净值： {:.3f}  {}'.format(float(latest_pre_net_518880), latest_pre_net_date_518880))

    latest_net_518800 = sr_net_518800.head(1)
    latest_net_date_518800, = sr_net_518800.head(1).index
    latest_pre_net_518800 = sr_net_518800.values[1]  # 前一个净值
    latest_pre_net_date_518800 = sr_net_518800.index[1]  # 前一个净值日期

    latest_auc, = sr_auc.head(1)
    latest_au_pct_chg = df_au.iloc[0].pct_chg  # au9999涨跌幅%
    latest_auc_date, = sr_auc.head(1).index

    # print('[L33]---------sr_r_518880预测比值---------')
    # print(sr_r_518880)
    # print()
    latest_pct_chg_518880, = df_518880.head(1).pct_chg  # etf 518880 变动%
    latest_close_518880, = df_518880.head(1).close  # etf 518880 收盘
    latest_date_518880, = df_518880.head(1).index  # etf 518880 日期
    net_auc_518880 = sr_r_518880.agg(['median'])['median']  # 518880净值与au9999收盘价的比率进20交易日的中位数
    print('518880预测用比值x1w: {:.2f}'.format(net_auc_518880 * 10000))
    fcst_net_518880 = latest_auc * net_auc_518880  # 根据AU9999收盘价及近几日的net_auc比值推断出518880的净值

    # print('[L33]---------sr_r_518800国泰黄金预测比值---------')
    # print(sr_r_518800)
    # print()
    latest_pct_chg_518800, = df_518800.head(1).pct_chg  # etf 518800 变动%
    latest_close_518800, = df_518800.head(1).close  # etf 518800 收盘
    latest_date_518800, = df_518800.head(1).index  # etf 518800 日期
    net_auc_518800 = sr_r_518800.agg(['median'])['median']  # 518800净值与au9999收盘价的比率进20交易日的中位数
    print('518800预测用比值x1w: {:.2f}'.format(net_auc_518800 * 10000))
    fcst_net_518800 = latest_auc * net_auc_518800  # 根据AU9999收盘价及近几日的net_auc比值推断出518800的净值

    print('AU9999收盘: {:.2f}  {:.2%}  {}'.format(latest_auc, latest_au_pct_chg, latest_auc_date))

    print('---------华安黄金----------')
    print('518880.SH 收盘： {}  {:.2f}%  {}'.format(latest_close_518880, latest_pct_chg_518880, latest_date_518880))
    print('推测518880.SH净值： {:.3f}  {}'.format(fcst_net_518880, latest_auc_date))
    # 当时基金已经公布净值的情况下，打印出来与预测比较
    if latest_net_date_518880 == latest_auc_date:
        print('官方518880.SH净值： {:.3f}  {}'.format(float(latest_net_518880), latest_net_date_518880))
        print('官方518880.SH净值： {:.3f}  {}'.format(float(latest_pre_net_518880), latest_pre_net_date_518880))
    if latest_date_518880 == latest_auc_date:
        fcst_premium_rate = (latest_close_518880 - fcst_net_518880) / fcst_net_518880
        print('518880.SH收盘较预测净值溢价: {:.2%}  {}'.format(fcst_premium_rate, latest_auc_date))

    print('---------国泰黄金----------')
    print('518800.SH 收盘： {}  {:.2f}%  {}'.format(latest_close_518800, latest_pct_chg_518800, latest_date_518800))
    print('推测518800.SH净值： {:.3f}  {}'.format(fcst_net_518800, latest_auc_date))
    # 当时基金已经公布净值的情况下，打印出来与预测比较
    if latest_net_date_518800 == latest_auc_date:
        print('官方518800.SH净值： {:.3f}  {}'.format(float(latest_net_518800), latest_net_date_518800))
        print('官方518800.SH净值： {:.3f}  {}'.format(float(latest_pre_net_518800), latest_pre_net_date_518800))
    if latest_date_518800 == latest_auc_date:
        fcst_premium_rate = (latest_close_518800 - fcst_net_518800) / fcst_net_518800
        print('518800.SH收盘较预测净值溢价: {:.2%}  {}'.format(fcst_premium_rate, latest_auc_date))


if __name__ == '__main__':
    report()
    pass
