from datetime import datetime
from st_board import Fund, Asset
import tushare as ts

ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()

if __name__ == "__main__":
    # df = ts_pro.fund_nav(ts_code='518880.SH', start_date='20191231', end_date='20200430')
    # print(df)
    etr_col = {'unit_net',}
    etf = Asset('518880.SH', load_daily=etr_col)
    daily_df = etf.daily_data
    daily_df.loc[:, 'premium_rate'] = (daily_df.unit_net - daily_df.close) / daily_df.close
    # print(etf.daily_data)

    from spider.spider_common import update_gold_df, load_daily_df
    update_gold_df('AU9999.SH')
    au9999 = load_daily_df('AU9999.SH')
    # print('--------------AU9999-------------')
    # print(au9999)
    sr_net = daily_df['unit_net'].head(450)
    sr_auc = au9999['close'].head(450)  # 收盘价
    # sr_rate = sr_net / sr_auc *10000
    # print('----------sr_rate 518880净值与au9999收盘价的比率----------')
    # print(sr_rate)
    sr_r = (sr_net / sr_auc).tail(20)
    # print(sr_r)
    net_auc = sr_r.agg(['median'])['median']  # 518880净值与au9999收盘价的比率进20交易日的中位数
    # 观察到518880.SH的净值与AU9999收盘价的耦合性是非常好的；可以通过当日晚公布的AU9999收盘价及近几日的net_auc比值推断出518880的净值
    print('net_auc:{}'.format(net_auc))
    print()

    print('----------sr_net_auc推算预测转换系数----------')
    _sr = sr_net / sr_auc
    sr_net_auc = _sr.rolling(20).median()  # 预测转换系数
    # sr_net_auc = sr_net_auc.iloc[::-1]
    print(sr_net_auc)
    print()

    print('----------比较预测净值和实际净值的差异----------')
    sr_fcst_net = sr_net_auc * sr_auc
    sr_fcst_err = (sr_fcst_net - sr_net) / sr_net *10000
    print(sr_fcst_err)
    pass







