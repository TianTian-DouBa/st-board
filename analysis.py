from XF_common.XF_LOG_MANAGE import add_log
import pandas as pd


def select_df_csv():
    """
    选择要操作的包含<df>的.csv文件，读入返回<df>
    return: <df> of in_out
            None failed
    """
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()  # 隐藏主窗口
    file_path = filedialog.askopenfilename()
    df = pd.read_csv(file_path, index_col=0)
    return df


def in_out_agg(io_df, trade_cost=0.005):
    """
    显示<df>in_out的统计信息
    io_df: <df> of in_out
    trade_cost: <float> 每次交易的固定成本，加可能的滑点损失
    """
    if not isinstance(io_df, pd.DataFrame):
        log_args = [type(io_df)]
        add_log(20, '[fns]Analysis.in_out_agg(). io_df type:{} is not <df>', log_args)
        return
    s_earn_pct = io_df['earn_pct']  # <Series>
    n_records = len(io_df)  # 记录条数
    n_positive = s_earn_pct[s_earn_pct > 0].count()
    n_negative = s_earn_pct[s_earn_pct < 0].count()
    avg_earn_pct, median_earn_pct = s_earn_pct.agg(['mean', 'median'])  # 平均值，中位数
    dscb = s_earn_pct.describe(percentiles=[.1, .25, .75, .9])
    std_earn_pct, med_10, med_25, med_75, med_90 = dscb[['std', '10%', '25%', '75%', '90%']]
    stay_days, max_days, min_days = io_df['stay_days'].agg(['mean', 'max', 'min'])
    s_high_pct = io_df['high_pct']  # <series>
    dscb_h_pct = s_high_pct.describe(percentiles=[.1, .5, .9])
    high_10, high_50, high_90 = dscb_h_pct[['10%', '50%', '90%']]
    s_low_pct = io_df['low_pct']  # <series>
    dscb_l_pct = s_low_pct.describe(percentiles=[.1, .5, .9])
    low_10, low_50, low_90 = dscb_l_pct[['90%', '50%', '10%']]  # 高低价有意倒过来
    avg_20d_earn_pct = avg_earn_pct / stay_days * 20  # 20交易日标杆的平均涨幅
    net_avg_earn_pct = avg_earn_pct - trade_cost  # 平均单次交易的净收益%
    net_avg_20d_earn_pct = net_avg_earn_pct / stay_days * 20  # 20交易日标杆的平均净收益%

    # 结果展示
    msg = '\n======================================================================================\n'
    msg = msg + 'in_out Aggregate:\n'
    msg = msg + 'Num of Records:    {:>8}        Stay Days(avg):  {:8.1f}\n'.format(n_records, stay_days)
    msg = msg + '% of Pos. Earns:   {:8.2%}        Stay Days(max):  {:8.0f}\n'.format(n_positive / n_records, max_days)
    msg = msg + '% of Neg. Earns:   {:8.2%}        Stay Days(min):  {:8.0f}\n'.format(n_negative / n_records, min_days)
    msg = msg + '\n'

    msg = msg + 'Average Earn%: {:12.2%}        90% High%:   {:12.2%}\n'.format(avg_earn_pct, high_90)
    msg = msg + 'Median Earn%:  {:12.2%}        50% High%:   {:12.2%}\n'.format(median_earn_pct, high_50)
    msg = msg + 'Std Dev Earn%: {:12.2%}        10% High%:   {:12.2%}\n'.format(std_earn_pct, high_10)
    msg = msg + 'Avg 20D Earn%: {:12.2%}\n'.format(avg_20d_earn_pct)
    msg = msg + '\n'

    msg = msg + '90% Earn%:     {:12.2%}        90% Low%:    {:12.2%}\n'.format(med_90, low_90)
    msg = msg + '75% Earn%:     {:12.2%}        50% Low%:    {:12.2%}\n'.format(med_75, low_50)
    msg = msg + '25% Earn%:     {:12.2%}        10% Low%:    {:12.2%}\n'.format(med_25, low_10)
    msg = msg + '10% Earn%:     {:12.2%}\n'.format(med_10)
    msg = msg + '\n'
    msg = msg + 'Net avg Earn%: {:12.2%}\n'.format(net_avg_earn_pct)
    msg = msg + 'Net 20D Earn%: {:12.2%}\n'.format(net_avg_20d_earn_pct)

    msg = msg + '======================================================================================\n'
    return msg


if __name__ == '__main__':
    df = select_df_csv()
    print(in_out_agg(df))  # in_out io_xxxxx.csv文件的报告
    # print(df.MAQS.describe())  # 显示<df>某列的简报
