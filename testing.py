import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
import tushare as ts

ts_pro = ts.pro_api()
register_matplotlib_converters()


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


def string_to_dt(string):
    """convert string '20180816' to datetime"""
    result = datetime.strptime(string, "%Y%m%d")
    return result


def plot_in_out(df_io):
    """
    将io_xxxxx.csv gain_pct 绘制成散点图
    """
    df = df_io[['in_date', 'earn_pct']]
    df.set_index('in_date', inplace=True)
    df.index = pd.to_datetime(df.index.astype('str'))  # 变为<datetime>
    xvalues = df.index
    yvalues = df['earn_pct']
    fig, ax = plt.subplots()

    ax.scatter(xvalues, yvalues)
    ax.grid(True, linestyle='-.')

    plt.show()

    # x = np.linspace(-1, 1, 50)
    # y = 2*x+1
    #
    # plt.figure(num=3, figsize=(8,5))
    # new_ticks = np.linspace(-1, 2, 5)
    # plt.xticks(new_ticks)
    # plt.plot(x, y)
    # plt.show()


if __name__ == '__main__':
    # from st_board import Hsgt
    # Hsgt.get_moneyflow()
    # ts_code = '600739.SH'
    # start_date = '20140101'
    # end_date = '20200229'
    # df = ts_pro.hk_hold(ts_code=ts_code, start_date=start_date, end_date=end_date, exchange='SH')
    # data = {'trade_date': ['20200101', '20200102', '20200103', '20200104'], 'close': [1, 2, 3, 4]}
    # df1 = pd.DataFrame.from_dict(data)
    # df1.set_index('trade_date', inplace=True)
    #
    # data = {'trade_date': ['20200102', '20200104'], 'vol': [22, 44]}
    # df2 = pd.DataFrame.from_dict(data)
    # df2.set_index('trade_date', inplace=True)
    #
    # df1.loc[:, 'vol'] = df2['vol']

    from st_board import Stock, bulk_calc_dfq
    bulk_calc_dfq('dl_stocks', reload=True)





