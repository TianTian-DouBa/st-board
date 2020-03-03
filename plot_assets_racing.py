from XF_common.XF_LOG_MANAGE import add_log
import pandas as pd
from st_board import Plot_Assets_Racing


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


if __name__ == '__main__':
    period = 3
    df = select_df_csv()
    plot = Plot_Assets_Racing(df, period)

