from st_common import Raw_Data
from st_common import sub_path_rpt
from st_board import Select_Collect, Stock
import xlsxwriter as xlw
from XF_LOG_MANAGE import add_log
import xml.etree.ElementTree as ET
import pandas as pd
from interact_portal import load_xml
from xlw_common import *

al_file_name = Select_Collect.al_file_name


def report(al_file):
    """
    报告：每日资产概览
    al_file: <str> al_<al_file>.csv
    """
    from st_board import Strategy
    from st_board import today_str
    global raw_data

    def head_across(row, column, text):
        """
        用于辅助打印标题栏0行的一组标题
        """
        nonlocal ws1
        cross = 4  # 单元格跨四格
        ws1.write(row, column, text, fmt_head0)
        for i in range(1, cross):
            ws1.write_blank(row, column + i, '', fmt_head0)

    # =================报告文件=================
    today_s = today_str()
    trade_day_str = raw_data.last_trade_day(today_s)
    file_name = 'rpt_racing_' + al_file + '_' + trade_day_str + '.xlsx'
    file_path = '.\\' + sub_path_rpt + '\\' + file_name

    # =================初始化Strategy=================
    stg = Strategy('report_daily_basic')
    stg.add_pool(desc='p10', al_file=al_file, assets_daily_data='basic', del_trsfed=None)
    p10 = stg.pools[10]

    # ------condition_0
    pre_args1 = {'idt_type': 'ma',
                 'period': 2}
    pre_args2 = {'idt_type': 'ma',
                 'period': 5}
    p10.add_condition(pre_args1, pre_args2, '>')

    # ------condition_1
    pre_args1 = {'idt_type': 'ma',
                 'period': 10}
    pre_args2 = {'idt_type': 'ma',
                 'period': 20}
    p10.add_condition(pre_args1, pre_args2, '>')

    # ------condition_2
    pre_args1 = {'idt_type': 'ma',
                 'period': 60}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p10.add_condition(pre_args1, pre_args2, '>')

    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()
    end_date = trade_day_str
    start_date = raw_data.previous_trade_day(end_date, 2)
    stg.update_cycles(start_date=start_date, end_date=end_date)

    # ----当前周期的结束日期
    m1d = raw_data.previous_trade_day(end_date, 1)  # 前1交易日
    m2d = start_date  # 前2交易日
    m5d = raw_data.previous_trade_day(end_date, 5)  # 前5交易日
    m10d = raw_data.previous_trade_day(end_date, 10)  # 前10交易日
    m20d = raw_data.previous_trade_day(end_date, 20)  # 前20交易日
    m60d = raw_data.previous_trade_day(end_date, 60)  # 前60交易日

    # ----前一周期的开始结束日期
    p1d_end = m1d  # 前1周期结束日期
    p1d_start = raw_data.previous_trade_day(p1d_end, 1)  # 前1周期开始日期
    p2d_end = m2d  # 前1周期结束日期
    p2d_start = raw_data.previous_trade_day(p2d_end, 2)  # 前1周期开始日期
    p5d_end = m5d  # 前1周期结束日期
    p5d_start = raw_data.previous_trade_day(p5d_end, 5)  # 前1周期开始日期
    p10d_end = raw_data.previous_trade_day(m10d, 1)  # 前1周期结束日期
    p10d_start = m10d  # 前1周期开始日期
    p20d_end = raw_data.previous_trade_day(m20d, 1)  # 前1周期结束日期
    p20d_start = m20d  # 前1周期开始日期
    p60d_end = raw_data.previous_trade_day(m60d, 1)  # 前1周期结束日期
    p60d_start = m60d  # 前1周期开始日期

    # =================数据准备=================
    data = []  # [(数据1, 数据2, 数据3）, (...)]存放用于报告显示的数据
    df = pd.DataFrame(columns=['ts_code', 'm1d_pct', 'close', 'p1d_pct'])  # 缓存<df>
    # df.set_index('ts_code', inplace=True)
    for asset in p10.assets.values():
        ts_code = asset.ts_code
        name = asset.name
        # ----最新的复权因子，当日收盘
        adj = 1  # 非Stock复权因子设为1
        if isinstance(asset, Stock):
            adj_sr = Stock.load_adj_factor(ts_code, nrows=1)
            if adj_sr is not None:
                adj, = adj_sr['adj_factor'].head(1)  # 最新的复权因子
        close, _ = asset.get_price(trade_date=end_date, seek_direction='backwards')  # 最新收盘

        # ----本周期n日涨幅
        close_m1d, _ = asset.get_price(trade_date=m1d, seek_direction='backwards')
        close_m2d, _ = asset.get_price(trade_date=m2d, seek_direction='backwards')
        close_m5d, _ = asset.get_price(trade_date=m5d, seek_direction='backwards')
        close_m10d, _ = asset.get_price(trade_date=m10d, seek_direction='backwards')
        close_m20d, _ = asset.get_price(trade_date=m20d, seek_direction='backwards')
        close_m60d, _ = asset.get_price(trade_date=m60d, seek_direction='backwards')

        m1d_pct = (close - close_m1d) / close_m1d  # 1日涨跌幅
        m2d_pct = (close - close_m2d) / close_m2d  # 2日涨跌幅
        m5d_pct = (close - close_m5d) / close_m5d  # 5日涨跌幅
        m10d_pct = (close - close_m10d) / close_m10d  # 10日涨跌幅
        m20d_pct = (close - close_m20d) / close_m20d  # 20日涨跌幅
        m60d_pct = (close - close_m60d) / close_m60d  # 60日涨跌幅

        # ----前周期n日涨幅
        close_p1d_end, _ = asset.get_price(trade_date=p1d_end, seek_direction='backwards')
        close_p1d_start, _ = asset.get_price(trade_date=p1d_start, seek_direction='backwards')
        close_p2d_end, _ = asset.get_price(trade_date=p2d_end, seek_direction='backwards')
        close_p2d_start, _ = asset.get_price(trade_date=p2d_start, seek_direction='backwards')
        close_p5d_end, _ = asset.get_price(trade_date=p5d_end, seek_direction='backwards')
        close_p5d_start, _ = asset.get_price(trade_date=p5d_start, seek_direction='backwards')
        close_p10d_end, _ = asset.get_price(trade_date=p10d_end, seek_direction='backwards')
        close_p10d_start, _ = asset.get_price(trade_date=p10d_start, seek_direction='backwards')
        close_p20d_end, _ = asset.get_price(trade_date=p20d_end, seek_direction='backwards')
        close_p20d_start, _ = asset.get_price(trade_date=p20d_start, seek_direction='backwards')
        close_p60d_end, _ = asset.get_price(trade_date=p60d_end, seek_direction='backwards')
        close_p60d_start, _ = asset.get_price(trade_date=p60d_start, seek_direction='backwards')

        p1d_pct = (close_p1d_end - close_p1d_start) / close_p1d_start  # 1日涨跌幅
        p2d_pct = (close_p2d_end - close_p2d_start) / close_p2d_start  # 2日涨跌幅
        p5d_pct = (close_p5d_end - close_p5d_start) / close_p5d_start  # 5日涨跌幅
        p10d_pct = (close_p10d_end - close_p10d_start) / close_p10d_start  # 10日涨跌幅
        p20d_pct = (close_p20d_end - close_p20d_start) / close_p20d_start  # 20日涨跌幅
        p60d_pct = (close_p60d_end - close_p60d_start) / close_p60d_start  # 60日涨跌幅

        # ----本周期n日价格
        close_ma2, = asset.ma_2.df_idt.head(1)['MA'].values
        close_ma5, = asset.ma_5.df_idt.head(1)['MA'].values
        close_ma10, = asset.ma_10.df_idt.head(1)['MA'].values
        close_ma20, = asset.ma_20.df_idt.head(1)['MA'].values
        close_ma60, = asset.ma_60.df_idt.head(1)['MA'].values

        close_m = close / adj  # 最新未除权价格
        close_ma2r = close_ma2 / adj  # ma2未除权价格
        close_ma5r = close_ma5 / adj  # ma5未除权价格
        close_ma10r = close_ma10 / adj  # ma10未除权价格
        close_ma20r = close_ma20 / adj  # ma20未除权价格
        close_ma60r = close_ma60 / adj  # ma60未除权价格

        # ----asset加入<df>中
        data = {'ts_code': ts_code, 'a_name': name,
                'm1d_pct': m1d_pct, 'close': close_m, 'p1d_pct': p1d_pct,
                'm2d_pct': m2d_pct, 'ma2': close_ma2r, 'p2d_pct': p2d_pct,
                'm5d_pct': m5d_pct, 'ma5': close_ma5r, 'p5d_pct': p5d_pct,
                'm10d_pct': m10d_pct, 'ma10': close_ma10r, 'p10d_pct': p10d_pct,
                'm20d_pct': m20d_pct, 'ma20': close_ma20r, 'p20d_pct': p20d_pct,
                'm60d_pct': m60d_pct, 'ma60': close_ma60r, 'p60d_pct': p60d_pct
                }
        df = df.append(data, ignore_index=True)

        # ----本周期、前周期排名及排位变化
        df.loc[:, 'm1d_rank'] = df['m1d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p1d_rank'] = df['p1d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_1d'] = df['p1d_rank'] - df['m1d_rank']

        df.loc[:, 'm2d_rank'] = df['m2d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p2d_rank'] = df['p2d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_2d'] = df['p2d_rank'] - df['m2d_rank']

        df.loc[:, 'm5d_rank'] = df['m5d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p5d_rank'] = df['p5d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_5d'] = df['p5d_rank'] - df['m5d_rank']

        df.loc[:, 'm10d_rank'] = df['m10d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p10d_rank'] = df['p10d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_10d'] = df['p10d_rank'] - df['m10d_rank']

        df.loc[:, 'm20d_rank'] = df['m20d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p20d_rank'] = df['p20d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_20d'] = df['p20d_rank'] - df['m20d_rank']

        df.loc[:, 'm60d_rank'] = df['m60d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'p60d_rank'] = df['p60d_pct'].rank(ascending=False, method='min')
        df.loc[:, 'chg_60d'] = df['p60d_rank'] - df['m60d_rank']
    df.set_index('ts_code', inplace=True)

    # =================报告基础=================
    workbook = xlw.Workbook(file_path)
    ws1 = workbook.add_worksheet('日报')  # worksheet #1

    fmt_std = workbook.add_format(d_std)
    fmt_wrap = workbook.add_format(d_wrap)
    fmt_rpt_title = workbook.add_format(d_rpt_title)
    fmt_head0 = workbook.add_format(d_head0)
    fmt_head1 = workbook.add_format(d_head1)
    fmt_center = workbook.add_format(d_center)  # 居中
    fmt_int = workbook.add_format(d_int)  # 整数
    fmt_f1d = workbook.add_format(d_f1d)  # 1位小数
    fmt_f2d = workbook.add_format(d_f2d)  # 2位小数
    fmt_f3d = workbook.add_format(d_f3d)  # 3位小数
    fmt_pct = workbook.add_format(d_pct)  # 0%
    fmt_pct1d = workbook.add_format(d_pct1d)  # 1.1%
    fmt_pct2d = workbook.add_format(d_pct2d)  # 2.22%
    fmt_pct1d_g = workbook.add_format(d_pct1d_g)  # 1.1% 灰底

    # =================报告数据=================
    # ----标题栏
    ws1.write_string('A1', trade_day_str + '    ' + 'Report Assets Racing', fmt_rpt_title)
    ws1.set_row(0, 19)  # 第一行，标题栏行高
    ws1.set_row(1, options={'hidden': True})  # 第二行，分隔行行高
    # ----表头第0行
    row_h0 = 2
    column_h0 = 2
    head_across(row_h0, column_h0, '1 Day')
    column_h0 = 6
    head_across(row_h0, column_h0, '2 Days')
    column_h0 = 10
    head_across(row_h0, column_h0, '5 Days')
    column_h0 = 14
    head_across(row_h0, column_h0, '10 Days')
    column_h0 = 18
    head_across(row_h0, column_h0, '20 Days')
    column_h0 = 22
    head_across(row_h0, column_h0, '60 Days')
    # ----表头第1行
    head2 = ('代码', '名称', 'chg%', 'C', 'rank', '+ -', 'chg%', 'ma', 'rank', '+ -', 'chg%', 'ma', 'rank', '+ -', 'chg%', 'ma', 'rank', '+ -', 'chg%', 'ma', 'rank', '+ -', 'chg%', 'ma', 'rank', '+ -')
    ws1.write_row('A4', head2, fmt_head1)  # 标题栏第1行
    # ----ts_code列
    data = df.index.tolist()
    ws1.write_column(4, 0, data, fmt_center)
    # ----name列
    data = df.a_name.tolist()
    ws1.write_column(4, 1, data, fmt_center)
    # ----chg%列
    column_width = 8.38
    data = df.m1d_pct.tolist()
    ws1.write_column(4, 2, data, fmt_pct1d_g)  # 1 Day
    ws1.set_column(2, 2, width=column_width)
    data = df.m2d_pct.tolist()
    ws1.write_column(4, 6, data, fmt_pct1d_g)  # 2 Days
    ws1.set_column(6, 6, width=column_width)
    data = df.m5d_pct.tolist()
    ws1.write_column(4, 10, data, fmt_pct1d_g)  # 5 Days
    ws1.set_column(10, 10, width=column_width)
    data = df.m10d_pct.tolist()
    ws1.write_column(4, 14, data, fmt_pct1d_g)  # 10 Days
    ws1.set_column(14, 14, width=column_width)
    data = df.m20d_pct.tolist()
    ws1.write_column(4, 18, data, fmt_pct1d_g)  # 20 Days
    ws1.set_column(18, 18, width=column_width)
    data = df.m60d_pct.tolist()
    ws1.write_column(4, 22, data, fmt_pct1d_g)  # 60 Days
    ws1.set_column(22, 22, width=column_width)
    # ----close及ma列
    column_width = 8.38
    data = df.close.tolist()
    ws1.write_column(4, 3, data, fmt_f2d)  # 1 Day
    ws1.set_column(3, 3, width=column_width)
    data = df.ma2.tolist()
    ws1.write_column(4, 7, data, fmt_f2d)  # 2 Days
    ws1.set_column(7, 7, width=column_width)
    data = df.ma5.tolist()
    ws1.write_column(4, 11, data, fmt_f2d)  # 5 Days
    ws1.set_column(11, 11, width=column_width)
    data = df.ma10.tolist()
    ws1.write_column(4, 15, data, fmt_f2d)  # 10 Days
    ws1.set_column(15, 15, width=column_width)
    data = df.ma20.tolist()
    ws1.write_column(4, 19, data, fmt_f2d)  # 20 Days
    ws1.set_column(19, 19, width=column_width)
    data = df.ma60.tolist()
    ws1.write_column(4, 23, data, fmt_f2d)  # 60 Days
    ws1.set_column(23, 23, width=column_width)
    # ----rank列
    column_width = 5
    data = df.m1d_rank.tolist()
    ws1.write_column(4, 4, data, fmt_int)  # 1 Day
    ws1.set_column(4, 4, width=column_width)
    data = df.m2d_rank.tolist()
    ws1.write_column(4, 8, data, fmt_int)  # 2 Days
    ws1.set_column(8, 8, width=column_width)
    data = df.m5d_rank.tolist()
    ws1.write_column(4, 12, data, fmt_int)  # 5 Days
    ws1.set_column(12, 12, width=column_width)
    data = df.m10d_rank.tolist()
    ws1.write_column(4, 16, data, fmt_int)  # 10 Days
    ws1.set_column(16, 16, width=column_width)
    data = df.m20d_rank.tolist()
    ws1.write_column(4, 20, data, fmt_int)  # 20 Days
    ws1.set_column(20, 20, width=column_width)
    data = df.m60d_rank.tolist()
    ws1.write_column(4, 24, data, fmt_int)  # 60 Days
    ws1.set_column(24, 24, width=column_width)
    # ----change列
    column_width = 5
    data = df.chg_1d.tolist()
    ws1.write_column(4, 5, data, fmt_int)  # 1 Day
    ws1.set_column(5, 5, width=column_width)
    data = df.chg_2d.tolist()
    ws1.write_column(4, 9, data, fmt_int)  # 2 Days
    ws1.set_column(9, 9, width=column_width)
    data = df.chg_5d.tolist()
    ws1.write_column(4, 13, data, fmt_int)  # 5 Days
    ws1.set_column(13, 13, width=column_width)
    data = df.chg_10d.tolist()
    ws1.write_column(4, 17, data, fmt_int)  # 10 Days
    ws1.set_column(17, 17, width=column_width)
    data = df.chg_20d.tolist()
    ws1.write_column(4, 21, data, fmt_int)  # 20 Days
    ws1.set_column(21, 21, width=column_width)
    data = df.chg_60d.tolist()
    ws1.write_column(4, 25, data, fmt_int)  # 60 Days
    ws1.set_column(25, 25, width=column_width)

    # =================收尾格式=================
    ws1.set_column(0, 0, 9)  # 代码
    ws1.set_column(1, 1, 8)  # 名称
    ws1.set_landscape()  # 页面横向
    ws1.set_paper(9)  # 设置A4纸
    ws1.center_horizontally()  # 居中
    ws1.set_margins(0.1, 0.1, 0.1, 1)
    ws1.set_footer('&C&P of &N')  # 设置页脚
    ws1.repeat_rows(0, 3)  # 重复打印行
    ws1.repeat_columns(0, 1)  # 重复打印列
    ws1.freeze_panes(4, 2)  # freeze行列
    ws1.fit_to_pages(1, 0)  # 宽度放在1页中，长度不限
    workbook.close()

    # print(df)
    # df.to_csv('临时report_race.csv', encoding='utf-8')
    pass


if __name__ == '__main__':
    raw_data = Raw_Data(pull=False)
    al_file = al_file_name()
    report(al_file)
    pass
