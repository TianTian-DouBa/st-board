from st_common import Raw_Data
from st_common import sub_path_rpt
from st_board import Select_Collect
import xlsxwriter as xlw

al_file_name = Select_Collect.al_file_name


def rpt_d_basic(al_file):
    """
    报告：每日基本情况
    al_file: <str> al_<al_file>.csv
    """
    from st_board import Strategy
    from st_board import today_str
    global raw_data

    # =================报告文件=================
    today_s = today_str()
    trade_day_str = raw_data.last_trade_day(today_s)
    file_name = 'rpt_d_' + al_file + '_' + trade_day_str + '.xlsx'
    file_path = '.\\' + sub_path_rpt + '\\' + file_name

    # =================初始化Strategy=================
    stg = Strategy('report_daily_basic')
    # dd = {'turnover_rate_f'}  # 成交额
    # stg.add_pool(desc='p10', al_file=al_file, assets_daily_data=dd, del_trsfed=None)
    stg.add_pool(desc='p10', al_file=al_file, assets_daily_data='basic', del_trsfed=None)
    p10 = stg.pools[10]
    # ------condition_0
    pre_args1 = {'idt_type': 'ma',
                 'period': 20}
    pre_args2 = {'idt_type': 'maqs',
                 'period': 20}
    p10.add_condition(pre_args1, pre_args2, '>')
    # ------condition_1
    pre_args1 = {'idt_type': 'majh',
                 'long_n1': 60,
                 'middle_n2': 20,
                 'short_n3': 5}
    pre_args2 = {'idt_type': 'maqs',
                 'period': 60}
    p10.add_condition(pre_args1, pre_args2, '>')
    # ------condition_2
    pre_args1 = {'idt_type': 'macd',  # 日线macd
                 'long_n1': 26,
                 'short_n2': 12,
                 'dea_n3': 9}
    pre_args2 = {'idt_type': 'macd',  # 周线macd
                 'long_n1': 130,
                 'short_n2': 60,
                 'dea_n3': 45}
    p10.add_condition(pre_args1, pre_args2, '>')

    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()
    end_date = today_str()
    start_date = raw_data.previous_trade_day(end_date, 2)
    stg.update_cycles(start_date=start_date, end_date=end_date)

    # =================数据准备=================
    data = []  # [(数据1, 数据2, 数据3）, (...)]存放用于报告显示的数据
    for asset in p10.assets.values():
        ts_code = asset.ts_code
        name = asset.name
        comment1 = ''
        comment2 = ''
        # ----20日归%
        ma20_last, = asset.ma_20.df_idt.head(1)['MA'].values
        by_price = asset.by_price
        ma20_gl = (by_price / ma20_last - 1) * 100  # close与ma_20的归离率
        # ----20MA变化
        maqs20, = asset.maqs_20.df_idt.head(1)['MAQS'].values
        maqs20 = maqs20 * 1000
        # ----60MA变化
        maqs60, = asset.maqs_60.df_idt.head(1)['MAQS'].values
        maqs60 = maqs60 * 1000
        # ----价聚合<x%天数的占比（最近20个交易日中）
        X = 2  # 聚合的程度%限值
        df = asset.majh_60_20_5.df_idt
        DAYS = 20  # 交易日窗口
        sr = df.head(DAYS)['MAJH']  # 如果数据少于20个，有几个取几个
        n = len(sr)
        if n > 0:
            n_meet = len(sr[sr < X])
            jh_pct = n_meet / n  # 聚合天数的占比
        else:
            jh_pct = 0
        # ----添加数据
        data.append((ts_code, name, comment1, comment2, ma20_gl, maqs20, maqs60, jh_pct))

    # =================报告基础=================
    workbook = xlw.Workbook(file_path)
    ws1 = workbook.add_worksheet('日报')  # worksheet #1

    fmt_std = workbook.add_format()
    fmt_center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})  # 居中
    fmt_f2d = workbook.add_format({'num_format': '0.00', 'valign': 'vcenter'})  # 2位小数
    fmt_f3d = workbook.add_format({'num_format': '0.000', 'valign': 'vcenter'})  # 3位小数
    fmt_pct = workbook.add_format({'num_format': '0%', 'valign': 'vcenter'})  # 0%
    fmt_pct1d = workbook.add_format({'num_format': '0.0%', 'valign': 'vcenter'})  # 1.1%
    fmt_pct2d = workbook.add_format({'num_format': '0.00%', 'valign': 'vcenter'})  # 2.22%

    # 与data对应的显示格式
    #           ts_code       名称      备注     备注    20日归    maqs20   maqs60
    formats = [fmt_center, fmt_center, fmt_std, fmt_std, fmt_f2d, fmt_f2d, fmt_f2d]
    #                    聚合占比
    formats = formats + [fmt_pct]

    # =================报告数据=================
    # ----标题栏
    head = ('代码', '名称', '备注', '备注', '20日归 %', 'MAQS20 ‰', 'MAQS60 ‰',
            '聚2%天比')
    ws1.write_row('A1', head, fmt_center)
    ws1.write_comment('E1', '(by_price / ma20_last - 1) * 100')
    ws1.write_comment('F1', 'maqs_20 * 1000')
    ws1.write_comment('G1', 'maqs_60 * 1000')
    ws1.write_comment('H1', '在20个交易日内，majh<' + str(X) + '%天数的占比')
    # ----填充数据
    row = 1
    assert len(head) == len(formats)
    for item in data:
        for column in range(len(head)):
            ws1.write(row, column, item[column], formats[column])
        row += 1

    workbook.close()
    pass


if __name__ == '__main__':
    raw_data = Raw_Data(pull=False)
    al_file = al_file_name()
    rpt_d_basic(al_file)

