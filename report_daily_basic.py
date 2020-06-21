from st_common import raw_data
from st_common import sub_path_rpt
from st_board import Select_Collect
import xlsxwriter as xlw
from XF_LOG_MANAGE import add_log
import xml.etree.ElementTree as ET
from interact_portal import load_xml
from xlw_common import *
from pathlib import PurePath

al_file_name = Select_Collect.al_file_name


def rpt_d_basic(al_file):
    """
    报告：每日资产概览
    al_file: <str> al_<al_file>.csv
    """
    from st_board import Strategy
    from st_board import today_str
    # global raw_data

    # =================报告文件=================
    today_s = today_str()
    trade_day_str = raw_data.last_trade_day(today_s)
    file_name = PurePath('rpt_d_' + al_file + '_' + trade_day_str + '.xlsx')
    file_path = sub_path_rpt / file_name

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
                 'long_n3': 60,
                 'medium_n2': 20,
                 'short_n1': 5}
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

    # ------condition_3
    pre_args1 = {'idt_type': 'jdxz',  # 绝对吸资比例
                 'period': 10}
    pre_args2 = {'idt_type': 'jdxz',  # 绝对吸资比例
                 'period': 250}
    p10.add_condition(pre_args1, pre_args2, '>')

    # ------condition_4
    pre_args1 = {'idt_type': 'dktp',  # 多空头排列
                 'short_n1': 5,
                 'medium_n2': 10,
                 'long_n3': 20}
    pre_args2 = {'idt_type': 'dktp',  # 多空头排列
                 'source': 'vol',  # 成交量
                 'short_n1': 5,
                 'medium_n2': 10,
                 'long_n3': 20}
    p10.add_condition(pre_args1, pre_args2, '>')

    # ------condition_5
    pre_args1 = {'idt_type': 'maqs',  # 量趋势变化
                 'period': 10,
                 'source': 'vol'}
    pre_args2 = {'idt_type': 'maqs',  # 量趋势变化
                 'period': 20,
                 'source': 'vol'}
    p10.add_condition(pre_args1, pre_args2, '>')

    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()
    end_date = trade_day_str
    start_date = raw_data.previous_trade_day(end_date, 2)
    stg.update_cycles(start_date=start_date, end_date=end_date)

    # =================数据准备=================
    data = []  # [(数据1, 数据2, 数据3）, (...)]存放用于报告显示的数据
    X = 2  # 聚合的程度%限值
    for asset in p10.assets.values():
        ts_code = asset.ts_code
        name = asset.name
        # ----从an_<ts_code>.xml读入comment
        tree = load_xml(ts_code)
        if isinstance(tree, ET.ElementTree):
            root = tree.getroot()
            comment1 = root.find('comment1').find('content').text
            comment2 = root.find('comment2').find('content').text
        else:
            comment1 = ''
            comment2 = ''

        # ----20日归%
        try:
            ma20_last, = asset.ma_20.df_idt.head(1)['MA'].values
            by_price = asset.by_price
            ma20_gl = (by_price / ma20_last - 1) * 100  # close与ma_20的归离率
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(20, '[fn]rpt_d_basic() ts_code:{0[0]}; ma20_gl AttributeError, set value = -99', log_args)
            ma20_gl = -99

        # ----20MA变化
        try:
            maqs20, = asset.maqs_20.df_idt.head(1)['MAQS'].values
            maqs20 = maqs20 * 1000
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(20, '[fn]rpt_d_basic() ts_code:{0[0]}; maqs20 AttributeError, set value = -99', log_args)
            maqs20 = -999

        # ----60MA变化
        try:
            maqs60, = asset.maqs_60.df_idt.head(1)['MAQS'].values
            maqs60 = maqs60 * 1000
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(30, '[fn]rpt_d_basic() ts_code:{0[0]}; maqs60 AttributeError, set value = -99', log_args)
            maqs60 = -999

        # ----量20MA变化
        try:
            maqs_vol_20, = asset.maqs_vol_20.df_idt.head(1)['MAQS'].values
            maqs_vol_20 = maqs_vol_20 * 1000
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(30, '[fn]rpt_d_basic() ts_code:{0[0]}; maqs_vol_20 AttributeError, set value = -99', log_args)
            maqs_vol_20 = -99

        # ----量10MA变化
        try:
            maqs_vol_10, = asset.maqs_vol_10.df_idt.head(1)['MAQS'].values
            maqs_vol_10 = maqs_vol_10 * 1000
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(30, '[fn]rpt_d_basic() ts_code:{0[0]}; maqs_vol_10 AttributeError, set value = -99', log_args)
            maqs_vol_10 = -99

        # ----价聚合<x%天数的占比（最近20个交易日中）
        df = asset.majh_60_20_5.df_idt
        DAYS = 20  # 交易日窗口
        try:
            sr = df.head(DAYS)['MAJH']  # 如果数据少于20个，有几个取几个
            n = len(sr)
            if n > 0:
                n_meet = len(sr[sr < X])
                jh_pct = n_meet / n  # 聚合天数的占比
            else:
                jh_pct = 0
        except AttributeError:
            log_args = [asset.ts_code]
            add_log(30, '[fn]rpt_d_basic() ts_code:{0[0]}; jh_pct AttributeError, set value = -0.99', log_args)
            jh_pct = -0.99

        # ----吸资归离, 10D 与 250D
        try:
            jdxz10, = asset.jdxz_10.df_idt.head(1)['JDXZ'].values
            jdxz250, = asset.jdxz_250.df_idt.head(1)['JDXZ'].values
            xz_rate = jdxz10 / jdxz250 - 1
        except Exception as e:
            log_args = [asset.ts_code, e]
            add_log(20, '[fn]rpt_d_basic() ts_code:{0[0]}; xz_rate explicit type:{0[1]} to catch', log_args)
            xz_rate = -99

        # ----吸资10QS`   `````
        try:
            xz_current, xz_previous = asset.jdxz_10.df_idt.head(2)['JDXZ'].values
            xzqs = (xz_current / xz_previous - 1) * 100
        except Exception as e:
            log_args = [asset.ts_code, e]
            add_log(20, '[fn]rpt_d_basic() ts_code:{0[0]}; xzqs explicit type:{0[1]} to catch', log_args)
            xzqs = -99

        # ----价多空头排列天数
        p_dktp, = asset.dktp_5_10_20.df_idt.head(1)['DKTP'].values

        # ----量多空头排列天数
        v_dktp, = asset.dktp_vol_5_10_20.df_idt.head(1)['DKTP'].values

        # ----添加数据
        data.append((ts_code, name, comment1, comment2, ma20_gl, maqs20, maqs60,
                     maqs_vol_10, maqs_vol_20, jh_pct, xz_rate, xzqs, p_dktp, v_dktp))

    # =================报告基础=================
    workbook = xlw.Workbook(file_path)
    ws1 = workbook.add_worksheet('日报')  # worksheet #1

    fmt_std = workbook.add_format(d_std)
    fmt_wrap = workbook.add_format(d_wrap)
    fmt_rpt_title = workbook.add_format(d_rpt_title)
    fmt_center = workbook.add_format(d_center)  # 居中
    fmt_int = workbook.add_format(d_int)  # 整数
    fmt_f1d = workbook.add_format(d_f1d)  # 1位小数
    fmt_f2d = workbook.add_format(d_f2d)  # 2位小数
    fmt_f3d = workbook.add_format(d_f3d)  # 3位小数
    fmt_pct = workbook.add_format(d_pct)  # 0%
    fmt_pct1d = workbook.add_format(d_pct1d)  # 1.1%
    fmt_pct2d = workbook.add_format(d_pct2d)  # 2.22%

    # 与data对应的显示格式
    #           ts_code       名称      备注        备注    20日归    maqs20   maqs60
    formats = [fmt_center, fmt_center, fmt_wrap, fmt_wrap, fmt_f2d, fmt_f2d, fmt_f2d]
    #                    量10QS   量20QS    聚合占比 吸资归离 吸资10QS 价多空排  量多空排
    formats = formats + [fmt_f2d, fmt_f2d, fmt_pct, fmt_f2d, fmt_f2d, fmt_int, fmt_int]

    # =================报告数据=================
    # ----标题栏
    ws1.write_string('A1', trade_day_str + '    ' + 'Report Daily Basic', fmt_rpt_title)
    ws1.set_row(0, 19)  # 第一行，标题栏行高
    ws1.set_row(1, options={'hidden': True})  # 第二行，分隔行行高
    # ----表头
    head = ('代码', '名称', '备注', '备注', '价20归%', '价20QS‰', '价60QS‰',
            '量10QS‰', '量20QS‰', '聚2%天比', '吸资归离', '吸资10QS', '价多空排', '量多空排')
    ws1.write_row('A3', head, fmt_center)
    ws1.write_comment('A1', '确保下载数据当日数据后再生成报表！')
    ws1.write_comment('E3', '(by_price / ma20_last - 1) * 100')
    ws1.write_comment('F3', 'maqs_20 * 1000')
    ws1.write_comment('G3', 'maqs_60 * 1000')
    ws1.write_comment('H3', 'maqs_vol_10 * 1000')
    ws1.write_comment('I3', 'maqs_vol_20 * 1000')
    ws1.write_comment('J3', '在20个交易日内，majh<' + str(X) + '%天数的占比 周期5, 10, 20')
    ws1.write_comment('K3', '10日吸资比 / 250日吸资比 -1')
    ws1.write_comment('L3', '10日吸资比变化率 * 100')
    ws1.write_comment('M3', '价多空头排列天数')
    ws1.write_comment('N3', '量多空头排列天数')
    # ----填充数据
    row = 3
    assert len(head) == len(formats)
    for item in data:
        for column in range(len(head)):
            ws1.write(row, column, item[column], formats[column])
        row += 1

    # =================收尾格式=================
    ws1.set_column(0, 0, 10.2)  # 代码
    ws1.set_column(1, 1, 8.2)  # 名称
    ws1.set_column(2, 3, 15)  # 备注
    max_column = len(head) - 1
    max_row = row - 1
    ws1.set_landscape()  # 页面横向
    ws1.set_paper(9)  # 设置A4纸
    ws1.center_horizontally()  # 居中
    ws1.set_margins(0.3, 0.3, 0.3, 1)
    ws1.set_footer('&C&P of &N')  # 设置页脚
    ws1.repeat_rows(0, 2)  # 重复打印行
    ws1.repeat_columns(0, 1)  # 重复打印列
    ws1.freeze_panes(3, 2)  # freeze 3行2列
    workbook.close()
    pass


if __name__ == '__main__':
    # raw_data = Raw_Data(pull=False)
    al_file = al_file_name()
    rpt_d_basic(al_file)
    pass

