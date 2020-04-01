"""
价格三线开花
"""
import os
os.chdir('../')  # 将工作目录定位到上一层
import pandas as pd
import numpy as np
import os
import time
import weakref
from st_common import raw_data  # 不能去掉
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt, sub_analysis, sub_pledge
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR, FORMAT_FIELDS, FORMAT_HEAD
from datetime import datetime, timedelta
from XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl
import tushare as ts
from pandas.plotting import register_matplotlib_converters
from st_board import Strategy

if __name__ == "__main__":
    from st_common import Raw_Data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)
    print('[msg] ================ stg_3bloom 三线开花 ================')
    stg = Strategy('测试')

    # ----选择策略资产列表范围
    # al_file = 'HS300成分股'
    al_file = 'try_001'

    # ----添加pools
    stg.add_pool(desc='p10初始池', al_file=al_file, in_date=None, price_seek_direction=None, del_trsfed=None)
    p10 = stg.pools[10]

    stg.add_pool(desc='p20三线开花触发', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    p20 = stg.pools[20]

    stg.add_pool(desc='p30同周期，附加筛选出', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    p30 = stg.pools[30]

    stg.add_pool(desc='p40持仓', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    p40 = stg.pools[40]

    stg.pools_brief()  # 打印pool列表

    # ++++++++pool10 calcs++++++++
    # ------calc_0
    pre_args = {'idt_type': 'majh',
                'short_n1': 5,
                'medium_n2': 20,
                'long_n3': 60}
    kwargs = {'cover': 20,
              'threshold': 2,
              'ops': '<='}
    p10.add_calc(method='coverage', pre_args=pre_args, **kwargs)

    # ========pool10 conditions========
    # ------condition_0
    pre_args1 = {'idt_type': 'dktp',
                 'short_n1': 5,
                 'medium_n2': 10,
                 'long_n3': 20,
                 'shift_periods': -1}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p10.add_condition(pre_args1, pre_args2, '<=')
    # ------condition_1
    pre_args1 = {'idt_type': 'dktp',
                 'short_n1': 5,
                 'medium_n2': 20,
                 'long_n3': 60}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p10.add_condition(pre_args1, pre_args2, '>')

    # ------filter
    p10.add_filter(cnd_indexes={0, 1}, down_pools={20}, in_price_mode='open_sxd', in_shift_days=1)

    # ========pool20 conditions========
    # ------condition_0
    pre_args1 = {'idt_type': 'jdxzqs',
                 'period': 20}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p20.add_condition(pre_args1, pre_args2, '>')

    # ------condition_1
    pre_args1 = {'idt_type': 'maqs',
                 'period': 60}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p20.add_condition(pre_args1, pre_args2, '>=')

    # ------condition_2 20周期内价聚合天数比例
    pre_args1 = {'idt_type': 'calc',
                 'calc_idx': 0}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p20.add_condition(pre_args1, pre_args2, '>=')

    # ------filter
    p20.add_filter(cnd_indexes={0, 1, 2}, down_pools={30})

    # ========初始化各pool.cnds_matrix, strategy.ref_assets========
    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()

    # ---stg循环-----------
    # stg.update_cycles(start_date='20050101', end_date='20200101')
    # stg.update_cycles(start_date='20050201', end_date='20200101')
    stg.update_cycles(start_date='20170101', cycles=20)
    # ---报告-----------
    p40.csv_in_out()

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
    pass
