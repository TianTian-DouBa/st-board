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


def rpt_fundamental(al_file):
    """
    报告：基本面信息
    al_file: <str> al_<al_file>.csv
    """
    from st_board import Strategy
    from st_board import today_str

    # =================报告文件=================
    today_s = today_str()
    trade_day_str = raw_data.last_trade_day(today_s)
    file_name = PurePath('rpt_fd_' + al_file + '_' + trade_day_str + '.xlsx')
    file_path = sub_path_rpt / file_name

    # =================初始化Strategy=================
    stg = Strategy('report_fundamental')
    stg.add_pool(desc='p10', al_file=al_file, assets_daily_data='basic', del_trsfed=None)
    p10 = stg.pools[10]

    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()
    end_date = trade_day_str
    start_date = raw_data.previous_trade_day(end_date, 2)
    stg.update_cycles(start_date=start_date, end_date=end_date)

    # =================数据准备=================
    data = []  # [(数据1, 数据2, 数据3）, (...)]存放用于报告显示的数据
    for asset in p10.assets.values():
        ts_code = asset.ts_code
        name = asset.name
        