import pandas as pd
import numpy as np
import os
import time
import weakref
from st_common import raw_data  # 不能去掉
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt, sub_analysis
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR, FORMAT_FIELDS, FORMAT_HEAD
from datetime import datetime, timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl
import tushare as ts
from pandas.plotting import register_matplotlib_converters

ts_pro = ts.pro_api()
register_matplotlib_converters()  # 否则Warning


def valid_date_str_fmt(date_str):
    """
    验证date_str形式格式是否正确
    date_str:<str> e.g. '20190723' YYYYMMDD
    return:<bool> True=valid
    """
    if isinstance(date_str, str):
        if len(date_str) == 8:
            return True


def today_str():
    """
    return: <str> today in 'YYYYMMDD' e.g.'20190712'
    """
    dt = datetime.now()
    today_str_ = dt.strftime("%Y%m%d")
    return today_str_

def now_time_str():
    """
    return: <str> e.g.'13-90-45'
    """
    dt = datetime.now()
    now_str = dt.strftime("%H-%M-%S")
    return now_str

def valid_file_path(file_path):
    r"""
    检验file_path的形式有效性
    file_path:<str> e.g. '.\data_csv\daily_data\xxx.csv'
    return: Ture if valid, None if invalid
    """
    if isinstance(file_path, str):
        if len(file_path) >= 3:
            return True


def date_to_date_str(dt):
    """
    将datetime转化为data_str
    dt:<datetime>
    return:<str> e.g. '20190723'
    """
    return dt.strftime("%Y%m%d")


def date_str_to_date(date_str):
    """
    将date_str转化为datetime
    date_str:<str> e.g. '20190723'
    return:<datetime>
    """
    date = datetime.strptime(date_str, "%Y%m%d")
    return date


def download_data(ts_code, category, reload=False):
    r"""
    通过调用sgmt_daily_index_download下载资产数据到数据文e.g. daily_data\d_<ts_code>.csv文件
    需要在增加资产类别时改写
    ts_code: <str> '399001.SZ'
    category: <str> in READER e.g. 'Index.load_index_daily'
    reload: <bool> True=重头开始下载
    return: <df> if success, None if fail
    """
    global raw_data
    try:
        # loader = LOADER[category]
        que_limit = QUE_LIMIT[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]download_data(). ts_code: "{0[0]}" category: "{0[1]}" incorrect', log_args)
        return

    def _category_to_file_name(ts_code, category):
        """
        ts_code: <str> '399001.SZ'
        category: <str> e.g. 'stock_daily_basic'
        return: <str> e.g. "d_<ts_code>.csv"
        """
        if category == 'stock_daily_basic':
            file_name = 'db_' + ts_code + '.csv'
        elif category == 'adj_factor':
            file_name = 'fq_' + ts_code + '.csv'
        elif category == 'index_sw' or category == 'index_sse' or category == 'index_szse' or category == 'stock':
            file_name = 'd_' + ts_code + '.csv'
        # ----------其它类型时修改------------
        else:
            log_args = [category]
            add_log(20, '[fn]download_data(). invalid category: {0[0]}', log_args)
            return
        return file_name

    def _category_to_start_date_str(ts_code, category):
        """
        ts_code: <str> '399001.SZ'
        category: <str> e.g. 'adj_factor'
        return: <str> e.g. "20190402"
        """
        global raw_data
        # -----------index类别--------------
        if category == 'index_sw' or category == 'index_sse' or category == 'index_szse':
            start_date_str = str(raw_data.index.que_base_date(ts_code))
        # -----------stock类别--------------
        elif category == 'stock':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        # -----------stock每日指标类别--------------
        elif category == 'stock_daily_basic':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        # -----------复权因子--------------
        elif category == 'adj_factor':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        # -----------其它类型(未完成)--------------
        else:
            log_args = [ts_code, category]
            add_log(20, '[fn]download_data() {0[0]} category:{0[1]} invalid', log_args)
            return
        return start_date_str

    if raw_data.valid_ts_code(ts_code):
        if reload is True:  # 重头开始下载
            # #-----------index类别--------------
            # if category == 'index_sw' or category == 'index_sse' or category == 'index_szse':
            #     start_date_str = str(raw_data.index.que_base_date(ts_code))
            # #-----------stock类别--------------
            # elif category == 'stock':
            #     start_date_str = str(raw_data.stock.que_list_date(ts_code))
            # #-----------stock每日指标类别--------------
            # elif category == 'stock_daily_basic':
            #     start_date_str = str(raw_data.stock.que_list_date(ts_code))
            # #-----------复权因子--------------
            # elif category == 'adj_factor':
            #     start_date_str = str(raw_data.stock.que_list_date(ts_code))
            # #-----------其它类型(未完成)--------------
            # else:
            #     log_args = [ts_code,category]
            #     add_log(20, '[fn]download_data() {0[0]} category:{0[1]} invalid', log_args)
            #     return
            start_date_str = _category_to_start_date_str(ts_code, category)
            if start_date_str is None:
                return
            end_date_str = today_str()
            df = sgmt_download(ts_code, start_date_str, end_date_str, que_limit, category)
            if isinstance(df, pd.DataFrame):
                # if category == 'stock_daily_basic':
                #     file_name = 'db_' + ts_code + '.csv'
                # elif category == 'adj_factor':
                #     file_name = 'fq_' + ts_code + '.csv'
                # elif category == 'index_sw' or category == 'index_sse' or category == 'index_szse' or category == 'stock':
                #     file_name = 'd_' + ts_code + '.csv'
                # #----------其它类型时修改------------
                # else:
                #     log_args = [category]
                #     add_log(20, '[fn]download_data(). invalid category: {0[0]}', log_args)
                #     return
                file_name = _category_to_file_name(ts_code, category)
                if file_name is None:
                    return
                df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                if logable(40):
                    number_of_items = len(df)
                    log_args = [ts_code, category, file_name, number_of_items]
                    add_log(40, "[fn]download_data() ts_code:{0[0]}, category:{0[1]}, file:{0[2]}, total items:{0[3]}",
                            log_args)
                return df
            else:
                log_args = [ts_code, df]
                add_log(20, '[fn]download_data() fail to get DataFrame from Tushare. ts_code: "{0[0]}" df: ', log_args)
                return
        else:  # reload != True 读入文件，看最后条目的日期，继续下载数据
            loader = LOADER[category]
            df = loader(ts_code)
            if isinstance(df, pd.DataFrame):
                try:
                    # last_date_str = df.iloc[0]['trade_date'] #注意是否所有类型都有'trade_date'字段
                    last_date_str = str(df.index.values[0])
                    # print('[L507] {}'.format(last_date_str))
                except IndexError:
                    # #-----------index类别--------------
                    # if category == 'index_sw' or category == 'index_sse' or category == 'index_szse':
                    #     last_date_str = str(raw_data.index.que_base_date(ts_code))
                    # #-----------stock类别--------------
                    # elif category == 'stock':
                    #     last_date_str = str(raw_data.stock.que_list_date(ts_code))
                    # #-----------stock每日指标类别--------------
                    # elif category == 'stock_daily_basic':
                    #     last_date_str = str(raw_data.stock.que_list_date(ts_code))
                    # #-----------复权因子--------------
                    # elif category == 'adj_factor':
                    #     last_date_str = str(raw_data.stock.que_list_date(ts_code))
                    # #-----------其它类型(未完成)--------------
                    # else:
                    #     log_args = [category]
                    #     add_log(20, '[fn]download_data(). invalid category: {0[0]}', log_args)
                    #     return
                    last_date_str = _category_to_start_date_str(ts_code, category)
                    if last_date_str is None:
                        return
                last_date = date_str_to_date(last_date_str)
                today_str_ = today_str()
                today = date_str_to_date(today_str_)  # 只保留日期，忽略时间差别
                start_date = last_date + timedelta(1)
                _start_str = date_to_date_str(start_date)
                _end_str = today_str_
                if last_date < today:
                    _df = sgmt_download(ts_code, _start_str, _end_str, que_limit, category)
                    _frames = [_df, df]
                    # df=pd.concat(_frames,ignore_index=True,sort=False)
                    df = pd.concat(_frames, sort=False)
                    # if category == 'stock_daily_basic':
                    #     file_name = 'db_' + ts_code + '.csv'
                    # elif category == 'adj_factor':
                    #     file_name = 'fq_' + ts_code + '.csv'
                    # elif category == 'index_sw' or category == 'index_sse' or category == 'index_szse' or category == 'stock':
                    #     file_name = 'd_' + ts_code + '.csv'
                    # #----------其它类型时修改------------
                    # else:
                    #     log_args = [category]
                    #     add_log(20, '[fn]download_data(). invalid category: {0[0]}', log_args)
                    #     return
                    file_name = _category_to_file_name(ts_code, category)
                    if file_name is None:
                        return
                    df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                    if logable(40):
                        number_of_items = len(df)
                        log_args = [ts_code, category, file_name, number_of_items]
                        add_log(40,
                                "[fn]download_data() ts_code:{0[0]}, category:{0[1]}, file:{0[2]}, total items:{0[3]}",
                                log_args)
                    return df
            else:
                log_args = [ts_code]
                add_log(20, '[fn]download_data() ts_code "{0[0]}" load csv fail', log_args)
                return
    else:
        log_args = [ts_code]
        add_log(20, '[fn]download_data(). ts_code "{0[0]}" invalid', log_args)
        return


def sgmt_download(ts_code, start_date_str, end_date_str, size, category):
    """
    通过TuShare API分段下载数据
    ts_code: <str> 对象的tushare代码 e.g. '399001.SZ'
    start_date_str: <str> 开始时间字符串 YYYYMMDD '19930723'
    end_date_str: <str> 结束时间字符串 YYYYMMDD '20190804'
    size: <int> 每个分段的大小 1 to xxxx
    category: <str> listed in HANDLER e.g. ts_pro.index_daily, ts_pro.sw_daily
    return: <df> if success, None if fail
    """
    TRY_TIMES = 20
    SLEEP_TIME = 20  # in seconds
    try:
        getter = GETTER[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]sgmt_download(). ts_code: "{0[0]}" category: "{0[1]}" incorrect', log_args)
        return
    df = None
    start_date = date_str_to_date(start_date_str)
    end_date = date_str_to_date(end_date_str)
    duration = (end_date - start_date).days
    _start_str = start_date_str
    while duration > size:
        _end_time = date_str_to_date(_start_str) + timedelta(size)
        _end_str = date_to_date_str(_end_time)
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code, start_date=_start_str, end_date=_end_str)
            except Exception as e:  # ConnectTimeout, 每分钟200:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
        if not isinstance(df, pd.DataFrame):
            df = _df
        else:
            _frames = [_df, df]
            df = pd.concat(_frames, ignore_index=True, sort=False)
        _start_time = _end_time + timedelta(1)
        _start_str = date_to_date_str(_start_time)
        duration = duration - size
    else:
        _end_str = end_date_str
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code, start_date=_start_str, end_date=_end_str)
            except Exception as e:  # ConnectTimeout:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
        if not isinstance(df, pd.DataFrame):
            df = _df
        else:
            _frames = [_df, df]
            df = pd.concat(_frames, ignore_index=True)
    df.set_index('trade_date', inplace=True)
    return df


def bulk_download(al_file, reload=False):
    r"""
    根据资产列表文件，批量下载数据到csv文件
    需要在增加资产类别时改写
    al_file:<str> path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    file_path = None
    if isinstance(al_file, str):
        if len(al_file) > 0:
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path is None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_download(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_download(). al_file "{0[0]}" not exist', log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_download(). df_al loaded -sucess, items:"{0[0]}"', log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category is None:
                continue
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            if reload is True or (not os.path.exists(file_path)):
                _reload = True
            else:
                _reload = False
            download_data(ts_code, category, _reload)


def bulk_dl_appendix(al_file, reload=False):
    r"""
    根据资产列表文件，批量下载每日指标daily_basic及复权因子
    al_file:<str> path for al file e.g. '.\data_csv\assets_lists\al_<al_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    print('[L697] bulk_dl_appendix()只能处理个股，对其它类别如index会将')
    file_path = None
    if isinstance(al_file, str):
        if len(al_file) > 0:
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path is None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_dl_appendix(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_dl_appendix(). al_file "{0[0]}" not exist', log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_dl_appendix(). df_al loaded -success, items:"{0[0]}"', log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category is None:
                continue
            elif category == 'stock':
                # -------------每日指标--------------
                category = 'stock_daily_basic'  # 股票每日指标
                file_name = 'db_' + ts_code + '.csv'
                file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
                if reload is True or (not os.path.exists(file_path)):
                    _reload = True
                else:
                    _reload = False
                download_data(ts_code, category, _reload)
                # -------------复权因子--------------
                category = 'adj_factor'  # 股票复权
                file_name = 'fq_' + ts_code + '.csv'
                file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
                if reload is True or (not os.path.exists(file_path)):
                    _reload = True
                else:
                    _reload = False
                download_data(ts_code, category, _reload)
            else:
                log_args = [ts_code, category]
                add_log(40, '[fn]bulk_dl_appendix(). {0[0]} category:{0[1]} skip', log_args)


def bulk_calc_dfq(al_file, reload=False):
    r"""
    根据资产列表文件，对category='stock'的，进行批量计算复权后的开收盘高低价
    al_file:<str> path for al file e.g. '.\data_csv\assets_lists\al_<al_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    file_path = None
    if isinstance(al_file, str):
        if len(al_file) > 0:
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path is None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_calc_dfq(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_calc_dfq(). al_file "{0[0]}" not exist', log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_calc_dfq(). df_al loaded -sucess, items:"{0[0]}"', log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category is None:
                continue
            elif category == 'stock':
                Stock.calc_dfq(ts_code, reload=reload)
            else:
                log_args = [ts_code, category]
                add_log(40, '[fn]bulk_calc_dfq(). {0[0]} category:{0[1]} skip', log_args)


def take_head_n(df, nrows):
    """
    df: <DataFrame>实例
    nrows: <int>需要提取的前nrows行
    return: <DataFrame>实例, None = failed
    """
    if not isinstance(df, pd.DataFrame):
        add_log(20, '[fn]take_head_n() df is not an instance of <DataFrame>')
        return
    nrows = int(nrows)
    result = df.head(nrows)
    return result


def load_source_df(ts_code, source, nrows=None):
    """
    根据source来选择不同数据源，返回df；数据源需要事先下载到csv，本函数不做补全
    source:<str> e.g. 'close_hfq' defined in st_common.SOURCE
    nrows: <int> 指定读入最近n个周期的记录,None=全部
    return:<df> trade_date(index); 数据只有一列 close; high...
    """
    try:
        # SOURCE[source]
        column_name = SOURCE_TO_COLUMN[source]
    except KeyError:
        log_args = [ts_code, source]
        add_log(20, '[fn]load_source_df ts_code:{0[0]}; source:{0[1]} not valid', log_args)
        return
    # ---------------close_hfq 收盘后复权---------------
    if source == 'close_hfq':
        result = Stock.load_stock_dfq(ts_code=ts_code, nrows=nrows)[[column_name]]
        return result
    # ---------------无匹配，报错---------------
    else:
        log_args = [ts_code, source]
        add_log(30, '[fn]load_source_df ts_code:{0[0]}; source:{0[1]} not matched', log_args)
        return


class All_Assets_List:
    """处理全资产列表"""

    @staticmethod
    def rebuild_all_assets_list(que_from_ts=False):
        """
        重头开始构建全资产列表
        que_from_ts: <bool> F：从文件读 T:从tushare 接口读
        """
        global raw_data
        file_name = "all_assets_list_rebuild.csv"  # 不同名避免误操作
        file_path_al = sub_path + sub_path_config + '\\' + file_name
        df_al = pd.DataFrame(columns=['ts_code', 'valid', 'selected', 'name', 'type', 'stype1', 'stype2'])
        df_al = df_al.set_index('ts_code')
        # --------------SW 指数---------------
        if que_from_ts is True:
            df_l1, df_l2, df_l3 = Index.get_sw_index_classify()
            df_l1 = df_l1[['index_code', 'industry_name']]
            df_l2 = df_l2[['index_code', 'industry_name']]
            df_l3 = df_l3[['index_code', 'industry_name']]
        else:
            file_path_sw_l1 = sub_path + '\\' + 'index_sw_L1_list.csv'
            file_path_sw_l2 = sub_path + '\\' + 'index_sw_L2_list.csv'
            file_path_sw_l3 = sub_path + '\\' + 'index_sw_L3_list.csv'
            try:
                _file_path = file_path_sw_l1
                df_l1 = pd.read_csv(_file_path, usecols=['index_code', 'industry_name'])
                _file_path = file_path_sw_l2
                df_l2 = pd.read_csv(_file_path, usecols=['index_code', 'industry_name'])
                _file_path = file_path_sw_l3
                df_l3 = pd.read_csv(_file_path, usecols=['index_code', 'industry_name'])
            except FileNotFoundError:
                log_args = [_file_path]
                add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found', log_args)
                return
        df_l1.rename(columns={'index_code': 'ts_code', 'industry_name': 'name'}, inplace=True)
        df_l1['valid'] = 'T'
        df_l1['selected'] = 'T'
        df_l1['type'] = 'index'
        df_l1['stype1'] = 'SW'
        df_l1['stype2'] = 'L1'
        df_l1.set_index('ts_code', inplace=True)
        df_l2.rename(columns={'index_code': 'ts_code', 'industry_name': 'name'}, inplace=True)
        df_l2['valid'] = 'T'
        df_l2['selected'] = 'T'
        df_l2['type'] = 'index'
        df_l2['stype1'] = 'SW'
        df_l2['stype2'] = 'L2'
        df_l2.set_index('ts_code', inplace=True)
        df_l3.rename(columns={'index_code': 'ts_code', 'industry_name': 'name'}, inplace=True)
        df_l3['valid'] = 'T'
        df_l3['selected'] = 'T'
        df_l3['type'] = 'index'
        df_l3['stype1'] = 'SW'
        df_l3['stype2'] = 'L3'
        df_l3.set_index('ts_code', inplace=True)
        _frame = [df_al, df_l1, df_l2, df_l3]
        df_al = pd.concat(_frame, sort=False)

        # --------------上交所指数---------------
        if que_from_ts is True:
            raw_data.index.get_index_basic()
        _file_path = sub_path + '\\' + 'index_basic_sse.csv'
        try:
            df_sse = pd.read_csv(_file_path, usecols=['ts_code', 'name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found', log_args)
            return
        df_sse['valid'] = 'T'
        df_sse['selected'] = 'T'
        df_sse['type'] = 'index'
        df_sse['stype1'] = 'SSE'
        df_sse['stype2'] = ''
        df_sse.set_index('ts_code', inplace=True)
        _file_path = sub_path + '\\' + 'index_basic_szse.csv'
        try:
            df_szse = pd.read_csv(_file_path, usecols=['ts_code', 'name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found', log_args)
            return
        df_szse['valid'] = 'T'
        df_szse['selected'] = 'T'
        df_szse['type'] = 'index'
        df_szse['stype1'] = 'SZSE'
        df_szse['stype2'] = ''
        df_szse.set_index('ts_code', inplace=True)
        _frame = [df_al, df_sse, df_szse]
        df_al = pd.concat(_frame, sort=False)
        # --------------个股---------------
        if que_from_ts is True:
            raw_data.stock.get_stock_basic()
        file_name = 'stock_basic.csv'
        _file_path = sub_path + '\\' + file_name
        try:
            df = pd.read_csv(_file_path, usecols=['ts_code', 'name'], index_col='ts_code')
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found', log_args)
            return
        df['valid'] = 'T'
        df['selected'] = 'T'
        df['type'] = 'stock'
        # df['stype1'] = ''
        # df['stype2'] = ''
        df.loc[df.index.str.startswith('600'), 'stype1'] = 'SHZB'  # 上海主板
        df.loc[df.index.str.startswith('601'), 'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('602'), 'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('603'), 'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('688'), 'stype1'] = 'KCB'  # 科创板
        df.loc[df.index.str.startswith('000'), 'stype1'] = 'SZZB'  # 深圳主板
        df.loc[df.index.str.startswith('001'), 'stype1'] = 'SZZB'
        df.loc[df.index.str.startswith('002'), 'stype1'] = 'ZXB'  # 中小板
        df.loc[df.index.str.startswith('003'), 'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('004'), 'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('300'), 'stype1'] = 'CYB'  # 创业板
        df['stype2'] = ''
        _frame = [df_al, df]
        df_al = pd.concat(_frame, sort=False)
        # --------------结尾---------------
        df_al.to_csv(file_path_al, encoding="utf-8")
        return

    @staticmethod
    def query_category_str(ts_code):
        """
        根据all_assets_list中的type, stype1, stype2字段来返回category
        ts_code: <str> e.g. '000001.SZ'
        return: None, if no match
                <str> e.g. 'index_sw'; 'stock'
        """
        global raw_data
        name, _type, stype1, stype2 = raw_data.all_assets_list.loc[ts_code][['name', 'type', 'stype1', 'stype2']]
        category = None  # 资产的类别，传给下游[fn]处理
        # --------------申万指数---------------
        if _type == 'index' and stype1 == 'SW':
            category = 'index_sw'
        # --------------上证指数---------------
        elif _type == 'index' and stype1 == 'SSE':
            category = 'index_sse'
        # --------------深圳指数---------------
        elif _type == 'index' and stype1 == 'SZSE':
            category = 'index_szse'
        # --------------个股---------------
        # 'stock_daily_basic'和'adj_factor'不在此处理
        elif _type == 'stock':
            category = 'stock'
        # --------------其它类型(未完成)----------
        else:
            log_args = [ts_code]
            add_log(20, '[fn]All_Assets_List.query_category_str(). No matched category for "{0[0]}"', log_args)
            return
        return category

    @staticmethod
    def load_al_file(al_file=None):
        r"""
        load al_file into assets
        al_file:None = create empty <df>; <str> = path for al file e.g. '.\data_csv\assets_lists\al_<al_file>.csv'
        """
        file_path = None
        if al_file is None:
            df_al = pd.DataFrame(columns=['ts_code', 'selected'])
            df_al.set_index('ts_code', inplace=True)
            add_log(40, '[fns]All_Assets_List.load_al_file() empty <df> created')
        elif isinstance(al_file, str):
            if len(al_file) > 0:
                file_name = 'al_' + al_file + '.csv'
                file_path = sub_path + sub_path_al + '\\' + file_name
            if file_path is None:
                log_args = [al_file]
                add_log(10, '[fns]All_Assets_List.load_al_file(). invalid al_file string: {0[0]}', log_args)
                return
            try:
                df_al = pd.read_csv(file_path, index_col='ts_code')
            except (FileNotFoundError, UnicodeDecodeError):
                log_args = [file_path]
                add_log(10, '[fns]All_Assets_List.load_al_file(). al_file "{0[0]}" not exist', log_args)
                return
        log_args = [len(df_al)]
        add_log(40, '[fns]All_Assets_List.load_al_file(). df_al loaded -success, items:"{0[0]}"', log_args)
        return df_al

    @staticmethod
    def create_al_file(input_list, file_name, overwrite=True):
        """
        根据input_list输入的ts_code列表，生成以al_[file_name].csv命名的资产列表文件
        input_list: <list> e.g. ['000001.SZ', '600032.SH']
        file_name: <str> e.g. 'try001' ->  al_try001.csv
        overwrite: <bool> True=overwrite existing file
        """
        if not isinstance(input_list, list):
            log_args = [type(input_list)]
            add_log(20, '[fn]All_Assets_List.create_al_file(). Type of input_list "{0[0]}" must be <list>', log_args)
            return

        if not (len(input_list) > 0):
            add_log(20, '[fn]All_Assets_List.create_al_file(). empty input_list')
            return

        file_path = sub_path + sub_path_al + r'\al_' + file_name + '.csv'

        mode = "w" if overwrite is True else "x"

        print("[L697] except for file overwrite not ready")
        with open(file_path, mode) as f:
            f.write("ts_code,selected\n")
            for ts_code in input_list:
                line = ts_code + ',T\n'
                f.write(line)

        log_args = ['al_' + file_name + '.csv']
        add_log(40, '[fn]All_Assets_List.create_al_file(). "{0[0]}" updated', log_args)
        return

    @staticmethod
    def update_hs300_al():
        """
        更新沪深300的 al 文件
        """
        df = ts_pro.index_weight(index_code='399007.SZ')
        trade_date = df.head(1)['trade_date'].values[0]
        s_ts_code = df[df.trade_date == trade_date]['con_code']
        al_list = s_ts_code.tolist()
        All_Assets_List.create_al_file(al_list, 'HS300成分股')


class Asset:
    """
    资产的基类
    """

    def __init__(self, ts_code, in_date=None, load_daily='basic'):
        """
        in_date: None: 不提供
                 'latest': 根据基础数据里取有价格的最新那个时间
                 '20191231': 指定的日期
        load_daily: 'basic': 读入[close][open][high][low][amount]基本字段到self.daily_data
                  : None: self.daily_data = None
                  : set('raw_close', 'raw_vol'...) 基本字段外的其他补充字段
        """
        self.ts_code = ts_code
        self.in_date = in_date  # <str> 如"20200126", 如果是'latest'则需要在各子类中计算
        self.by_date = None  # <str> 当前计算的日期如"20191231"
        self.stay_days = None  # <int> 在pool中的天数
        self.in_price = None  # <float>加入pool的价格
        self.by_price = None  # 当前计算的价格
        self.out_price = None  # 出pool时的价格，受get_price()参数，及交割费用影响
        self.out_date = None  # <str> 当前计算的日期如"20191231"
        self.earn = None  # by_price - in_price
        self.earn_pct = None  # earn / in_price
        self.daily_data = None  # <df>
        self.high_in_pool = None  # <float> 在pool期间的最高价
        self.high_pct = None  # <float> (high_in_pool - in_price) / in_price
        self.low_in_pool = None  # <float> 在pool期间的最低价
        self.low_pct = None  # <float> (low_in_pool - in_price) / in_price

    def load_daily_data(self):
        """
        在子类中改写
        """
        pass

    def get_price(self, trade_date, mode='close', seek_direction=None, shift_days=0):
        """
        根据mode返回价格
        trade_date: <str> 如 '20200126'
        mode: <str>
              'close' 返回当日的close收盘价，股票都为后复权值
              'open' 返回当日的open开盘价，股票都为后复权值
              'high' 返回当日的high最高价，股票都为后复权值
              'low' 返回当日的low最低价，股票都为后复权值
              'close_sxd' 返回当日偏离x周期的close收盘价，股票都为后复权值
              'close_open'
              'high_sxd'
              'low_sxd'
              其它还未完成
        seek_direction: <str> 当价格数据不在daily_data中，比如停牌是，向前或后搜索数据
                       None: 返回None,不搜索
                       'forwards': 向时间增加的方向搜索
                       'backwards': 向时间倒退的方向搜索
        shift_days: <int> 价格偏离的日期，-n往早移  +n往晚移
        return: (price <float>, trade_date <str>)
                None 并报错
        """
        global raw_data
        SEEK_DAYS = 365  # 前后查找数据的最大天数
        is_open = raw_data.valid_trade_date(trade_date)
        if is_open is True:
            if mode == 'close' or mode == 'open' or mode == 'high' or mode == 'low':
                if seek_direction is None:
                    try:
                        rslt = self.daily_data.loc[int(trade_date)][mode]
                        return rslt, trade_date
                    except KeyError:
                        log_args = [self.ts_code, trade_date]
                        add_log(20, '[fn]Asset.get_price() failed to get price of {0[0]} on {0[1]}', log_args)
                        return
                elif seek_direction == 'forwards':
                    _trade_date = trade_date
                    for i in range(SEEK_DAYS):  # 检查SEEK_DAYS天内有没有数据
                        if int(_trade_date) in self.daily_data.index:
                            rslt = self.daily_data.loc[int(_trade_date)][mode]
                            return rslt, _trade_date
                        else:
                            _trade_date = raw_data.next_trade_day(_trade_date)
                            continue
                    log_args = [self.ts_code, trade_date, SEEK_DAYS]
                    add_log(20, '[fn]Asset.get_price() failed to get price of {0[0]} on {0[1]} and next {0[2]} days', log_args)
                    return
                elif seek_direction == 'backwards':
                    _trade_date = trade_date
                    for i in range(SEEK_DAYS):  # 检查SEEK_DAYS天内有没有数据
                        if int(_trade_date) in self.daily_data.index:
                            rslt = self.daily_data.loc[int(_trade_date)][mode]
                            return rslt, _trade_date
                        else:
                            _trade_date = raw_data.previous_trade_day(_trade_date)
                            continue
                    log_args = [self.ts_code, trade_date, SEEK_DAYS]
                    add_log(20, '[fn]Asset.get_price() failed to get price of {0[0]} on {0[1]} and previous {0[2]} days', log_args)
                else:
                    log_args = [seek_direction]
                    add_log(20, '[fn]Asset.get_price() seek_direction "{0[0]}" invalid',
                            log_args)
                    return
            elif mode == 'close_sxd' or mode == 'open_sxd' or mode == 'high_sxd' or mode == 'low_sxd':
                if mode == 'close_sxd':
                    head_name = 'close'
                elif mode == 'open_sxd':
                    head_name = 'open'
                elif mode == 'high_sxd':
                    head_name = 'high'
                elif mode == 'low_sxd':
                    head_name = 'low'
                else:
                    head_name = None

                if seek_direction is None:
                    try:
                        pos = self.daily_data.index.get_loc(int(trade_date))
                        rslt = self.daily_data.iloc[pos - shift_days][head_name]
                        trade_date = str(self.daily_data.iloc[pos - shift_days].name)
                        return rslt, trade_date
                    except (KeyError, IndexError):
                        log_args = [self.ts_code, trade_date]
                        add_log(20, '[fn]Asset.get_price() failed to get price of {0[0]} on {0[1]}', log_args)
                        return
                else:
                    log_args = [self.ts_code, seek_direction]
                    add_log(20, '[fn]Asset.get_price() ts_code:{0[0]}, seek_direction of  {0[1]} not configured yet', log_args)
                    return
            # ----------------其它模式待完善-----------------
        else:
            log_args = [self.ts_code, trade_date]
            add_log(20, '[fn]Asset.get_price(). {0[0]} trade_date:"{0[1]}" is not a trade day', log_args)

    def add_indicator(self, idt_class, **post_args):
        """
        添加Indicator的实例
        idt_name: 指标名 <str> e.g. 'ma_10'
        idt_class: 指标类 <Class> e.g. Ma
        """
        from indicator import Indicator
        idt_name_ = post_args['idt_name']
        par_asset = weakref.ref(self)  # 用par_asset()应用原对象
        # print('[L675] before instance Indicator')
        idt = idt_class(ts_code=self.ts_code, par_asset=par_asset(), **post_args)
        # print('[L677] exit instance Indicator')
        setattr(self, idt_name_, idt)

        try:
            idt = getattr(self, idt_name_)
        except Exception as e:
            log_args = [self.ts_code, idt_name_, e.__class__.__name__]
            add_log(10, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed. Except:{0[2]}', log_args)
            return

        if isinstance(idt, Indicator):
            idt.calc_idt()
        else:
            log_args = [self.ts_code, idt_name_, type(idt)]
            add_log(20, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed. idt_type: {0[2]}', log_args)
            return

        # print('[L692] exit add_indicator()')
        log_args = [self.ts_code, idt_name_]
        add_log(40, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} succeed.', log_args)

    def valid_idt_utd(self, idt_name):
        """
        validate if self.[ins]Indicator up to df_source date
        idt_name: <str> e.g. 'ema12_close_hfq'
        return: True: up to date;
                None: invalid or not uptodate
        """
        from indicator import Indicator
        if hasattr(self, idt_name):
            idt = getattr(self, idt_name)
            if isinstance(idt, Indicator):
                result = idt.valid_utd()
                return result
            else:
                log_args = [self.ts_code, idt_name]
                add_log(20, '[fn]Asset.valid_idt_utd() ts_code:{0[0]}, idt_name:{0[1]} is not an Indicator.', log_args)
                return
        else:
            log_args = [self.ts_code, idt_name]
            add_log(20, '[fn]Asset.valid_idt_utd() ts_code:{0[0]}, idt_name:{0[1]} does not exist.', log_args)
            return


class Stock(Asset):
    """
    股票类的资产
    in_date: None: 无效
             'latest': 根据基础数据里取有价格的最新那个时间
    """

    def __init__(self, ts_code, in_price=None, in_date=None, load_daily='basic', in_price_mode='close', price_seek_direction=None):
        """
        load_daily: 'basic': 读入[close][open][high][low]基本字段到self.daily_data
                     None: self.daily_data = None
                     set('raw_close', 'amount'...) 基本字段外的其他补充字段
        in_price: <float> 进入价格
        in_price_mode: 'close', 'high', 'low'等，详见Asset.get_price()
        price_seek_direction: <str> 当价格数据不在daily_data中，比如停牌是，向前或后搜索数据
                               None: 返回None,不搜索
                               'forwards': 向时间增加的方向搜索
                              'backwards': 向时间倒退的方向搜索
        """
        global raw_data
        Asset.__init__(self, ts_code=ts_code, in_date=in_date)

        # daily_data的其它字段还待完成
        if load_daily is not None:
            self.load_daily_data(load_daily=load_daily)

        # 处理self.in_date
        if isinstance(self.in_date, str):
            if self.in_date == 'latest':
                self.in_date = str(self.daily_data[0]['trade_date'])
            elif raw_data.valid_trade_date(self.in_date) is not True:
                log_args = [self.ts_code, self.in_date]
                add_log(20, '[fn]Stock.__init__(). "{0[0]}" in_date "{0[1]}" is not a trade date', log_args)
                self.in_date = None

        # 处理self.in_price
        if self.in_date is not None:
            if in_price is None:
                rslt = self.get_price(trade_date=self.in_date, mode=in_price_mode, seek_direction=price_seek_direction)
                if rslt is not None:
                    self.in_price, self.in_date = rslt
                else:
                    self.in_price = None
                    self.in_date = None
            else:
                self.in_price = float(in_price)
        else:
            self.in_price = None

    def load_daily_data(self, load_daily='basic'):
        """
        根据load_daily输入字段，从csv文件将数据读入self.daily_data
        load_daily: 'basic': 读入[close][open][high][low]基本字段到self.daily_data
                     None: self.daily_data = None
                     set('raw_close', 'amount'...) 基本字段外的其他补充字段
        """
        if load_daily == 'basic':
            self.daily_data = self.load_stock_dfq(ts_code=self.ts_code, columns='basic')
            return
        elif isinstance(load_daily, set):
            print('[L820] 扩展字段的情况待完成')

    @staticmethod
    def load_adj_factor(ts_code, nrows=None):
        """
        从文件读入复权因子
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'fq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path, dtype={'trade_date': str}, usecols=['ts_code', 'trade_date', 'adj_factor'],
                                 index_col='trade_date', nrows=nrows)
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_adj_factor() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def load_stock_daily(ts_code, nrows=None, columns='all'):
        """
        从文件读入股票日线数据，columns的修改不必要20200126
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        columns: <str> 'all' = 所有可用的列
                       'basic' = 'trade_date', 'close', 'open', 'high', 'low', 'amount'
        return: <df> index = 'trade_date'
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            if columns == 'all':
                result = pd.read_csv(file_path, dtype={'trade_date': str},
                                     usecols=['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'pre_close',
                                              'change', 'pct_chg', 'vol', 'amount'], index_col='trade_date', nrows=nrows)
                result['vol'] = result['vol'].astype(np.int64)
            elif columns == 'basic':
                result = pd.read_csv(file_path, dtype={'trade_date': str},
                                     usecols=['trade_date', 'close', 'open', 'high', 'low', 'amount'], index_col='trade_date', nrows=nrows)
            # 待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_daily() ts_code:{0[0]} invalid', log_args)
            return

    @staticmethod
    def load_stock_daily_basic(ts_code, nrows=None):
        """
        从文件读入股票日线指标数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'db_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path, dtype={'trade_date': str},
                                 usecols=['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f',
                                          'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_share',
                                          'float_share', 'free_share', 'total_mv', 'circ_mv'], index_col='trade_date',
                                 nrows=nrows)
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_daily_basic() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def load_stock_dfq(ts_code, nrows=None, columns='all'):
        """
        从文件读入后复权的股票日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        columns: <str> 'all' = 所有可用的列
                       'basic' = 'trade_date', 'close', 'open', 'high', 'low'
        return: <df>
        """
        global raw_data
        # print('[L940] raw_data:{}'.format(raw_data))
        if raw_data.valid_ts_code(ts_code):
            file_name = 'dfq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            if columns == 'all':
                result = pd.read_csv(file_path, dtype={'trade_date': str},
                                     usecols=['trade_date', 'adj_factor', 'close', 'open', 'high', 'low'],
                                     index_col='trade_date', nrows=nrows)
            elif columns == 'basic':
                result = pd.read_csv(file_path, dtype={'trade_date': str},
                                     usecols=['trade_date', 'close', 'open', 'high', 'low'],
                                     index_col='trade_date', nrows=nrows)
            else:
                log_args = [columns]
                add_log(20, '[fn]Stock.load_stock_dfq() attribute columns "{0[0]}" invalid', log_args)
                return
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_dfq() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def calc_dfq(ts_code, reload=False):
        """
        计算后复权的日线数据
        ts_code: <str> e.g. '000001.SZ'
        reload: <bool> True重头创建文件
        """
        global raw_data
        df_fq = Stock.load_adj_factor(ts_code)[['adj_factor']]
        fq_head_index_str, = df_fq.head(1).index.values
        # print('[354] latest_date_str:{}'.format(fq_head_index_str))
        df_stock = Stock.load_stock_daily(ts_code)[['close', 'open', 'high', 'low', 'vol', 'amount']]

        def _create_dfq():
            # ---[drop]通过将df_factor头部的index定位到df_stock中行号x；x=0 无操作；x>0 drop df_stock前x行; 无法定位，倒查定位到df_factor中y，y>=0 无操作，无法定位 报错
            fq_head_in_stock = None
            try:
                fq_head_in_stock = df_stock.index.get_loc(fq_head_index_str)  # fq头在stock中的位置
            except KeyError:
                stock_head_index_str, = df_stock.head(1).index.values
                try:
                    df_fq.index.get_loc(stock_head_index_str)
                except KeyError:
                    log_args = [ts_code]
                    add_log(20, '[fn]calc_dfq() ts_code:{0[0]}; head_index mutually get_loc fail; unknown problem',
                            log_args)  # df_stock和df_fq(复权）相互查询不到第一条index的定位
                    return
            # print('[357] fq_head_in_stock position:{}'.format(fq_head_in_stock))
            if fq_head_in_stock is not None:
                if fq_head_in_stock > 0:
                    df_stock.drop(df_stock.index[:fq_head_in_stock], inplace=True)
            # ---[/drop]
            with pd.option_context('mode.chained_assignment', None):  # 将包含代码的SettingWithCopyWarning暂时屏蔽
                df_stock.loc[:, 'adj_factor'] = df_fq['adj_factor']
                df_stock.loc[:, 'dfq_cls'] = df_stock['close'] * df_stock['adj_factor']
                df_stock.loc[:, 'dfq_open'] = df_stock['open'] * df_stock['adj_factor']
                df_stock.loc[:, 'dfq_high'] = df_stock['high'] * df_stock['adj_factor']
                df_stock.loc[:, 'dfq_low'] = df_stock['low'] * df_stock['adj_factor']
            df_dfq = df_stock[['adj_factor', 'dfq_cls', 'dfq_open', 'dfq_high', 'dfq_low']]
            df_dfq.rename(columns={'dfq_cls': 'close', 'dfq_open': 'open', 'dfq_high': 'high', 'dfq_low': 'low'},
                          inplace=True)
            return df_dfq

        def _generate_from_begin():
            """
            从头开始创建dfq文件
            """
            result = _create_dfq()
            if isinstance(result, pd.DataFrame):
                result.to_csv(file_path, encoding="utf-8")
                log_args = [file_name]
                add_log(40, '[fn]:Stock.calc_dfq() file: "{0[0]}" reloaded".', log_args)

        if raw_data.valid_ts_code(ts_code):
            file_name = 'dfq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
        else:
            log_args = [ts_code]
            add_log(10, '[fn]Stock.calc_dfq() ts_code:{0[0]} invalid', log_args)
            return
        if reload is True:
            _generate_from_begin()
            return
        else:  # read dfq file, calculate and fill back the new items
            try:
                df_dfq = Stock.load_stock_dfq(ts_code)
            except FileNotFoundError:
                log_args = [file_path]
                add_log(20, '[fn]Stock.calc_dfq() file "{0[0]}" not exist, regenerate', log_args)
                _generate_from_begin()
                return
            dfq_head_index_str, = df_dfq.head(1).index.values
            try:
                dfq_head_in_stock = df_stock.index.get_loc(dfq_head_index_str)
            except KeyError:
                log_args = [ts_code, dfq_head_index_str]
                add_log(20,
                        '[fn]calc_dfq() ts_code:{0[0]}; dfq_head_index_str:"{0[1]}" not found in df_stock, df_stock maybe not up to date',
                        log_args)
                return
            if dfq_head_in_stock == 0:
                log_args = [ts_code]
                add_log(40, '[fn]calc_dfq() ts_code:{0[0]}; df_dfq up to df_stock date, no need to update', log_args)
                return
            elif dfq_head_in_stock > 0:
                df_stock = take_head_n(df_stock, dfq_head_in_stock)
            try:
                dfq_head_in_fq = df_fq.index.get_loc(dfq_head_index_str)
            except KeyError:
                log_args = [ts_code, dfq_head_index_str]
                add_log(20,
                        '[fn]calc_dfq() ts_code:{0[0]}; dfq_head_index_str:"{0[1]}" not found in df_fq, df_fq maybe not up to date',
                        log_args)
                return
            if dfq_head_in_fq == 0:
                log_args = [ts_code]
                add_log(40, '[fn]calc_dfq() ts_code:{0[0]}; df_dfq up to df_fq date, no need to update', log_args)
                return
            elif dfq_head_in_fq > 0:
                df_fq = take_head_n(df_fq, dfq_head_in_fq)
            _df_dfq = _create_dfq()
            _frames = [_df_dfq, df_dfq]
            result = pd.concat(_frames, sort=False)
            result.to_csv(file_path, encoding="utf-8")
            log_args = [file_name]
            add_log(40, '[fn]:Stock.calc_dfq() file: "{0[0]}" updated".', log_args)


class Index:
    """
    指数相关，包括行业板块指数等
    """

    @staticmethod
    def get_sw_index_classify(return_df=True):
        """
        从ts_pro获取申万行业指数的分类
        """
        # 一级行业列表
        file_name = "index_sw_L1_list.csv"
        df_l1 = ts_pro.index_classify(level='L1', src='SW', fields='index_code,industry_name,level,industry_code,src')
        df_l1.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        # 二级行业列表
        file_name = "index_sw_L2_list.csv"
        df_l2 = ts_pro.index_classify(level='L2', src='SW', fields='index_code,industry_name,level,industry_code,src')
        df_l2.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        # 三级行业列表
        file_name = "index_sw_L3_list.csv"
        df_l3 = ts_pro.index_classify(level='L3', src='SW', fields='index_code,industry_name,level,industry_code,src')
        df_l3.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        if return_df is not True:
            return None
        return df_l1, df_l2, df_l3

    @staticmethod
    def load_index_daily(ts_code, nrows=None):
        """
        从文件读入指数日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            result = pd.read_csv(sub_path + sub_path_2nd_daily + '\\' + file_name, dtype={'trade_date': str},
                                 usecols=['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'pre_close',
                                          'change', 'pct_chg', 'vol', 'amount'], index_col='trade_date', nrows=nrows)
            result['vol'] = result['vol'].astype(np.int64)
            # 待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_index_daily() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def load_sw_daily(ts_code, nrows=None):
        """
        从文件读入指数日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        sub_path_2nd_daily = r"\daily_data"
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            try:
                result = pd.read_csv(file_path, dtype={'trade_date': str},
                                     usecols=['ts_code', 'trade_date', 'name', 'open', 'low', 'high', 'close', 'change',
                                              'pct_change', 'vol', 'amount', 'pe', 'pb'], index_col='trade_date',
                                     nrows=nrows)
            except FileNotFoundError:
                log_args = [file_path]
                add_log(20, '[fn]Index.load_sw_daily() "{0[0]}" not exist', log_args)
                return
            result['vol'] = result['vol'].astype(np.int64)
            # 待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_sw_daily() ts_code "{0[0]}" invalid', log_args)
            return


class Analysis:
    """
    分析相关
    """
    @staticmethod
    def select_in_out_csv():
        """
        选择要操作的io_xxxx.csv文件，读入返回<df>
        return: <df> of in_out
                None failed
        """
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()  # 隐藏主窗口
        file_path = filedialog.askopenfilename()
        io_df = pd.read_csv(file_path, index_col=0)
        return io_df

    @staticmethod
    def in_out_agg(io_df):
        """
        显示<df>in_out的统计信息
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
        stay_days, max_days, min_days = io_df['stay_days'].agg(['mean', 'max', 'min'])
        # 结果展示
        print('=============================')
        print('in_out Aggregate:')
        print('Number of Records:        {:>8}        Stay Days(avg):  {:8.1f}'.format(n_records, stay_days))
        print('Number of Positive Earns: {:>8}        Stay Days(max):  {:8.0f}'.format(n_positive, max_days))
        print('Number of Negative Earns: {:>8}        Stay Days(min):  {:8.0f}'.format(n_negative, min_days))
        print('Average Earn%:        {:12.2%}'.format(avg_earn_pct))
        print('Median Earn%:         {:12.2%}'.format(median_earn_pct))
        print('=============================')


# LOADER读入.csv数据的接口
LOADER = {'index_sse': Index.load_index_daily,
          'index_szse': Index.load_index_daily,
          'index_sw': Index.load_sw_daily,
          'stock': Stock.load_stock_daily,
          'stock_daily_basic': Stock.load_stock_daily_basic,
          'adj_factor': Stock.load_adj_factor}
# GETTER从Tushare下载数据的接口
GETTER = {'index_sse': ts_pro.index_daily,
          'index_szse': ts_pro.index_daily,
          'index_sw': ts_pro.sw_daily,
          'stock': ts_pro.daily,
          'stock_daily_basic': ts_pro.daily_basic,
          'adj_factor': ts_pro.adj_factor}
# QUE_LIMIT,Tushare接口的单查询条目上限
QUE_LIMIT = {'index_sse': 8000,
             'index_szse': 8000,
             'index_sw': 1000,
             'stock': 4000,
             'stock_daily_basic': 4000,  # changed on '20200105', 8000 before
             'adj_factor': 8000}


class Plot_Utility:
    """存放绘图出报告用的公用工具"""

    @staticmethod
    def gen_al(al_name=None, ts_code=None, valid='T', selected='T', type_=None, stype1=None, stype2=None):
        r"""生成资产列表文件.\assets_lists\al_<name>.csv
        al_name: default=None只返回df不生成文件；<str> 填充文件名的<name>部分
        ts_code: <str> default=None 全部，[未完成] 考虑批量传入
        valid：<str> default='T' 筛选出值为'T'的，None=全部
        selected：<str> default='T' 筛选出值为'T'的，None=全部
        type: <str> default=None 全部， 其它<str>按此筛选
        stype1: <str> default=None 全部， 其它<str>按此筛选
        stype2: <str> default=None 全部， 其它<str>按此筛选
        return: <df> or None
        """
        global raw_data
        if raw_data.valid_all_assets_list:
            _aal = raw_data.all_assets_list  # all=all_assets_list
            if ts_code is None:
                al = _aal
            else:  # [未完成] 考虑批量传入
                print("[not complete L285] 考虑批量传入")
                return
            # -------valid-----------
            if valid == 'T':
                al = al[(al.valid == 'T')]
            elif isinstance(valid, str):
                al = al[(al.valid == valid)]
            # -------selected-----------
            if selected == 'T':
                al = al[(al.selected == 'T')]
            elif isinstance(selected, str):
                al = al[(al.valid == selected)]
            # -------type-----------
            if isinstance(type_, str):
                al = al[(al.type == type_)]
            # -------stype1-----------
            if isinstance(stype1, str):
                al = al[(al.stype1 == stype1)]
            # -------stype2-----------
            if isinstance(stype2, str):
                al = al[(al.stype2 == stype2)]

            al = al['selected']
        else:
            add_log(10, '[fn]Plot_Utility.gen_al(). raw_data.all_assets_list is not valid')
            return
        if isinstance(al_name, str):
            if len(al_name) > 0:
                file_name = 'al_' + al_name + '.csv'
                file_path = sub_path + sub_path_al + '\\' + file_name
                al.to_csv(file_path, encoding='utf-8', header=True)  # header=True是Series.to_csv的处理，否则Warning
                log_args = [al_name]
                add_log(40, '[fn]Plot_Utility.gen_al(). "al_{0[0]}.csv" generated', log_args)
            else:
                log_args = [al_name]
                add_log(20, '[fn]Plot_Utility.gen_al(). al_name invalid. "al_{0[0]}.csv" file not generated', log_args)
        return al


class Plot_Assets_Racing:
    """资产竞速图表：不同资产从同一基准起跑，一定时间内的价格表现
    """

    def __init__(self, al_file, period=30):
        """
        al_file: <str> 资产表的文件名e.g.'al_SW_Index_L1.csv'
        period: <int> 比较的周期
        """
        global raw_data
        al_path = sub_path + sub_path_al + '\\' + al_file
        try:
            al_df = pd.read_csv(al_path)
            # print("[debug L335] al_df:{}".format(al_df))
        except FileNotFoundError:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). file "{0[0]}" not found', log_args)
            return
        al_df.set_index('ts_code', inplace=True)
        # print("[debug L341] al_df:{}".format(al_df))
        self.al = al_df[al_df == 'T'].index  # index of ts_code
        if len(self.al) == 0:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). no item in "{0[0]}"', log_args)
            return
        fig = plt.figure()
        ax = fig.add_subplot(111)
        _aal = raw_data.all_assets_list
        self.raw_data = pd.DataFrame(columns=['ts_code', 'name', 'base_close', 'last_chg', 'df'])
        self.raw_data.set_index('ts_code', inplace=True)
        for ts_code in self.al:
            # print("[debug L349] ts_code:{}".format(ts_code))
            name, _type, stype1, stype2 = _aal.loc[ts_code][['name', 'type', 'stype1', 'stype2']]
            handler = None
            # --------------申万指数---------------
            if _type == 'index' and stype1 == 'SW':
                handler = Index.load_sw_daily
            # --------------上证及深圳指数---------------
            if _type == 'index' and (stype1 == 'SSE' or stype1 == 'SZSE'):
                handler = Index.load_index_daily
            # --------------个股类型---------------
            if _type == 'stock':
                handler = Stock.load_stock_daily
            # --------------handler = (未完成)----------
            if handler is None:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing.__init__(). No matched handler for "{0[0]}"', log_args)
                continue
            # print("[debug L777] df:{}".format(handler))
            df = handler(ts_code=ts_code, nrows=period)
            if isinstance(df, pd.DataFrame):
                log_args = [ts_code]
                add_log(40, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -success-', log_args)
            else:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -fail-', log_args)
                continue
            # df = df[['trade_date','close']]
            df = df[['close']]  # 创建单列df而不是serial
            # df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.index = pd.to_datetime(df.index.astype('str'))
            # df.set_index('trade_date', inplace=True)
            base_close, = df.tail(1)['close'].values
            df['base_chg_pct'] = (df['close'] / base_close - 1) * 100
            last_chg, = df.head(1)['base_chg_pct'].values
            row = pd.Series(
                {'ts_code': ts_code, 'name': name, 'base_close': base_close, 'last_chg': last_chg, 'df': df},
                name=ts_code)
            # print("[L383] row:{}".format(row))
            self.raw_data = self.raw_data.append(row)
            # self.raw_data.loc[ts_code]=[name,base_close,last_chg,df]
        # print("[L383] self.raw_data:{}".format(self.raw_data))
        self.raw_data.sort_values(by='last_chg', inplace=True, ascending=False)
        result = self.raw_data[['name', 'last_chg']]
        print(result)
        file_name = str('资产竞速{}周期涨幅比较_{}'.format(period, al_file[3:-4])) + '_' + today_str() + '.csv'
        file_path = sub_path_result + r'\Plot_Assets_Racing' + '\\' + file_name
        result.to_csv(file_path, encoding="utf-8")
        log_args = [file_path]
        add_log(40, '[fn]:Plot_Assets_Racing() result saved to file "{}"', log_args)
        for ts_code, pen in self.raw_data.iterrows():
            name, last_chg, df = pen['name'], pen['last_chg'], pen['df']
            last_chg = str(round(last_chg, 2))
            label = last_chg + '%  ' + name  # + '\n' + ts_code
            ax.plot(df.index, df['base_chg_pct'], label=label, lw=1)
        plt.legend(handles=ax.lines)
        plt.grid(True)
        mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体
        mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题
        plt.xticks(df.index, rotation='vertical')
        plt.title('资产竞速{}周期涨幅比较 - {}'.format(period, al_file[3:-4]))
        plt.ylabel('收盘%')
        plt.subplots_adjust(left=0.03, bottom=0.11, right=0.85, top=0.97, wspace=0, hspace=0)
        plt.legend(bbox_to_anchor=(1, 1), bbox_transform=plt.gcf().transFigure)
        plt.show()


class Strategy:
    """
    量化策略
    """

    def __init__(self, desc='stg#01', log_trans=None):
        """
        desc: <str> strategy description
        log_trans: True or None, True to log the self.trans_logs for diagnostic
        """
        # print('L1160 to be continued')
        self.desc = desc
        self.pools = {}  # dict of pools {execute order: pool #1, ...}
        self.by_date = None  # <str> 如'20191231' 当前处理周期的时间
        self.completed_cycle = None  # # <str>当by_date的日期计算循环完成后，将by_date日期赋给此参数；两参数非None值相同代表该周期结束
        self.trans_logs = []  # pool流转记录<list> of <Trans_Log>
        self.log_trans = log_trans  # 是否将asset流转记录到self.trans_logs供问题诊断
        self.ref_assets = {}  # 参考资产

    def add_pool(self, **kwargs):
        """
        add the pool to the strategy
        """

        def _get_next_order():
            k_max = 0
            for k in self.pools.keys():
                k_max = max(k, k_max)
            if k_max is None:
                result = 10
            else:
                result = (k_max // 10 + 1) * 10
            return result

        next_order = _get_next_order()
        par_strategy = weakref.ref(self)
        self.pools[next_order] = Pool(par_strategy=par_strategy(), **kwargs)

    def chg_pool_order(self, org_order, new_order):
        """
        change the execution order of the pool
        org_order: <int> original order
        new_order: <int> new order to set
        """
        if new_order in self.pools:
            log_args = [new_order]
            add_log(20, '[fn]Strategy.chg_pool_order(). new_order:{0[0]} was in the dict already. Order not changed',
                    log_args)
        else:
            try:
                pool = self.pools[org_order]
                del self.pools[org_order]
                self.pools[new_order] = pool
            except KeyError:
                log_args = [org_order]
                add_log(20, '[fn]Strategy.chg_pool_order(). org_order:{0[0]} was not exist. Order not changed',
                        log_args)

    def pools_brief(self):
        """
        printout the brief of pools
        """
        print("----Strategy: <{}> pools brief:----".format(self.desc))
        print("Order: Description")
        for k, v in sorted(self.pools.items()):
            # print("key: ",k,"    desc: ", v.desc)
            print("{:>5}: {:<50}".format(k, v.desc))
        print("Number of pools: {}".format(len(self.pools)))

    def update_cycles(self, start_date=None, end_date=None, cycles=None):
        """
        执行strategy计算的循环
        start_date: <str> e.g. '20191231', None的话不适用
        end_date:  <str> e.g. '20191231', None的话不适用
        cycles: <int> 执行的周期数，如果非None则end_date无效
        """
        global raw_data

        def _cycle():
            """
            遍历所有pools,执行1次筛选循环
            """
            for pl_i, pool in self.pools.items():
                tsf_list = pool.cycle(self.by_date)
                if tsf_list is not None:
                    self.trans_assets(out_pool_index=pl_i, transfer_list=tsf_list, trade_date=self.by_date)
            self.completed_cycle = self.by_date
            return True

        def _check_end_cycles():
            nonlocal cycles
            if end_date is None:
                if cycles is None:  # 就执行1次_cycle()结束
                    return_ok = _cycle()
                    if return_ok is True:
                        return True
                    else:
                        add_log(20, '[fn]Strategy.update_cycles._check_end_cycles(). _cycle() return error [L1476], aborted')
                        return
                else:  # cycles有<int>次数
                    try:
                        cycles = int(cycles)
                    except Exception as e:
                        log_args = [cycles, e.__class__.__name__]
                        add_log(10, '[fn]Strategy.update_cycles._check_end_cycles(). cycles:{0[0]}, except_type:{0[1]}, aborted', log_args)
                        return
                    if cycles < 1:
                        log_args = [cycles]
                        add_log(10, '[fn]Strategy.update_cycles._check_end_cycles(). cycles:{0[0]} < 1, aborted', log_args)
                        return
                    return_ok = None
                    for _ in range(cycles):  # 执行cycles次_cycle()
                        return_ok = _cycle()
                        if return_ok is not True:  # _cycle()出错跳出
                            log_args = [self.by_date]
                            add_log(20, '[fn]Strategy.update_cycles._check_end_cycles(). _cycle() return error on {0[0]}  [L1494], aborted', log_args)
                            return
                        next_trade_day = raw_data.next_trade_day(self.by_date)
                        if next_trade_day is None:
                            log_args = [self.by_date]
                            add_log(30, '[fn]Strategy.update_cycles._check_end_cycles() no more trade days after by_date:{0[0]}, stopped here', log_args)
                            return
                        else:
                            log_args = [self.by_date, next_trade_day]
                            add_log(40, '[fn]Strategy.update_cycles._check_end_cycles() by_date:{0[0]} changed to {0[1]}', log_args)
                            self.by_date = next_trade_day
                    return return_ok
            else:  # end_date is not None
                # 检查end_date格式有效性
                if valid_date_str_fmt(end_date) is not True:
                    log_args = [end_date]
                    add_log(10, '[fn]Strategy.update_cycles._check_end_cycles() end_date:{0[0]} invalid, aborted', log_args)
                    return
                # 检查end_date是否逆流
                if int(end_date) < int(self.by_date):
                    log_args = [end_date, self.by_date]
                    add_log(10, '[fn]Strategy.update_cycles._check_end_cycles() end_date:{0[0]} before by_date:{0[1]}, aborted', log_args)
                    return
                # pptx黄框中，end_date内的每cycles次循环后进行aggregate()处理
                if cycles is None:  # 不管cycles，一次做到end_date结束
                    return_ok = None
                    while int(self.by_date) < int(end_date):
                        return_ok = _cycle()
                        if return_ok is not True:  # _cycle()出错跳出
                            log_args = [self.by_date]
                            add_log(20, '[fn]Strategy.update_cycles._check_end_cycles(). _cycle() return error on {0[0]}  [L1524], aborted', log_args)
                            return
                        next_trade_day = raw_data.next_trade_day(self.by_date)
                        if next_trade_day is None:
                            log_args = [self.by_date]
                            add_log(30, '[fn]Strategy.update_cycles._check_end_cycles() no more trade days after by_date:{0[0]}, stopped here', log_args)
                            return
                        else:
                            log_args = [self.by_date, next_trade_day]
                            add_log(40, '[fn]Strategy.update_cycles._check_end_cycles() by_date:{0[0]} changed to {0[1]}', log_args)
                            self.by_date = next_trade_day
                    return return_ok
                else:  # cycles有<int>次数
                    try:
                        cycles = int(cycles)
                    except Exception as e:
                        log_args = [cycles, e.__class__.__name__]
                        add_log(10, '[fn]Strategy.update_cycles._check_end_cycles(). cycles:{0[0]}, except_type:{0[1]}, aborted', log_args)
                        return
                    if cycles < 1:
                        log_args = [cycles]
                        add_log(10, '[fn]Strategy.update_cycles._check_end_cycles(). cycles:{0[0]} < 1, aborted', log_args)
                        return
                    # 外层循环条件，by_date <= end_date; 内层每cycles次做aggregate;外层条件触发，终止内层循环，直接跳出
                    return_ok = None
                    while True:
                        for _ in range(cycles):
                            return_ok = _cycle()
                            if return_ok is not True:  # _cycle()出错跳出
                                log_args = [self.by_date]
                                add_log(20, '[fn]Strategy.update_cycles._check_end_cycles(). _cycle() return error on {0[0]} [L1554], aborted', log_args)
                                return
                            next_trade_day = raw_data.next_trade_day(self.by_date)
                            if next_trade_day is None:
                                log_args = [self.by_date]
                                add_log(30, '[fn]Strategy.update_cycles._check_end_cycles() no more trade days after by_date:{0[0]}, stopped here', log_args)
                                break  # 特殊：会跳出while
                            elif int(next_trade_day) > int(end_date):  # 外层终止条件
                                break  # 特殊：会跳出while
                            else:  # by_date往后推1交易日
                                log_args = [self.by_date, next_trade_day]
                                add_log(40, '[fn]Strategy.update_cycles._check_end_cycles() by_date:{0[0]} changed to {0[1]}', log_args)
                                self.by_date = next_trade_day
                        else:  # 如果for执行中没有break
                            continue  # continue while next round
                        break  # break while out
                    return return_ok

        def _check_completed_cycle():
            if self.completed_cycle is None:
                return_ok = _check_end_cycles()
                if return_ok is True:
                    return True
                else:
                    return
            elif int(self.completed_cycle) < int(self.by_date):
                return_ok = _check_end_cycles()
                if return_ok is True:
                    return True
                else:
                    return
            elif self.completed_cycle == self.by_date:
                log_args = [self.by_date]
                add_log(40, '[fn]Strategy.update_cycles._check_completed_cycle() {0[0]} executed before, skip to next trade day', log_args)
                next_trade_day = raw_data.next_trade_day(self.by_date)
                if next_trade_day is None:
                    add_log(10, '[fn]Strategy.update_cycles._check_completed_cycle() by_date:{0[0]} failed to get next trade day, aborted', log_args)
                    return
                else:
                    log_args = [self.by_date, next_trade_day]
                    add_log(40, '[fn]Strategy.update_cycles._check_completed_cycle() by_date:{0[0]} is not a trade day, changed to {0[1]}', log_args)
                    self.by_date = next_trade_day
                    return_ok = _check_end_cycles()
                    if return_ok is True:
                        return True
                    else:
                        return
            elif int(self.completed_cycle) > int(self.by_date):
                log_args = [self.completed_cycle, self.by_date]
                add_log(10, '[fn]Strategy.update_cycles._check_completed_cycle() completed_cycle:{0[0]} > by_date:{0[1]}, aborted', log_args)
                return
            else:
                log_args = [self.completed_cycle]
                add_log(10, '[fn]Strategy.update_cycles._check_completed_cycle() completed_cycle:{0[0]} unknown problem', log_args)
                return

        return_ok = None  # 内部函数返回成功标志
        if start_date is None:
            if self.by_date is None:
                add_log(10, '[fn]Strategy.update_cycles(). Both start_date and by_date are not specified. Aborted')
                return
            else:
                if raw_data.valid_trade_date(self.by_date):
                    return_ok = _check_completed_cycle()
                else:
                    next_trade_day = raw_data.next_trade_day(self.by_date)
                    if next_trade_day is None:
                        log_args = [self.by_date]
                        add_log(10, '[fn]Strategy.update_cycles.() by_date:{0[0]} failed to get next trade day, aborted', log_args)
                        return
                    else:
                        log_args = [self.by_date, next_trade_day]
                        add_log(40, '[fn]Strategy.update_cycles.() by_date:{0[0]} is not a trade day, changed to {0[1]}', log_args)
                        self.by_date = next_trade_day
                        return_ok = _check_completed_cycle()
        else:  # start_date is not None
            if valid_date_str_fmt(start_date) is not True:
                log_args = [start_date]
                add_log(10, '[fn]Strategy.update_cycles.() start_date:{0[0]} invalid', log_args)
                return
            # start_data非交易日处理
            if raw_data.valid_trade_date(start_date) is not True:
                next_trade_day = raw_data.next_trade_day(start_date)
                if next_trade_day is None:
                    log_args = [start_date]
                    add_log(10, '[fn]Strategy.update_cycles.() start_date:{0[0]} failed to get next trade day, aborted', log_args)
                    return
                else:
                    log_args = [start_date, next_trade_day]
                    add_log(40, '[fn]Strategy.update_cycles.() start_date:{0[0]} is not a trade day, changed to {0[1]}', log_args)
                    start_date = next_trade_day
            # start_date早于by_data处理
            if self.by_date is not None:
                if int(start_date) < int(self.by_date):
                    log_args = [start_date, self.by_date]
                    add_log(10, '[fn]Strategy.update_cycles.() start_date:{0[0]} before by_date:{0[1]}, aborted', log_args)
                    return
            if self.by_date != start_date:  # 有变化才增加日志
                log_args = [self.by_date, start_date]
                add_log(40, '[fn]Strategy.update_cycles.() by_date:{0[0]} changed to {0[1]}', log_args)
                self.by_date = start_date
                return_ok = _check_completed_cycle()
        # 收尾update
        if return_ok is not True:
            return  # _check_completed_cycle()执行报错，外层同样return
        print('[L1715] aggregate 收尾未完成')

    def trans_assets(self, out_pool_index, transfer_list, trade_date):
        """
        将assets从一个out_pool划转到一个或多个in_pool
        从源pool，及它的cnds_matrix中删除条目
        source_pool_index: <int> 源头pool在self.pools中的index
        transfer_list: <list> e.g. [(in_pool_index, <list> of ts_code),
                                    (in_pool_index, <list> of ts_code),...]
        in_price_mode, out_price_mode: <str> refer Asset.get_price() attr: mode
        in_shift_days, out_shift_days: <int> refer Asset.get_price() attr: shift_days
        """
        for transfer in transfer_list:
            in_pool_index = transfer[0]
            al = transfer[1]  # <list> of ts_code
            out_price_mode = transfer[2]
            in_price_mode = transfer[3]
            out_shift_days = transfer[4]
            in_shift_days = transfer[5]
            for ts_code in al:  # 给in_pool添加assets
                _rslt = self.trans_asset_down(ts_code=ts_code, trade_date=trade_date, out_pool_index=out_pool_index, in_pool_index=in_pool_index, out_price_mode=out_price_mode, in_price_mode=in_price_mode, out_shift_days=out_shift_days, in_shift_days=in_shift_days)
                if _rslt == 'duplicated':
                    continue
                elif _rslt is True:
                    log_args = [ts_code, in_pool_index]
                    add_log(40, '[fn]Strategy.trans_assets.() {0[0]} added to pool#{0[1]}', log_args)
                    continue
                else:  # None
                    continue  # 在trans_asset_down()中已有报错

            # 从out_pool中删除assets
            out_pool = self.pools[out_pool_index]
            if out_pool.del_trsfed is True:
                for ts_code in al:  # 给out_pool删除assets
                    out_pool.del_asset(ts_code)
                    out_pool.op_cnds_matrix(mode='d', ts_code=ts_code)

    def trans_asset_down(self, ts_code, trade_date, out_pool_index, in_pool_index, out_price_mode=None, in_price_mode='close', out_shift_days=0, in_shift_days=0, volume=None):
        """
        将单个asset加载到下游in_pool,但不删除原out_pool的asset（因为1个循环同1资产可能转去多个pool)； 给self.trans_logs添加1条记录； 给out_pool.
        手续税费等还未考虑
        ----------      --------
        |out_pool|---->|in_pool|
        ---------      --------
        ts_code: <str> e.g. '000001.SZ'
        trade_date: <str> e.g. ‘20191231’
        out_pool_index: <int> 源头pool的index
                        'al' 从al文件导入，此[fn]不适用
        in_pool_index: <int> 目的pool的index
                       'discard' 无下游pool，只从out_pool中移除asset
        out_price_mode: <str> 详见Asset.get_price()
                        None 根据in的情况在设out_price和out_date
        in_price_mode: <str> 详见Asset.get_price()
        in_shift_days, out_shift_days: <int> refer Asset.get_price() attr: shift_days
        volume: <int> 成交股数
                <None> 不适用

        return: True  success
                None  failed
        """
        from st_common import CND_SPC_TYPES
        # get in_pool
        in_pool = None
        if in_pool_index != 'discard':
            try:
                in_pool = self.pools[in_pool_index]
            except KeyError:
                log_args = [in_pool_index]
                add_log(20, '[fn]Strategy.trans_asset_down() in_pool_index:{0[0]} invalid', log_args)
                return

        try:  # get out_pool
            out_pool = self.pools[out_pool_index]
        except KeyError:
            log_args = [out_pool_index]
            add_log(20, '[fn]Strategy.trans_asset_down() out_pool_index:{0[0]} invalid', log_args)
            return

        try:  # get asset
            asset = out_pool.assets[ts_code]
        except KeyError:
            log_args = [ts_code, out_pool_index, out_pool.desc]
            add_log(20, '[fn]Strategy.trans_asset_down() {0[0]} was not found in pool_{0[1]}:{0[2]}', log_args)
            return

        # 处理in_price 和 in_date
        _rslt = asset.get_price(trade_date=trade_date, mode=in_price_mode, shift_days=in_shift_days)  # 资产的交割应该都在交易日，所以get_price的seek_direction默认放None，不搜索
        if _rslt is None:
            log_args = [ts_code, trade_date, in_price_mode]
            add_log(20, '[fn]Strategy.trans_asset_down() {0[0]} in_price (mode: {0[2]}) not available on {0[1]}, aborted', log_args)
            return  # 未找到价格
        in_price, in_date = _rslt

        # 处理out_price 和 out_date
        if out_price_mode is None:  # 根据in_price, in_date来
            out_price, out_date = in_price, in_date
        else:
            _rslt = asset.get_price(trade_date=trade_date, mode=out_price_mode, shift_days=out_shift_days)  # 资产的交割应该都在交易日，所以get_price的seek_direction默认放None，不搜索
            if _rslt is None:
                add_log(20, '[fn]Strategy.trans_asset_down() out_price not available, aborted')
                return  # 未找到价格
            out_price, out_date = _rslt
        asset.out_price = out_price
        asset.out_date = out_date

        # 给下游in_pool及cnds_matrix添加asset
        if in_pool is not None:  # 非discard的情况
            asset = in_pool.add_asset(ts_code=ts_code, in_date=in_date, in_price=in_price)
            if asset == 'duplicated':
                return 'duplicated'
            elif asset is not None:
                for cond in in_pool.conditions:
                    if cond.para1.idt_name not in CND_SPC_TYPES:  # 跳过condition的常量para
                        post_args1 = cond.para1.idt_init_dict
                        asset.add_indicator(**post_args1)
                    if cond.para2.idt_name not in CND_SPC_TYPES:  # 跳过condition的常量para
                        post_args2 = cond.para2.idt_init_dict
                        asset.add_indicator(**post_args2)
                in_pool.op_cnds_matrix(mode='a', ts_code=ts_code)  # 为cnds_matrix增加条目
            else:  # 添加失败
                log_args = [in_pool.desc, ts_code, in_pool_index]
                add_log(20, '[fn]Strategy.trans_asset_down() failed to add {0[1]} to pool_{0[2]}:{0[0]}', log_args)
                return

        # 增加trans_log和in_out
        if self.log_trans is True:
            self.trans_logs.append(Trans_Log(ts_code=ts_code, out_pool_index=out_pool_index, out_price=out_price, out_date=out_date, in_pool_index=in_pool_index, in_price=in_price, in_date=in_date, volume=volume))
        asset_out_pool = out_pool.assets[ts_code]
        if out_pool.log_in_out is True:
            out_pool.append_in_out(asset=asset_out_pool, in_pool_index=in_pool_index)
        return True

    def init_pools_cnds_matrix(self):
        """
        遍历调用pool.iter_al()给所有asset添加指标
        遍历调用pool.op_cnds_matrix()初始化各pool的条件矩阵
        在所有的cnds都加载完后调用
        """
        for pool in self.pools.values():
            pool.iter_al()
            pool.op_cnds_matrix(mode='i')

    def init_ref_assets(self):
        """
        为self.ref_assets添加资产
        """
        from st_common import CND_SPC_TYPES  # 特殊的非Indicator类Condition

        def _init_asset():
            category = All_Assets_List.query_category_str(ts_code)
            # 根据category不同，实例化对应的Asset
            if category is None:
                log_args = [ts_code]
                add_log(30, '[fn]Strategy.init_ref_assets(). ts_code:{0[0]} category is None, skip', log_args)
                return
            elif category == 'stock':
                if ts_code in self.ref_assets:
                    log_args = [ts_code]
                    add_log(40, '[fn]Strategy.init_ref_assets(). ts_code:{0[0]} already in the ref_assets, skip', log_args)
                else:
                    self.ref_assets[ts_code] = Stock(ts_code=ts_code)
                    log_args = [ts_code]
                    add_log(40, '[fn]Strategy.init_ref_assets(). ts_code:{0[0]} added', log_args)
            # ----other categories are to be implemented here-----
            else:
                print('[L1931] other categories are to be implemented')
            # 给assets添加indicator
            asset = self.ref_assets[ts_code]
            if cnd.para1.idt_name not in CND_SPC_TYPES:
                post_args1 = cnd.para1.idt_init_dict
                asset.add_indicator(**post_args1)
            if cnd.para2.idt_name not in CND_SPC_TYPES:
                post_args2 = cnd.para2.idt_init_dict
                asset.add_indicator(**post_args2)

        for pool in self.pools.values():
            for cnd in pool.conditions:
                if cnd.para1.specific_asset is not None:
                    ts_code = cnd.para1.specific_asset
                    _init_asset()
                if cnd.para2.specific_asset is not None:
                    ts_code = cnd.para2.specific_asset
                    _init_asset()


class Pool:
    """
    股票池
    """
    def __init__(self, par_strategy, desc="", al_file=None, in_date=None, in_price_mode='close', price_seek_direction=None, del_trsfed=True, log_in_out=None):
        r"""
        par_strategy: <weak ref> parent strategy
        desc: <str> 描述
        al_file: None = create empty dict;
                 <str> = path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
        in_date: asset进入pool的日期
                 None: 不提供
                 'latest': 根据asset基础数据里取有价格的最新那个时间
                 '20191231': 指定的日期
        del_trsfed: 当资产通过filter转到下游in_pool后，是否删除源out_pool中的资产
                    True or None
        log_in_out: True or None, True把asset在pool中的对应进出记录到self.io_out中
        """
        global raw_data
        self.desc = desc
        self.assets = {}  # {ts_code, <ins> Asset}
        self.in_out = None  # <df>资产进出该pool的对应记录
        self.log_in_out = log_in_out  # 是否记录资产的对应进出供分析
        self.init_in_out()
        self.in_date = in_date  # 仅做诊断用
        self.init_assets(al_file=al_file, in_date=in_date, in_price_mode=in_price_mode, price_seek_direction=price_seek_direction)
        self.conditions = []
        self.filters = []
        self.db_buff = Register_Buffer()  # dashboard buffer area
        self.dashboard = Dashboard(self.db_buff)
        self.cnds_matrix = None  # <DataFrame> index:'ts_code'; data: (True, False, numpy.nan...) 在所有cnds都初始化完后，使用[fn]op_cnds_matrix()初始化
        self.by_date = None  # <str> 如'20191231' 当前处理周期的时间
        self.completed_cycle = None  # <str>当by_date的日期计算循环完成后，将by_date日期赋给此参数；两参数非None值相同代表该周期结束
        self.del_trsfed = del_trsfed
        # if valid_date_str_fmt(in_date):
        #     self.by_date = raw_data.next_trade_day(in_date)  # None的话无效
        self.par_strategy = weakref.ref(par_strategy)  # <weak ref> parent strategy

    def init_assets(self, al_file=None, in_date=None, in_price_mode='close', price_seek_direction=None):
        r"""
        初始化self.assets，可选赋值in_date和in_price
        al_file: None = create empty dict;
                 <str> = path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
        in_date: asset进入pool的日期
                 None: 不提供
                 'latest': 根据asset基础数据里取有价格的最新那个时间
                 '20191231': 指定的日期
        in_price_mode: 'close', 'high', 'low'等，详见Asset.get_price()
        price_seek_direction: <str> 当价格数据不在daily_data中，比如停牌是，向前或后搜索数据
                               None: 返回None,不搜索
                               'forwards': 向时间增加的方向搜索
                              'backwards': 向时间倒退的方向搜索
        """
        if al_file is None:
            self.assets = {}
            return
        df_al = All_Assets_List.load_al_file(al_file)
        # print('[L1218] {}'.format(df_al))
        for index, row in df_al.iterrows():
            # print('[L1220] index:{} ;selected:{}'.format(index, row['selected']))
            # print('[L1221]', repr(row['selected']))
            selected = row['selected'].strip().upper()
            if selected == 'T':
                ts_code = index
                category = All_Assets_List.query_category_str(ts_code)
                # 根据category不同，实例化对应的Asset
                if category is None:
                    log_args = [ts_code]
                    add_log(30, '[fn]Pool.init_assets(). ts_code:{0[0]} category is None, skip', log_args)
                    continue
                elif category == 'stock':
                    if ts_code in self.assets:
                        log_args = [ts_code]
                        add_log(30, '[fn]Pool.init_assets(). ts_code:{0[0]} already in the assets, skip', log_args)
                    else:
                        self.assets[ts_code] = Stock(ts_code=ts_code, in_date=in_date, in_price_mode=in_price_mode, price_seek_direction=price_seek_direction)
                        log_args = [ts_code]
                        add_log(40, '[fn]Pool.init_assets(). ts_code:{0[0]} added', log_args)
                # ----other categories are to be implemented here-----
                else:
                    print('[L1228] other categories are to be implemented')
            else:
                log_args = [index]
                add_log(40, '[fn]Pool.init_assets() {0[0]} selected is not "T", skipped', log_args)

    def add_asset(self, ts_code, in_date=None, in_price=None):
        """
        给pool添加一个asset,初始化资产的daily_data和在pool中的指标
        ts_code: <str>
        in_date: None or <str> e.g. '20191231'，如给定则需要是交易日
        in_price: None or <float> 必须在in_date同时给值才有效，如果给定数值，则直接使用，不再查询历史价格
        return: asset, asset added
                ‘duplicated', asset exists already
                None, failed
        """
        global raw_data
        category = All_Assets_List.query_category_str(ts_code)
        # 根据category不同，实例化对应的Asset
        if category is None:
            log_args = [ts_code]
            add_log(20, '[fn]Pool.add_asset(). ts_code:{0[0]} category is None, aborted', log_args)
            return
        if ts_code in self.assets:
            log_args = [ts_code, self.desc]
            add_log(30, '[fn]Pool.add_asset(). ts_code:{0[0]} already in pool:{0[1]}, aborted', log_args)
            return 'duplicated'
        if in_date is not None:
            if raw_data.valid_trade_date(in_date) is not True:
                log_args = [ts_code, in_date]
                add_log(20, '[fn]Pool.add_asset(). {0[0]} in_date:{0[1]} is not a trade day, aborted', log_args)
                return

        if category == 'stock':
            asset = Stock(ts_code=ts_code, in_date=in_date, in_price=in_price)
            self.assets[ts_code] = asset
            log_args = [ts_code, self.desc]
            add_log(40, '[fn]Pool.add_asset(). ts_code:{0[0]} added to pool:{0[1]}', log_args)
            return asset
        # ----other categories are to be implemented here-----
        else:
            print('[L1228] other categories are to be implemented')

    def assets_brief(self):
        """
        显示pool资产概表
        """
        print('Pool:{} assets brief:'.format(self.desc))
        head = ('ts_code', 'by_price', 'earn_pct', 'earn', 'stay_days', 'by_date', 'in_date', 'in_price')
        # print head
        formats = []
        for name in head:
            fmt = FORMAT_HEAD[name]
            fmt_record = FORMAT_FIELDS[name]
            formats.append(fmt_record)
            print(fmt.format(name), end='')
        print()
        # print records
        num = len(formats)
        for asset in self.assets.values():
            for i in range(num):
                zd = getattr(asset, head[i])  # 用于显示的字段赋值
                if zd is None:
                    print('{:^14}'.format('None'), end='')
                else:
                    print(formats[i].format(zd), end='')
            print()
        print("Total Assets: {}".format(len(self.assets)))

    def init_in_out(self):
        """
        初始化self.in_out
        """
        self.in_out = pd.DataFrame(columns=['ts_code', 'earn_pct', 'earn', 'in_date', 'out_date', 'stay_days', 'in_price', 'out_price', 'in_pool_index'])

    def in_out_agg(self):
        """
        显示pool.in_out的统计信息
        """
        io_df = self.in_out
        print('pool:{} in_out aggregate'.format(self.desc))
        Analysis.in_out_agg(io_df)

    def append_in_out(self, asset, in_pool_index='None'):
        """
        将完成一组进出pool操作的asset的当前状态，记录到pool.in_out的新条目中
        此处出pool的意思是传到下游in_pool就算，不必须从out_pool中删掉asset
        注意调用此函数前更新好asset的相关参数
        asset: <Asset>
        return: True  success
                None  failed
        """
        if isinstance(asset, Asset):
            record = {'ts_code': asset.ts_code,
                      'earn_pct': asset.earn_pct,
                      'earn': asset.earn,
                      'in_date': asset.in_date,
                      'out_date': asset.out_date,
                      'stay_days': asset.stay_days,
                      'high_in_pool': asset.high_in_pool,
                      'high_pct': asset.high_pct,
                      'low_in_pool': asset.low_in_pool,
                      'low_pct': asset.low_pct,
                      'in_price': asset.in_price,
                      'out_price': asset.out_price,
                      'in_pool_index': in_pool_index}
            self.in_out = self.in_out.append(record, ignore_index=True)
            return True
        else:
            log_args = [type(asset)]
            add_log(20, '[fn]Pool.append_in_out(). asset type: {0[0]} is not Asset', log_args)
            return

    def csv_in_out(self, csv=None):
        """
        导出pool.in_out记录到csv
        csv: None  默认文件名 io_<date_of_generate>_<pool_desc>.csv
             <str> io_<str>.csv
        """
        if csv is None:  # 默认名
            name = today_str() + '_' + self.desc + '_' + now_time_str()
        file_name = 'io_' + name + '.csv'
        file_path = sub_path + sub_analysis + '\\' + file_name
        if isinstance(self.in_out, pd.DataFrame):
            self.in_out.to_csv(file_path, encoding="utf-8")
            log_args = [file_path]
            add_log(40, '[fn]:Pool.csv_in_out() {0[0]} exported', log_args)
        else:
            log_args = [self.desc, type(self.in_out)]
            add_log(10, '[fn]:Pool.csv_in_out() pool:{0[0]} in_out type:{0[0]} is not <df>', log_args)

    def add_condition(self, pre_args1, pre_args2, ops, required_period=0):
        """
        add the condition to the pool
        pre_argsN: <dict> refer indicator.idt_name() pre_args 创建para的必要输入参数
        e.g.
        {'idt_type': 'macd',
         'long_n1': 26,
         'short_n2': 12,
         'dea_n3': 9,
         'field': 'DEA'  # 在idt结果为多列，选取非默认列时需要填
         'source': 'close',
         'subtype': 'w',
         'update_csv': False,  # 指标文件结果是否保存到csv文件
         'reload': False  # 功能待查看代码
         'bias': 0.05  # 偏置量
         'specific_asset': '000001.SZ'  # 特定资产的数据作为条件
         }
        ops: <str> e.g. '>', '<=', '='...
        required_period: <int> 需要保持多少个周期来达成条件
        """
        self.conditions.append(Condition(pre_args1=pre_args1, pre_args2=pre_args2, ops=ops, required_period=required_period))

    def conditions_brief(self):
        """
        print the brief of conditions
        """
        print('No.             Condition Description')
        for i in range(len(self.conditions)):
            print('{:>3}    {:<32}'.format(i, self.conditions[i].desc))

    def add_filter(self, cnd_indexes=None, down_pools=None, out_price_mode=None, in_price_mode='close', out_shift_days=0, in_shift_days=0):
        """
        add the filter to the pool
        cnd_indexes: <set> {0, 1, 2}
        down_pools: <set> {0, 1}
        in_price_mode, out_price_mode: <str> refer Asset.get_price() attr: mode
        in_shift_days, out_shift_days: <int> refer Asset.get_price() attr: shift_days
        """
        if cnd_indexes is None:
            cnd_indexes = set()
        if down_pools is None:
            down_pools = set()
        self.filters.append(Filter(cnd_indexes, down_pools, out_price_mode=out_price_mode, in_price_mode=in_price_mode, out_shift_days=out_shift_days, in_shift_days=in_shift_days))

    def iter_al(self):
        """
        dual iterate the pool.assets and pool.conditions, add indicators to each asset
        在给每个资产添加指标时，指标的值会根据已下载的基础数据计算补完到可能的最新值；但不会触发基础数据的补完下载
        """
        from st_common import CND_SPC_TYPES  # 特殊的非Indicator类Condition
        for asset in self.assets.values():
            for cond in self.conditions:
                if cond.para1.idt_name not in CND_SPC_TYPES:
                    post_args1 = cond.para1.idt_init_dict
                    asset.add_indicator(**post_args1)
                if cond.para2.idt_name not in CND_SPC_TYPES:
                    post_args2 = cond.para2.idt_init_dict
                    asset.add_indicator(**post_args2)

    def op_cnds_matrix(self, mode='i', ts_code=None, al=None, **kwargs):
        """
        self.cnds_matrix相关的操作
        mode: 'i' = initialize根据当前assets及cnds初始化
              'a' = append增加ts_code行，旧记录不变
              'd' = delete删除ts_code行，其它记录不变
        ts_code: <str> 用于 a 或 d 模式，比al参数优先级高
        al: <list> of ts_code,用于 a 或 d 模式
        """
        if mode == 'i':
            n_cnds = len(self.conditions)
            if n_cnds > 0:
                head_list = list('cond' + str(i) for i in range(n_cnds))
                head_list.insert(0, 'ts_code')
                # print('[L1386] head_list: {}'.format(head_list))
                self.cnds_matrix = pd.DataFrame(columns=head_list)
                self.cnds_matrix.set_index('ts_code', inplace=True)
                data_nan = list(np.nan for _ in range(n_cnds))
                # print('[L1394] data_nan:'.format(data_nan))
                for ts_code in self.assets.keys():
                    self.cnds_matrix.loc[ts_code] = data_nan
                # print('[L1396] cnds_matrix:\n{}'.format(self.cnds_matrix))
            else:
                log_args = [self.desc]
                add_log(30, '[fn]Pool.op_cnds_matrix(). {0[0]} conditions not loaded', log_args)
                return

        elif mode == 'a':  # append
            if self.cnds_matrix is None:
                log_args = [self.desc]
                add_log(30, '[fn]Pool.op_cnds_matrix(). cnds in pool:{0[0]} is None, append aborted', log_args)  # 可能此pool没有加载任何conditions
                return
            else:  # self.cnds_matrix is <df>
                n_cnds = len(self.conditions)
                data_nan = list(np.nan for _ in range(n_cnds))
                if isinstance(ts_code, str):
                    try:  # 是否已存在
                        self.cnds_matrix.index.get_loc(ts_code)
                    except KeyError:
                        self.cnds_matrix.loc[ts_code] = data_nan
                        return
                    log_args = [ts_code, self.desc]
                    add_log(30, '[fn]Pool.op_cnds_matrix(). {0[0]} already in cnds_matrix of pool:{0[0]}, append skipped', log_args)
                elif isinstance(al, list):
                    if len(al) > 0:
                        for ts_code in al:
                            try:  # 是否已存在
                                self.cnds_matrix.index.get_loc(ts_code)
                            except KeyError:
                                self.cnds_matrix.loc[ts_code] = data_nan
                                continue
                            log_args = [ts_code, self.desc]
                            add_log(30, '[fn]Pool.op_cnds_matrix(). {0[0]} already in cnds_matrix of pool:{0[0]}, append skipped', log_args)
                            continue
                    else:
                        log_args = [al]
                        add_log(20, '[fn]Pool.op_cnds_matrix(). no ts_code in al:{0[0]}, append aborted', log_args)
                        return
                else:  # ts_code, al在'a','d'模式都没给定
                    add_log(20, '[fn]Pool.op_cnds_matrix(). both ts_code and al are not specified, append aborted')
                    return

        elif mode == 'd':  # delete
            if isinstance(ts_code, str):
                try:
                    self.cnds_matrix.drop(ts_code, inplace=True)
                except KeyError:
                    log_args = [ts_code, self.desc]
                    add_log(30, '[fn]Pool.op_cnds_matrix(). failed to delete {0[0]} from pool:{0[1]}', log_args)  # 可能已被删除
            elif isinstance(al, list):
                if len(al) > 0:
                    for ts_code in al:
                        try:
                            self.cnds_matrix.drop(ts_code, inplace=True)
                        except KeyError:
                            log_args = [ts_code, self.desc]
                            add_log(30, '[fn]Pool.op_cnds_matrix(). failed to delete {0[0]} from pool:{0[1]}', log_args)  # 可能已被删除
                            continue
                else:
                    log_args = [al]
                    add_log(20, '[fn]Pool.op_cnds_matrix(). no ts_code in al:{0[0]}, del aborted', log_args)  # 可能已被删除
                    return
            else:  # ts_code, al在'a','d'模式都没给定
                add_log(20, '[fn]Pool.op_cnds_matrix(). both ts_code and al are not specified, del aborted')
                return

    def filter_cnd(self, cnd_index, datetime_='latest', csv=None, al=None, update_matrix=None):
        """
        filter the self.assets or al with the condition
        本函数不会发起基础数据的下载和或指标的重新计算
        cnd_index: <int>, self.conditions 的序号
        datetime_: <str> 'latest' or like '20190723' YYYYMMDD
        csv: None or 'default' or <str> al_'file_name'
             'default' = <pool_desc>_output.csv
        al: 输入资产列表 None=self.assets.values(); <list> of ts_code
        update_matrix: 是否更新self.cnds_matrix, True=更新
        return: <list> 成立ts_code列表
        """
        try:
            cnd = self.conditions[cnd_index]  # <Condition>, 过滤的条件
        except IndexError:
            log_args = [cnd_index]
            add_log(20, '[fn]Pool.filter_cnd(). invalid cnd_index:{0[0]}', log_args)
            return
        if datetime_ == 'latest':
            # -----para1
            if cnd.para1.shift_periods is None:
                shift1 = 0  # 前后移动周期数
            elif cnd.para1.shift_periods > 0:
                add_log(20, '[fn]Pool.filter_cnd(). can not shift forward is datetime is latest')
                return
            else:  # 负值，取前值
                shift1 = cnd.para1.shift_periods
            val_fetcher1 = lambda df, column_name: df.iloc[0 - shift1][column_name]  # [fn] 获取最新idt记录值
            date_fetcher1 = lambda df: str(df.index[0])  # [fn] 用以获取当前资产idt的最新记录时间
            # -----para2
            if cnd.para2.shift_periods is None:
                shift2 = 0  # 前后移动周期数
            elif cnd.para2.shift_periods > 0:
                add_log(20, '[fn]Pool.filter_cnd(). can not shift forward is datetime is latest')
                return
            else:  # 负值，取前值
                shift2 = cnd.para2.shift_periods
            val_fetcher2 = lambda df, column_name: df.iloc[0 - shift2][column_name]  # [fn] 获取最新idt记录值
            date_fetcher2 = lambda df: str(df.index[0])  # [fn] 用以获取当前资产idt的最新记录时间
        elif valid_date_str_fmt(datetime_):
            dt_int = int(datetime_)
            # -----para1
            if cnd.para1.shift_periods is None:
                shift1 = 0  # 前后移动周期数
            else:
                shift1 = cnd.para1.shift_periods
            val_fetcher1 = lambda df, column_name: df.iloc[df.index.get_loc(dt_int) - shift1][column_name]
            date_fetcher1 = lambda _: datetime_
            # -----para2
            if cnd.para2.shift_periods is None:
                shift2 = 0  # 前后移动周期数
            else:
                shift2 = cnd.para2.shift_periods
            val_fetcher2 = lambda df, column_name: df.iloc[df.index.get_loc(dt_int) - shift2][column_name]
            date_fetcher2 = lambda _: datetime_
        else:
            log_args = [datetime_]
            add_log(10, '[fn]Pool.filter_cnd(). datetime_:{0[0]} invalid', log_args)
            return

        if isinstance(cnd, Condition):
            self.dashboard.board_head = ('ts_code', 'cond_desc', 'cond_p1_value', 'cond_p2_value', 'cond_p1_date', 'cond_p2_date')
            out_list = []  # 存放filter出的ts_code列表
            if al is None:
                al_list = self.assets.values()
            elif isinstance(al, list):
                al_list = (self.assets[ts_code] for ts_code in al)
                # print('[L1388] al_list:{}'.format(al_list))
            else:
                add_log(20, '[fn]Pool.filter_cnd(). Invalid al')
                return

            for asset in al_list:
                # print("[L1383] ts_code: {}".format(asset.ts_code))
                # -------------刷新Pool.db_buff---------------------
                self.db_buff.ts_code = asset.ts_code

                # -------------para1---------------------
                idt_name1 = cnd.para1.idt_name
                if idt_name1 == 'const':
                    idt_value1 = cnd.para1.const_value  # para1 value
                    idt_date1 = 'const'  # 常量的特殊时间
                elif idt_name1 == 'stay_days':
                    idt_value1 = asset.stay_days
                    if idt_value1 is None:
                        idt_date1 = 'None'
                        idt_value1 = 0
                    else:
                        idt_date1 = asset.by_date
                else:  # 普通Indicator类condition
                    if cnd.para1.specific_asset is not None:  # specific asset指标
                        _asset = self.par_strategy().ref_assets[cnd.para1.specific_asset]  # 父strategy.ref_assets
                    else:
                        _asset = asset
                    try:  # 指标在资产中是否存在
                        idt1 = getattr(_asset, idt_name1)
                    except Exception as e:  # 待细化
                        log_args = [_asset.ts_code, e.__class__.__name__, e]
                        add_log(20, '[fn]Pool.filter_cnd(). ts_code:{0[0]}, except_type:{0[1]}; msg:{0[2]}', log_args)
                        continue
                    idt_df1 = idt1.df_idt
                    idt_field1 = cnd.para1.field
                    column_name1 = idt_field1.upper() if idt_field1 != 'default' else cnd.para1.idt_init_dict['idt_type'].upper()
                    # print('[L1316] column_name1: {}'.format(column_name1))
                    idt_date1 = date_fetcher1(idt_df1)
                    try:  # 获取目标时间的指标数值
                        idt_value1 = val_fetcher1(idt_df1, column_name1) + cnd.para1.bias  # <float>
                        # print('[L1407] idt_value1:{}'.format(idt_value1))
                    except (IndexError, KeyError):  # 指标当前datetime_无数据
                        log_args = [_asset.ts_code, cnd.para1.idt_name, idt_date1]
                        add_log(30, '[fn]Pool.filter_cnd(). {0[0]}, {0[1]}, data unavailable:{0[2]} skip', log_args)
                        continue

                # print("[L1405] idt_date1: {}".format(idt_date1))
                # print('[L1406] idt_value1: {}'.format(idt_value1))

                # -------------para2---------------------
                idt_name2 = cnd.para2.idt_name
                if idt_name2 == 'const':
                    idt_value2 = cnd.para2.const_value
                    idt_date2 = 'const'  # 常量的特殊时间
                elif idt_name2 == 'stay_days':
                    idt_value2 = asset.stay_days
                    if idt_value2 is None:
                        idt_date2 = 'None'
                        idt_value2 = 0
                    else:
                        idt_date2 = asset.by_date
                else:  # 普通Indicator类condition
                    if cnd.para2.specific_asset is not None:  # specific asset指标
                        _asset = self.par_strategy.ref_assets[cnd.para2.specific_asset]  # 父strategy.ref_assets
                    else:
                        _asset = asset
                    try:  # 指标在资产中是否存在
                        idt2 = getattr(_asset, idt_name2)
                    except Exception as e:  # 待细化
                        log_args = [_asset.ts_code, e.__class__.__name__, e]
                        add_log(20, '[fn]Pool.filter_cnd(). ts_code:{0[0], except_type:{0[1]}; msg:{0[2]}', log_args)
                        continue
                    idt_df2 = idt2.df_idt
                    idt_field2 = cnd.para2.field
                    column_name2 = idt_field2.upper() if idt_field2 != 'default' else cnd.para2.idt_init_dict['idt_type'].upper()
                    # print('[L1316] column_name2: {}'.format(column_name2))
                    idt_date2 = date_fetcher2(idt_df2)
                    try:
                        idt_value2 = val_fetcher2(idt_df2, column_name2) + cnd.para2.bias  # <float>
                        # print('[L1436] idt_value2:{}'.format(idt_value2))
                    except (IndexError, KeyError):  # 指标当前datetime_无数据
                        log_args = [_asset.ts_code, cnd.para2.idt_name, idt_date2]
                        add_log(30, '[fn]Pool.filter_cnd(). {0[0]}, {0[1]}, data unavailable:{0[2]} skip', log_args)
                        continue
                # print("[L1427] idt_date2: {}".format(idt_date2))
                # print('[L1428] idt_value2: {}'.format(idt_value2))

                # -------------调用Condition.calcer()处理---------------------
                if idt_date1 == "const" or idt_date2 == "const" or idt_date1 == idt_date2:
                    fl_result = cnd.calcer(idt_value1, idt_value2)  # condition结果
                    # print('[L1410] fl_result:{}'.format(fl_result))
                else:
                    log_args = [asset.ts_code, idt_date1, idt_date2]
                    add_log(20, '[fn]Pool.filter_cnd(). ts_code:{0[0], condition parameters timestamp mismatch, p1:{0[1]}; p2:{0[2]}', log_args)
                    continue

                # -------------刷新Pool.db_buff---------------------
                self.db_buff.cond_desc = cnd.desc
                self.db_buff.cond_result = fl_result
                self.db_buff.cond_p1_value = idt_value1
                self.db_buff.cond_p2_value = idt_value2
                self.db_buff.cond_p1_date = idt_date1
                self.db_buff.cond_p2_date = idt_date2
                # -------------append True record to dashboard and out_list---------------------
                # print('[L1459] fl_result type:{}'.format(type(fl_result)))
                # print('[L1460] fl_result:{}'.format(fl_result))
                if bool(fl_result) is True:  # bool()因为fl_result类型为numpi.bool
                    # print('[L1425] fl_result:{}'.format(fl_result))
                    cnd.increase_lasted(asset.ts_code)
                    if cnd.exam_lasted(asset.ts_code):  # 达到周期
                        out_list.append(asset.ts_code)
                        self.dashboard.append_record()
                        if update_matrix is True:
                            self.cnds_matrix.loc[asset.ts_code][cnd_index] = True
                    else:  # 未达到周期要求
                        if update_matrix is True:
                            self.cnds_matrix.loc[asset.ts_code][cnd_index] = False
                else:
                    cnd.reset_lasted(asset.ts_code)
                    if update_matrix is True:
                        self.cnds_matrix.loc[asset.ts_code][cnd_index] = False

            if isinstance(csv, str) and (len(out_list) > 0):
                if csv == 'default':
                    csv_file_name = self.desc + '_output'
                else:
                    csv_file_name = csv
                All_Assets_List.create_al_file(out_list, csv_file_name)

            return out_list  # <list> of ts_code

        else:  # 不是Condition
            log_args = [type(cnd)]
            add_log(10, '[fn]Pool.filter_cnd(). cnd type:{0[0]} is not <Condition>', log_args)
            return

    def filter_filter(self, filter_index, datetime_='latest', csv='default', al=None):
        """
        filter the self.assets or al with the <ins Filter>，本函数不会发起基础数据的下载和或指标的重新计算
        确保self.cnds_matrix已初始化
        filter_index: <int>, 过滤器的标号
        datetime_: <str> 'latest' or like '20190723' YYYYMMDD
        csv: None or 'default' or <str> al_'file_name'
             default = <pool_desc>_output.csv
        al: 输入资产列表 None=self.assets.values(); <list> of ts_code
        return: <list> 成立ts_code列表
        """
        try:
            flt = self.filters[filter_index]  # <Filter> 条件集合过滤器
        except IndexError:
            log_args = [filter_index]
            add_log(10, '[fn]Pool.filter_filter(). invalid filter_index:{0[0]}', log_args)
            return

        cnd_names = []
        for i in flt.cnd_indexes:  # i为pool.conditions中cnd的标号
            self.filter_cnd(cnd_index=i, datetime_=datetime_, al=al, update_matrix=True)
            cnd_names.append('cond' + str(i))

        exec_str = 'self.cnds_matrix['
        for cnd_name in cnd_names:
            exec_str = exec_str + '(self.cnds_matrix["' + cnd_name + '"] == True) & '
        exec_str = 'self.db_buff.filter_output_al = list(' + exec_str[:-3] + '].index)'
        # print('[L1583] exec_str: {}'.format(exec_str))
        exec(exec_str)
        out_al = self.db_buff.filter_output_al  # <list> of 'ts_code'
        # print('[L1585] out_al:\n{}'.format(out_al))

        if isinstance(csv, str) and (len(out_al) > 0):
            if csv == 'default':
                csv_file_name = self.desc + '_output'
            else:
                csv_file_name = csv
            All_Assets_List.create_al_file(out_al, csv_file_name)

        log_args = [len(out_al)]
        add_log(30, '[fn]Pool.filter_filter() output {} assets', log_args)
        return out_al

    def cycle(self, date_str):
        """
        由strategy调用执行pool计算的1次循环，
            补全资产的in_price，in_date如果之前为None的话
            更新by_price, by_date, stay_days
            按序遍历所有的filter
        返回<list> of the assets to be transferred
        资产的pools间transfer由strategy完成
        不触发aggregate()

        date_str: <str> e.g. '20191231'
        return: <list> e.g. [(in_pool_index, <list> of ts_code, <str> out_price_mode, <str> in_price_mode, <int> out_shift_days, <int> in_shift_days), ...]
                None 没有资产需要transfer
        """
        global raw_data
        # 更新pool.by_date
        if valid_date_str_fmt(date_str) is not True:
            log_args = [self.desc, date_str]
            add_log(10, '[fn]Pool.cycle() {0[0]} date_str:{0[1]} invalid', log_args)
            return
        if self.by_date is None:
            self.by_date = date_str
        elif int(self.by_date) > int(date_str):
            log_args = [self.desc, date_str, self.by_date]
            add_log(10, '[fn]Pool.cycle() {0[0]} date_str:{0[1]} before by_date:{0[2]}, aborted', log_args)
            return
        elif int(self.by_date) < int(date_str):
            self.by_date = date_str

        # 遍历assets, 补缺in_price, in_date
        lack_in_price = [asset for asset in self.assets.values() if asset.in_price is None]
        for asset in lack_in_price:
            rslt = asset.get_price(trade_date=date_str)  # 永远当日收盘价
            if rslt is not None:
                asset.in_price, asset.in_date = rslt

        # 遍历assets, 更新by_date, by_price, stay_days
        for asset in self.assets.values():
            rslt = asset.get_price(self.by_date)  # 永远当日收盘价
            if rslt is not None:  # 不停牌有收盘价
                _price, _ = rslt
                if asset.by_date is None:
                    asset.by_date = self.by_date
                    asset.by_price = _price
                    asset.earn = asset.by_price - asset.in_price
                    asset.earn_pct = (asset.earn / asset.in_price)
                elif int(asset.by_date) > int(self.by_date):
                    log_args = [asset.ts_code, asset.by_date, self.by_date]
                    add_log(20, '[fn]Pool.cycle() {0[0]} by_date:{0[1]} after {0[2]}, skipped', log_args)
                    continue  # skip this asset
                else:
                    asset.by_date = self.by_date
                    asset.by_price = _price
                    asset.earn = asset.by_price - asset.in_price
                    asset.earn_pct = (asset.earn / asset.in_price)

            # 更新stay_days
            if asset.in_date is not None:
                asset.stay_days = raw_data.len_trade_days(int(asset.in_date), int(self.by_date))

                # 更新high_in_pool, low_in_pool
                rslt = asset.get_price(self.by_date, mode='high')
                if rslt is not None:  # 不停牌有收盘价
                    _high, _ = rslt
                    if asset.high_in_pool is None:
                        asset.high_in_pool = _high
                        asset.high_pct = (_high - asset.in_price) / asset.in_price
                    elif asset.high_in_pool < _high:
                        asset.high_in_pool = _high
                        asset.high_pct = (_high - asset.in_price) / asset.in_price
                rslt = asset.get_price(self.by_date, mode='low')  # 永远当日收盘价
                if rslt is not None:  # 不停牌有收盘价
                    _low, _ = rslt
                    if asset.low_in_pool is None:
                        asset.low_in_pool = _low
                        asset.low_pct = (_low - asset.in_price) / asset.in_price
                    elif asset.low_in_pool > _low:
                        asset.low_in_pool = _low
                        asset.low_pct = (_low - asset.in_price) / asset.in_price

        # 依次遍历所有filters
        rslt_to_return = []  # 返回的结果
        nf = len(self.filters)
        for i in range(nf):
            filter_ = self.filters[i]
            rslt_assets = self.filter_filter(filter_index=i, datetime_=date_str, csv=None)  # csv=None is OK
            if rslt_assets is not None:
                if len(rslt_assets) > 0:
                    for index in filter_.down_pools:
                        rslt_item = (index, rslt_assets, filter_.out_price_mode, filter_.in_price_mode, filter_.out_shift_days, filter_.in_shift_days)
                        rslt_to_return.append(rslt_item)

        if len(rslt_to_return) > 0:
            return rslt_to_return
        else:
            return

    def del_asset(self, ts_code):
        """
        delete the asset from pool.assets
        return: True success
                None failed
        """
        try:
            del self.assets[ts_code]
        except KeyError:
            log_args = [ts_code, self.desc]
            add_log(30, '[fn]Pool.del_asset() {0[0]} was not found in pool:{0[1]}', log_args)
            return
        return True


class Trans_Log:
    """
    asset在pool之间流转的记录
    ----------      --------
    |out_pool|---->|in_pool|
    ---------      --------
    ts_code: <str> e.g. '000001.SZ'
    out_pool_index: <int> 源头pool的index
                    'al' 从al文件导入
    out_price: <float> 进入下游pool的价格
    out_date: <str> 进入下游pool的日期 e.g. ‘20191231’
    in_pool_index: <int> 目的pool的index
                   'discard' 无下游pool，只从out_pool中移除asset
    in_price: <float> 进入下游pool的价格
    in_date: <str> 进入下游pool的日期 e.g. ‘20191231’
    volume: <int> 成交股数
            <None> 不适用
    """
    def __init__(self, ts_code, out_pool_index, out_price, out_date, in_pool_index, in_price, in_date, volume=None):
        self.ts_code = ts_code
        self.out_pool_index = out_pool_index
        self.out_price = out_price
        self.out_date = out_date
        self.in_pool_index = in_pool_index
        self.in_price = in_price
        self.in_date = in_date
        self.volume = volume


class Aggregate:
    """
    Pool的聚合信息
    """
    def __init__(self):
        self.num_assets = None  # 资产个数
        self.complete_num = None  # 信息完整的资产个数
        self.incomplete_num = None  # 信息不完整的资产个数
        self.earn_average_pct = None  # 平均收益%

    def update(self):
        """
        刷新聚合信息，并更新pool每个asset的[by_date][stay_days][by_capital][earn][earn_pct]
        """
        print('[L1799] to be continued')


class Condition:
    """
    判断条件
    """

    def __init__(self, pre_args1, pre_args2, ops, required_period=0):
        """
        pre_argsN: <dict> refer idt_name() pre_args
        ops: <str> e.g. '>', '<=', '='...
        required_period: <int> 条件需要持续成立的周期
        """
        self.para1 = Para(pre_args1)
        self.para2 = Para(pre_args2)
        self.calcer = None  # <fn> calculator
        # self.result = None  # 要用? True of False, condition result of result_time
        # self.result_time = None  # 要用? <str> e.g. '20191209'
        self.desc = None  # <str> description 由后面程序修改
        self.required_period = required_period  # <int> 条件需要持续成立的周期
        self.true_lasted = {}  # <dict> {'000001.SZ': 2,...} 资产持续成立的周期

        '''
        pX_name的形式
        <str> idt_name:对应已定义的Indicator
        "const": 常量
        "stayed_days": asset在pool中停留的天数，如果不可用，则value=0, date="None"
        '''
        p1_name = self.para1.idt_name
        p2_name = self.para2.idt_name

        if ops == '>':
            self.calcer = lambda p1, p2: p1 > p2
            self.desc = p1_name + ' > ' + p2_name
        elif ops == '<':
            self.calcer = lambda p1, p2: p1 < p2
            self.desc = p1_name + ' < ' + p2_name
        elif ops == '>=':
            self.calcer = lambda p1, p2: p1 >= p2
            self.desc = p1_name + ' >= ' + p2_name
        elif ops == '<=':
            self.calcer = lambda p1, p2: p1 <= p2
            self.desc = p1_name + ' <= ' + p2_name
        elif ops == '=':
            self.calcer = lambda p1, p2: p1 == p2
            self.desc = p1_name + ' = ' + p2_name

    def increase_lasted(self, ts_code):
        """
        将对应资产的true_lasted计数增加1
        ts_code: <str>
        """
        if isinstance(ts_code, str):
            if ts_code in self.true_lasted:
                self.true_lasted[ts_code] += 1
            else:
                self.true_lasted[ts_code] = 1
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Condition.increase_lasted(). ts_code:{0[0]} in not str', log_args)

    def reset_lasted(self, ts_code):
        """
        重置对应资产的true_lasted到0
        """
        if ts_code in self.true_lasted:
            self.true_lasted[ts_code] = 0

    def exam_lasted(self, ts_code):
        """
        检查对应资产的true_lasted是否 >= self.required_period
        """
        if ts_code in self.true_lasted:
            return self.true_lasted[ts_code] >= self.required_period
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Condition.exam_lasted(). ts_code:{0[0]} not in self.true_lasted', log_args)


class Para:
    """
    Parameter的缩写
    Condition中比较用的元参数
    """

    def __new__(cls, pre_args):
        """
        检验<dict>kwargs的有效性
        """
        from st_common import CND_SPC_TYPES
        from indicator import IDT_CLASS
        if 'idt_type' in pre_args:
            idt_type = pre_args['idt_type']
            if (idt_type in CND_SPC_TYPES) or (idt_type in IDT_CLASS):
                obj = super().__new__(cls)
                return obj
        log_args = [pre_args]
        add_log(10, '[fn]Para.__new__() kwargs "{0[0]}" invalid, instance not created', log_args)

    def __init__(self, pre_args):
        """
        pre_args: <dict> 传给idt_name()用于生成idt_name和<ins Indicator>
        idt_type: <str> in indicator.IDT_CLASS.keys, or
                        'const' 常量
                        'stay_days' 资产在pool中停留的交易日数
        field: <str> 指标结果列名
                     'default' 指标结果是单列的，使用此默认值
                     如'DEA' 指标结果是多列的，非与指标名同名的列，用大写指定
        shift_periods: <int> 取值的偏移量，-值时间向早，+值时间向晚
        specific_asset: <str> None 默认不起作用
                        'ts_code' 如 '000001.SZ' 取此资产的值
        bias: <float> 取值的偏置量，会加到结果上
        """
        from indicator import idt_name
        idt_type = pre_args['idt_type']
        if idt_type == 'const':
            self.idt_name = 'const'
            self.idt_type = 'const'
            self.const_value = pre_args['const_value']
        elif idt_type == 'stay_days':
            self.idt_name = 'stay_days'
            self.idt_type = 'stay_days'
        else:
            self.field = None  # <str> string of the indicator result csv column name
            if 'field' in pre_args:
                self.field = pre_args['field']
                del pre_args['field']
            else:
                self.field = 'default'
            self.idt_init_dict = idt_name(pre_args)
            self.idt_name = self.idt_init_dict['idt_name']
        if 'shift_periods' in pre_args:
            self.shift_periods = pre_args['shift_periods']
            del pre_args['shift_periods']
        else:
            self.shift_periods = None
        if 'specific_asset' in pre_args:
            self.specific_asset = pre_args['specific_asset']
            del pre_args['specific_asset']
        else:
            self.specific_asset = None
        if 'bias' in pre_args:
            self.bias = pre_args['bias']
            del pre_args['bias']
        else:
            self.bias = 0


class Filter:
    """
    Condition的集合，assets在pools间按过滤条件流转的通道
    """
    def __new__(cls, cnd_indexes=None, down_pools=None, out_price_mode=None, in_price_mode='close', in_shift_days=0, out_shift_days=0):
        if down_pools is None:
            down_pools = set()
        if cnd_indexes is None:
            cnd_indexes = set()
        if isinstance(cnd_indexes, set) and isinstance(down_pools, set):
            obj = super().__new__(cls)
            return obj
        else:
            log_args = [type(cnd_indexes), type(down_pools)]
            add_log(10, '[fn]Filter.__new__() cnd_indexes type:{0[0]}, down_pools type:{0[1]} are not <set>', log_args)

    def __init__(self, cnd_indexes=None, down_pools=None, out_price_mode=None, in_price_mode='close', in_shift_days=0, out_shift_days=0):
        """
        in_price_mode, out_price_mode: <str> refer Asset.get_price() attr: mode
        in_shift_days, out_shift_days: <int> refer Asset.get_price() attr: shift_days
        """
        if down_pools is None:
            down_pools = set()
        if cnd_indexes is None:
            cnd_indexes = set()
        self.cnd_indexes = cnd_indexes  # <set> contains indexes of pool.condition
        self.down_pools = down_pools  # <set> contains indexes of downstream <Pool>
        self.out_price_mode = out_price_mode
        self.in_price_mode = in_price_mode
        self.in_shift_days = in_shift_days
        self.out_shift_days = out_shift_days


class Register_Buffer:
    """
    公共临时数据缓存区，只保存最新赋予的值，供看板、transition等调用。
    注意：
    - 在调用前确保值是为此调用刷新的，避免错乱
    """
    def __init__(self):
        self.ts_code = None
        self.trade_date = None  # <str> e.g. "20200102"

        # Condition
        self.cond_desc = None  # Condition.desc
        self.cond_result = None  # True or False
        self.cond_p1_value = None  # condition para1 的值
        self.cond_p2_value = None  # condition para2 的值
        self.cond_p1_date = None  # p1时间戳e.g."20190102", "const"
        self.cond_p2_date = None  # p2时间戳e.g."20190102", "const"

        # Filter
        self.filter_output_al = None  # <list> of 'ts_code'


class Dashboard:
    """
    看板
    """
    def __init__(self, register_buffer):
        self.buff_weakref = weakref.ref(register_buffer)
        self.records = []  # dashboard <list>
        self.board_head = ()  # <tuple> e.g. ('ts_code', 'cond_desc', 'cond_result')

    def append_record(self):
        """
        append the record to the board. data from self.buff_weakref(), template as self.board_head
        """
        if not self.valid_board_head():
            return

        buff = self.buff_weakref()  # 未考虑buff被回收后的情况
        record = tuple(getattr(buff, item_name) for item_name in self.board_head)
        # print('[L1545] record={}'.format(record))
        self.records.append(record)

    def disp_board(self):
        """
        print out the contents of dashboard
        字段的显示格式在st_common.py FORMAT_HEAD 和 FORMAT_FIELDS中
        """
        if not self.valid_board_head():
            return

        # print head
        formats = []
        for name in self.board_head:
            fmt = FORMAT_HEAD[name]
            fmt_record = FORMAT_FIELDS[name]
            formats.append(fmt_record)
            print(fmt.format(name), end='')
        print()
        # print records
        num = len(formats)
        # print('[L1560] self.records:{}'.format(self.records))
        for record in self.records:
            # print('[L1561] record:{}'.format(record))
            for i in range(num):
                print(formats[i].format(record[i]), end='')
            print()
        print("Total Records: {}".format(len(self.records)))

    def valid_board_head(self):
        """
        valid board head
        """
        if not isinstance(self.board_head, tuple):
            log_args = [self.board_head]
            add_log(20, '[fn]Dashboard.append_record(). board_head invalid: "{0[0]}"', log_args)
            return
        if len(self.board_head) == 0:
            add_log(20, '[fn]Dashboard.append_record(). board_head empty')
            return
        return True

    def clear_board(self):
        """
        clear the content of the dashboard
        """
        self.records = []


if __name__ == "__main__":
    from st_common import Raw_Data
    global raw_data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)

    print('================Strategy测试================')
    stg = Strategy('stg_p1_01')
    stg.add_pool(desc='p10初始池', al_file='try_001', in_date=None, price_seek_direction=None, del_trsfed=None)
    # stg.add_pool(desc='p10初始池', al_file='HS300成分股', in_date=None, price_seek_direction=None, del_trsfed=None)
    p10 = stg.pools[10]
    stg.add_pool(desc='p20持仓', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    p20 = stg.pools[20]
    # stg.add_pool(desc='p30持仓10日', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p30 = stg.pools[30]
    # stg.add_pool(desc='p40持仓15日', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p40 = stg.pools[40]
    # stg.add_pool(desc='p50持仓20日', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p50 = stg.pools[50]
    # stg.add_pool(desc='p60持仓25日', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p60 = stg.pools[60]
    # stg.add_pool(desc='p70持仓30日', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p70 = stg.pools[70]
    # stg.add_pool(desc='p30持仓', al_file=None, in_date=None, price_seek_direction=None, log_in_out=True)
    # p30 = stg.pools[30]
    # stg.add_pool(desc='p40已卖出', al_file=None, in_date=None, price_seek_direction=None)
    # p40 = stg.pools[40]
    stg.pools_brief()  # 打印pool列表
    # ---pool10 conditions-----------
    # ------condition_0
    pre_args1 = {'idt_type': 'madq',
                 'period': 5,
                 'dq_n1': 1,
                 'shift_periods': -1}
    pre_args2 = {'idt_type': 'madq',
                 'period': 20,
                 'dq_n1': 1,
                 'shift_periods': -1,
                 'update_csv': True}
    p10.add_condition(pre_args1, pre_args2, '<')
    # ------condition_1
    pre_args1 = {'idt_type': 'madq',
                 'period': 5,
                 'dq_n1': 1}
    pre_args2 = {'idt_type': 'madq',
                 'period': 20,
                 'dq_n1': 1}
    p10.add_condition(pre_args1, pre_args2, '>=')
    # ------condition_2
    pre_args1 = {'idt_type': 'maqs',
                 'period': 20,
                 'specific_asset': '000333.SZ',
                 'bias': 0}
    pre_args2 = {'idt_type': 'const',
                 'const_value': -0.5}
    p10.add_condition(pre_args1, pre_args2, '<')

    p10.add_filter(cnd_indexes={0, 1, 2}, down_pools={20}, in_price_mode='open_sxd', in_shift_days=1)
    # ---pool20 conditions-----------
    # ------condition_0
    pre_args1 = {'idt_type': 'stay_days'}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 20}
    p20.add_condition(pre_args1, pre_args2, '>=')
    # # ------condition_1
    # pre_args1 = {'idt_type': 'maqs',
    #              'period': 60}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 0}
    # p20.add_condition(pre_args1, pre_args2, '>=')
    #
    p20.add_filter(cnd_indexes={0}, down_pools={'discard'})
    # # ---pool20 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 5}
    # p20.add_condition(pre_args1, pre_args2, '>=')
    #
    # p20.add_filter(cnd_indexes={0}, down_pools={'discard'})
    #
    # # ---pool30 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 10}
    # p30.add_condition(pre_args1, pre_args2, '>=')
    #
    # p30.add_filter(cnd_indexes={0}, down_pools={'discard'})
    #
    # # ---pool40 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 15}
    # p40.add_condition(pre_args1, pre_args2, '>=')
    #
    # p40.add_filter(cnd_indexes={0}, down_pools={'discard'})
    #
    # # ---pool50 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 20}
    # p50.add_condition(pre_args1, pre_args2, '>=')
    #
    # p50.add_filter(cnd_indexes={0}, down_pools={'discard'})
    #
    # # ---pool60 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 25}
    # p60.add_condition(pre_args1, pre_args2, '>=')
    #
    # p60.add_filter(cnd_indexes={0}, down_pools={'discard'})
    #
    # # ---pool70 conditions-----------
    # # ------condition_0
    # pre_args1 = {'idt_type': 'stay_days'}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 30}
    # p70.add_condition(pre_args1, pre_args2, '>=')
    #
    # p70.add_filter(cnd_indexes={0}, down_pools={'discard'})
    # ---初始化各pool.cnds_matrix, strategy.ref_assets-----------
    stg.init_pools_cnds_matrix()
    stg.init_ref_assets()

    # ---stg循环-----------
    # stg.update_cycles(start_date='20050101', end_date='20200101')
    # stg.update_cycles(start_date='20050201', end_date='20200101')
    stg.update_cycles(start_date='20180101', cycles=50)
    # ---报告-----------
    p20.csv_in_out()
    # p30.csv_in_out()
    # p40.csv_in_out()
    # p50.csv_in_out()
    # p60.csv_in_out()
    # p70.csv_in_out()

    print("后续测试：多周期重复; asset transmit")
    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
    pass
