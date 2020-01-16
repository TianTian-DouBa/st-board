import pandas as pd
import numpy as np
import os
import time
import weakref
from st_common import raw_data
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt
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
    source:<str> e.g. 'close_hfq' defined in SOURCE
    nrows: <int> 指定读入最近n个周期的记录,None=全部
    return:<df> trade_date(index); close; high...
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
            except FileNotFoundError:
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


class Asset:
    """
    资产的基类
    """

    def __init__(self, ts_code):
        self.ts_code = ts_code

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
            if isinstance(idt, Indicator):
                idt.calc_idt()
            else:
                log_args = [self.ts_code, idt_name_]
                add_log(20, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed.', log_args)
        except Exception as e:
            log_args = [self.ts_code, idt_name_, e.__class__.__name__]
            add_log(10, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed. Except:{0[2]}', log_args)
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
    """股票类的资产"""

    def __init__(self, ts_code):
        Asset.__init__(self, ts_code=ts_code)

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
    def load_stock_daily(ts_code, nrows=None):
        """
        从文件读入股票日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path, dtype={'trade_date': str},
                                 usecols=['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'pre_close',
                                          'change', 'pct_chg', 'vol', 'amount'], index_col='trade_date', nrows=nrows)
            result['vol'] = result['vol'].astype(np.int64)
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
    def load_stock_dfq(ts_code, nrows=None):
        """
        从文件读入后复权的股票日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'dfq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path, dtype={'trade_date': str},
                                 usecols=['trade_date', 'adj_factor', 'close', 'open', 'high', 'low'],
                                 index_col='trade_date', nrows=nrows)
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

    def __init__(self, name):
        """
        name: <str> strategy name
        """
        # print('L1160 to be continued')
        self.name = name
        self.pools = {}  # dict of pools {execute order: pool #1, ...}

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
        self.pools[next_order] = Pool(**kwargs)

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
        print("----Strategy: <{}> pools brief:----".format(self.name))
        print("Order: Description")
        for k, v in sorted(self.pools.items()):
            # print("key: ",k,"    desc: ", v.desc)
            print("{:>5}: {:<50}".format(k, v.desc))
        print("Number of pools: {}".format(len(self.pools)))


class Pool:
    """
    股票池
    """

    def __init__(self, desc="", al_file=None):
        r"""
        exc_order: execution order <int>
        al_file:None = create empty dict; <str> = path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
        """
        self.desc = desc
        self.assets = {}
        self.init_assets(al_file)
        self.conditions = []
        self.db_buff = Register_Buffer()  # dashboard buffer area
        self.dashboard = Dashboard(self.db_buff)

    def init_assets(self, al_file=None):
        r"""
        init self.assets
        al_file:None = create empty dict; <str> = path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
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
                        self.assets[ts_code] = Stock(ts_code)
                        log_args = [ts_code]
                        add_log(40, '[fn]Pool.init_assets(). ts_code:{0[0]} added', log_args)
                # ----other categories are to be implemented here-----
                else:
                    print('[L1228] other categories are to be implemented')
            else:
                print('[L1241] al selected is not "T"')

    def add_condition(self, pre_args1_, pre_args2_, ops, required_period=0):
        """
        add the condition to the pool
        pre_argsN_: <dict> refer idt_name() pre_args
        ops: <str> e.g. '>', '<=', '='...
        """
        self.conditions.append(Condition(pre_args1_=pre_args1_, pre_args2_=pre_args2_, ops=ops, required_period=required_period))

    def iter_al(self):
        """
        iterate the al list in the pool, according to the self.conditions to add indicators to each asset
        """
        for asset in self.assets.values():
            for cond in self.conditions:
                if cond.para1.idt_name != 'const':  # 跳过condition的常量para
                    post_args1 = cond.para1.idt_init_dict
                    asset.add_indicator(**post_args1)
                if cond.para2.idt_name != 'const':  # 跳过condition的常量para
                    post_args2 = cond.para2.idt_init_dict
                    asset.add_indicator(**post_args2)

    def filter_cnd(self, cnd, datetime_='latest', csv='default', al=None):
        """
        filter the self.assets or al with the condition
        cnd: <Condition>, 过滤的条件
        datetime_: <str> 'latest' or like '20190723' YYYYMMDD
        csv: None or 'default' or <str> al_'file_name'
             default = <pool_desc>_output.csv
        al: 输入资产列表 None=self.assets.values(); <list> of ts_code
        return: <list> 成立ts_code列表
        """
        if datetime_ == 'latest':
            val_fetcher = lambda df, column_name: df.iloc[0][column_name]  # [fn] 获取最新idt记录值
            date_fetcher = lambda df: str(df.index[0])  # [fn] 用以获取当前资产idt的最新记录时间
        elif valid_date_str_fmt(datetime_):
            dt_int = int(datetime_)
            val_fetcher = lambda df, column_name: df.iloc[df.index.get_loc(dt_int)][column_name]
            date_fetcher = lambda _: datetime_
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
                print('[L1388] al_list:{}'.format(al_list))
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
                else:
                    try:  # 指标在资产中是否存在
                        idt1 = getattr(asset, idt_name1)
                        idt_df1 = idt1.df_idt
                        idt_field1 = cnd.para1.field
                        column_name1 = idt_field1.upper() if idt_field1 != 'default' else cnd.para1.idt_init_dict[
                            'idt_type'].upper()
                        # print('[L1316] column_name1: {}'.format(column_name1))
                    except Exception as e:  # 待细化
                        log_args = [asset.ts_code, e.__class__.__name__, e]
                        add_log(20, '[fn]Pool.filter_cnd(). ts_code:{0[0]}, except_type:{0[1]}; msg:{0[2]}', log_args)
                        continue
                    idt_date1 = date_fetcher(idt_df1)
                    try:  # 获取目标时间的指标数值
                        idt_value1 = val_fetcher(idt_df1, column_name1)
                        # print('[L1407] idt_value1:{}'.format(idt_value1))
                    except KeyError:  # 指标当前datetime_无数据
                        log_args = [asset.ts_code, cnd.para1.idt_name, idt_date1]
                        add_log(30, '[fn]Pool.filter_cnd(). {0[0]}, {0[1]}, data unavailable:{0[2]} skip', log_args)
                        continue

                # print("[L1405] idt_date1: {}".format(idt_date1))
                # print('[L1406] idt_value1: {}'.format(idt_value1))

                # -------------para2---------------------
                idt_name2 = cnd.para2.idt_name
                if idt_name2 == 'const':
                    idt_value2 = cnd.para2.const_value
                    idt_date2 = 'const'  # 常量的特殊时间
                else:
                    try:
                        idt2 = getattr(asset, idt_name2)
                        idt_df2 = idt2.df_idt
                        idt_field2 = cnd.para2.field
                        column_name2 = idt_field2.upper() if idt_field2 != 'default' else cnd.para2.idt_init_dict[
                            'idt_type'].upper()
                        # print('[L1316] column_name2: {}'.format(column_name2))
                    except Exception as e:  # 待细化
                        log_args = [asset.ts_code, e.__class__.__name__, e]
                        add_log(20, '[fn]Pool.filter_cnd(). ts_code:{0[0], except_type:{0[1]}; msg:{0[2]}', log_args)
                        continue
                    idt_date2 = date_fetcher(idt_df2)
                    try:
                        idt_value2 = val_fetcher(idt_df2, column_name2)
                        # print('[L1436] idt_value2:{}'.format(idt_value2))
                    except KeyError:  # 指标当前datetime_无数据
                        log_args = [asset.ts_code, cnd.para2.idt_name, idt_date2]
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
                    if cnd.exam_lasted(asset.ts_code):
                        out_list.append(asset.ts_code)
                        self.dashboard.append_record()
                else:
                    cnd.reset_lasted(asset.ts_code)

            if isinstance(csv, str):
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

    def filter_flt(self, flt, datetime_='latest', csv='default', al=None):
        """
        filter the self.assets or al with the <ins Filter>
        flt: <Filter>, 过滤的条件
        datetime_: <str> 'latest' or like '20190723' YYYYMMDD
        csv: None or 'default' or <str> al_'file_name'
             default = <pool_desc>_output.csv
        al: 输入资产列表 None=self.assets.values(); <list> of ts_code
        return: <list> 成立ts_code列表
        """
        print('[L1507] to be continued')


class Condition:
    """
    判断条件
    """

    def __init__(self, pre_args1_, pre_args2_, ops, required_period=0):
        """
        pre_argsN_: <dict> refer idt_name() pre_args
        ops: <str> e.g. '>', '<=', '='...
        required_period: <int> 条件需要持续成立的周期
        """
        self.para1 = Para(pre_args1_)
        self.para2 = Para(pre_args2_)
        self.calcer = None  # <fn> calculator
        self.result = None  # True of False, condition result of result_time
        self.result_time = None  # <str> e.g. '20191209'
        self.desc = None  # <str> description e.g. TBD
        self.required_period = required_period  # <int> 条件需要持续成立的周期
        self.true_lasted = {}  # <dict> {'000001.SZ': 2,...} 资产持续成立的周期

        p1_name = self.para1.idt_name
        p2_name = self.para2.idt_name

        if ops == '>':
            self.calcer = lambda p1, p2: p1 > p2
            self.desc = p1_name + ' > ' + p2_name + ' ?'
        elif ops == '<':
            self.calcer = lambda p1, p2: p1 < p2
            self.desc = p1_name + ' < ' + p2_name + ' ?'
        elif ops == '>=':
            self.calcer = lambda p1, p2: p1 >= p2
            self.desc = p1_name + ' >= ' + p2_name + ' ?'
        elif ops == '<=':
            self.calcer = lambda p1, p2: p1 <= p2
            self.desc = p1_name + ' <= ' + p2_name + ' ?'
        elif ops == '=':
            self.calcer = lambda p1, p2: p1 == p2
            self.desc = p1_name + ' = ' + p2_name + ' ?'

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
        from indicator import IDT_CLASS
        if 'idt_type' in pre_args:
            idt_type = pre_args['idt_type']
            if (idt_type == 'const') or (idt_type in IDT_CLASS):
                obj = super().__new__(cls)
                return obj
        log_args = [pre_args]
        add_log(10, '[fn]Para.__new__() kwargs "{0[0]}" invalid, instance not created', log_args)

    def __init__(self, pre_args):
        """
        pre_args: <dict> 传给idt_name()用于生成idt_name和<ins Indicator>
        """
        from indicator import idt_name
        idt_type = pre_args['idt_type']
        if idt_type == 'const':
            self.idt_name = 'const'
            self.idt_type = 'const'
            self.const_value = pre_args['const_value']
        else:
            self.field = None  # <str> string of the indicator result csv column name
            if 'field' in pre_args:
                self.field = pre_args['field']
                del pre_args['field']
            else:
                self.field = 'default'
            self.idt_init_dict = idt_name(pre_args)
            self.idt_name = self.idt_init_dict['idt_name']


class Filter:
    """
    Condition的集合，assets在pools间按过滤条件流转的通道
    """
    def __init__(self):
        pass


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


class Dashboard:
    """
    看板
    """
    def __init__(self, register_buffer):
        self.buff_weakref = weakref.ref(register_buffer)
        self.list = []  # dashboard <list>
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
        self.list.append(record)

    def disp_board(self):
        """
        print out the contents of dashboard
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
        # print('[L1560] self.list:{}'.format(self.list))
        for record in self.list:
            # print('[L1561] record:{}'.format(record))
            for i in range(num):
                print(formats[i].format(record[i]), end='')
            print()
        print("Total Records: {}".format(len(self.list)))

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
        self.list = []


if __name__ == "__main__":
    from st_common import Raw_Data
    global raw_data
    start_time = datetime.now()
    # df = get_stock_list()
    # df = load_stock_list()
    # df = get_daily_basic()
    # cl = get_trade_calendar()
    # last_trad_day_str()
    raw_data = Raw_Data(pull=False)
    # c = raw_data.trade_calendar
    # index = raw_data.index
    # zs = que_index_daily(ts_code="000009.SH",start_date="20031231")
    # ttt = index.get_index_daily('399003.SZ',reload=False)
    # #------------------------批量下载数据-----------------------
    # download_path = r"download_all"
    # download_path = r"dl_stocks"
    # download_path = r"try_001"
    # download_path = r"user_001"
    # bulk_download(download_path, reload=False)  # 批量下载数据
    # download_path = r"dl_stocks"
    # bulk_dl_appendix(download_path, reload=True)  # 批量下载股票每日指标数据，及股票复权因子
    # ttt = ts_pro.index_daily(ts_code='801001.SI',start_date='20190601',end_date='20190731')
    # ttt = ts_pro.sw_daily(ts_code='950085.SH',start_date='20190601',end_date='20190731')
    # Plot.try_plot()
    # #------------------------资产赛跑-----------------------
    # num_samples=30
    # df = Index.load_sw_daily('801003.SI',num_samples)
    # df = df[['trade_date','close']]
    # df['trade_date'] = pd.to_datetime(df['trade_date'])
    # df.set_index('trade_date', inplace=True)
    # base_close, = df.tail(1)['close'].values
    # df['base_chg_pct']=(df['close']/base_close-1)*100
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # ax.plot(df.index,df['base_chg_pct'],label='801003.SI\n指数A')

    # df1 = Index.load_sw_daily('850341.SI',30)
    # df1 = df1[['trade_date','close']]
    # df1['trade_date'] = pd.to_datetime(df1['trade_date'])
    # df1.set_index('trade_date', inplace=True)
    # base_close, = df1.tail(1)['close'].values
    # df1['base_chg_pct']=(df1['close']/base_close-1)*100
    # ax.plot(df1.index,df1['base_chg_pct'],label='850341.SI\n指数B')
    # plt.legend(handles=ax.lines)
    # plt.grid(True)
    # mpl.rcParams['font.sans-serif'] = ['FangSong'] # 指定默认字体
    # mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
    # plt.xticks(df.index,rotation='vertical')
    # plt.title('申万板块指标{}日涨幅比较'.format(num_samples))
    # plt.ylabel('收盘%')
    # plt.legend(bbox_to_anchor=(1, 1),bbox_transform=plt.gcf().transFigure)
    # plt.show()
    # #------------------------下载申万三级行业分类-----------------------
    # df_l1,df_l2,df_l3 = Index.get_sw_index_classify()
    # al = All_Assets_List.load_all_assets_list()
    # All_Assets_List.rebuild_all_assets_list()
    # #------------------------生成al文件-----------------------
    # al_l1 = Plot_Utility.gen_al(al_name='SW_Index_L1',stype1='SW',stype2='L1')  # 申万一级行业指数
    # al_l2 = Plot_Utility.gen_al(al_name='SW_Index_L2',stype1='SW',stype2='L2')  # 申万二级行业指数
    # al_l3 = Plot_Utility.gen_al(al_name='SW_Index_L3',stype1='SW',stype2='L3')  # 申万二级行业指数
    # al_dl_stocks = Plot_Utility.gen_al(al_name='dl_stocks',selected=None,type_='stock')#全部valid='T'的资产
    # al_download = Plot_Utility.gen_al(al_name='download_all',selected=None)#全部valid='T'的资产
    # #-------------------Plot_Assets_Racing资产竞速-----------------------
    # plot_ar = Plot_Assets_Racing('al_SW_Index_L1.csv',period=5)
    # plot_ar = Plot_Assets_Racing('al_user_001.csv',period=5)
    # #-------------------Stock Class-----------------------
    # stock = Stock(pull=True)
    # stock = Stock()
    # -------------------复权测试-----------------------
    # ts_code = '000001.SZ'
    # df_fq = Stock.load_adj_factor(ts_code)
    # df_stock = Stock.load_stock_daily(ts_code)
    # def fq_cls(x):
    #     print('x={}'.format(x))
    #     idx_date = x.index
    #     factor = df_fq.loc[idx_date]['adj_factor']
    #     result = x['close']*factor/109.169
    #     return result
    # df_stock.loc[:,'fq_cls']=df_stock['close']
    # df_stock.loc[:,'factor']=df_fq['adj_factor']
    # for index, row in df_stock.iterrows():
    #     factor = row['factor']
    #     close = row['close']
    #     #print("factor={}, close={}".format(factor,close))
    #     fq_cls_ = close*factor/109.169
    #     df_stock.at[index,'fq_cls']=fq_cls_
    # df1 = df_stock[(df_stock.index >= 20190624) & (df_stock.index <= 20190627)]
    # print(df1[['close','fq_cls','factor']])
    # df2 = ts.pro_bar(ts_code='000001.SZ', adj='dfq', start_date='20190624', end_date='20190915')
    # df2.set_index('trade_date',inplace=True)
    # print(df2['close'])
    # print("----------------第二组-----------------------")
    # df1 = df_stock[(df_stock.index >= 19920501) & (df_stock.index <= 19920510)]
    # print(df1[['close','fq_cls','factor']])
    # df2 = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='19920501', end_date='20190915')
    # df2.set_index('trade_date',inplace=True)
    # print(df2['close'])
    # Stock.calc_dfq('600419.SH',reload=False)
    # al_file_str = r"dl_stocks"
    # al_file_str = r"try_001"
    # bulk_calc_dfq(al_file_str, reload=False)  # 批量计算复权
    # print("===================Indicator===================")
    # from indicator import idt_name, Indicator, Ma, Ema, Macd
    #
    # print('------stock1.ma10_close_hfq--------')
    # stock5 = Stock(ts_code='000001.SZ')
    # _kwargs = {'idt_type': 'ema',
    #            'period': 10}
    # kwargs = idt_name(_kwargs)
    # stock5.add_indicator(**kwargs)
    # stock5.ema_10.calc_idt()
    # ema10 = stock5.ema_10.df_idt
    # print(ema10)

    # print('------stock1.ema26_close_hfq--------')
    # stock5 = Stock(ts_code='000001.SZ')
    # _kwargs = {
    #           'idt_type': 'ema',
    #           'period': 26}
    # kwargs = idt_name(_kwargs)
    # print('[L1424] kwargs:', kwargs)
    # stock5.add_indicator(**kwargs)
    # ema26 = stock5.ema_26.df_idt
    # print(ema26)

    # print('------stock1.ema12_close_hfq--------')
    # kwargs = {'idt_name': 'ema12_close_hfq',
    #           'idt_class': Ema,
    #           'period': 12}
    # stock1.add_indicator(**kwargs)
    # stock1.ema12_close_hfq.calc_idt()
    # ema12 = stock1.ema12_close_hfq.df_idt
    # print(ema12)

    # print('------valid Indicator up to source date--------')
    # print('uptodate ma10:',stock1.valid_idt_utd('ma10_close_hfq'))
    # print('uptodate ema12:',stock1.valid_idt_utd('ema12_close_hfq'))
    # print('uptodate ema26:',stock1.valid_idt_utd('ema26_close_hfq'))

    # print('------MACD--------')
    # stock1 = Stock(ts_code='000002.SZ')
    # _kwargs = {'idt_type': 'macd',
    #            'long_n1': 26,
    #            'short_n2': 12,
    #            'dea_n3': 9}
    # kwargs = idt_name(_kwargs)
    # print('[L1446]---kwargs:',kwargs)
    # stock1.add_indicator(**kwargs)
    # macd = stock1.macd_26_12_9
    # print('[L1448]---')
    # if macd.valid_utd() != True:
    #     macd.calc_idt()

    # #ema12 = stock1.ema_12.df_idt
    # #ema26 = stock1.ema_26.df_idt

    # print("===================Strategy===================")
    # print('------Strategy and Pool--------')
    # stg = Strategy('test strategy')
    # stg.add_pool(desc="pool#1", al_file='try_001')
    # stg.add_pool(desc="pool#2")
    # stg.add_pool(desc="pool#3")
    # stg.pools_brief()
    # pool_10 = stg.pools[10]
    # st_002 = pool_10.assets['000002.SZ']
    # print(stg.pools[10].assets.keys())
    # print('------Add Conditions, scripts required for each strategy--------')
    # # condition_1
    # pre_args1 = {'idt_type': 'ma',
    #              'period': 20}
    # pre_args2 = {'idt_type': 'macd',
    #              'long_n1': 26,
    #              'short_n2': 12,
    #              'dea_n3': 9,
    #              'field': 'MACD'}
    # pool_10.add_condition(pre_args1, pre_args2, '<')
    #
    # # condition_2
    # pre_args1 = {'idt_type': 'ma',
    #              'period': 5}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 30}
    # pool_10.add_condition(pre_args1, pre_args2, '<')
    #
    # pre_args1 = {'idt_type': 'ema',
    #              'period': 10}
    # pre_args2 = {'idt_type': 'const',
    #              'const_value': 8}
    # pool_10.add_condition(pre_args1, pre_args2, '>=')

    # print('------iterate  --------')
    # 手动为asset添加指标
    # kwargs1 = pool_10.conditions[0].para1.idt_init_dict
    # st_002.add_indicator(**kwargs1)
    #
    # kwargs2 = pool_10.conditions[0].para2.idt_init_dict
    # st_002.add_indicator(**kwargs2)

    # 自动iterate pool.assets 来添加
    # pool_10.iter_al()
    #
    # print('------定时间点过滤目标股Pool.filter_al()--------')
    # cond = pool_10.conditions[0]
    # pool_10.filter_al(cond)
    # print('-----')
    # pool_10.filter_al(cond, '20191205')
    #
    # print('------Dashboard--------')
    # pool_10.dashboard.disp_board()
    #
    # print('------临时便利--------')
    # st01 = pool_10.assets['000001.SZ']

    print('===========Phase-1 单pool，条件筛选测试===========')
    # print('------Strategy and Pool--------')
    stg = Strategy('stg_p1_00')
    # stg.add_pool(desc="pool10", al_file='try_001')
    stg.add_pool(desc="pool10", al_file='pool_001')
    # stg.add_pool(desc="pool20")
    # stg.add_pool(desc="pool30")
    stg.pools_brief()
    pool10 = stg.pools[10]
    st002 = pool10.assets['000002.SZ']
    print('------Add Conditions, scripts required for each strategy--------')
    # ------condition_0
    pre_args1 = {'idt_type': 'majh',
                 'long_n1': 60,
                 'middle_n2': 20,
                 'short_n3': 5}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 1.0}
    pool10.add_condition(pre_args1, pre_args2, '<', 3)
    #
    # ------condition_1
    pre_args1 = {'idt_type': 'ma',
                 'period': 20}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 5}
    pool10.add_condition(pre_args1, pre_args2, '<')
    # 自动iterate pool.assets 来添加
    pool10.iter_al()
    cond0 = pool10.conditions[0]
    cond1 = pool10.conditions[1]
    print('++++ 001 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20191226')
    pool10.dashboard.disp_board()
    print('++++ 002 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20191227')
    pool10.dashboard.disp_board()
    print('++++ 003 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20191230')
    pool10.dashboard.disp_board()
    print('++++ 004 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20191231')
    pool10.dashboard.disp_board()
    print('++++ 005 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20200102')
    pool10.dashboard.disp_board()
    print('++++ 006 ++++')
    pool10.dashboard.clear_board()
    pool10.filter_cnd(cond0, '20200103')
    pool10.dashboard.disp_board()
    # print('------test multi-stages filter--------')
    # stg.add_pool(desc="pool20", al_file='pool10_output')
    # pool20 = stg.pools[20]
    # stg.pools_brief()
    # pre_args1 = {'idt_type': 'ma',
    #              'period': 20}
    # pre_args2 = {'idt_type': 'ma',
    #              'period': 60}
    # pool20.add_condition(pre_args1, pre_args2, '>')
    # pool20.iter_al()
    # cond0 = pool20.conditions[0]
    # pool20.filter_al(cond0)
    # pool20.dashboard.disp_board()
    print('++++ xxx ++++')
    pool10.dashboard.clear_board()
    al = ['000001.SZ', '000002.SZ', '000010.SZ', '000011.SZ']
    pool10.filter_cnd(cnd=cond0, al=al)
    pool10.dashboard.disp_board()

    print("后续测试：cond成立周期;filter过滤; asset transmit")
    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
    pass
