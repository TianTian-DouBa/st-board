import tushare as ts
import pandas as pd
import numpy as np
import os
import time
import weakref
from st_common import raw_data
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
import matplotlib.pyplot as plt
from pylab import mpl
from pandas.plotting import register_matplotlib_converters

ts_pro = ts.pro_api()
register_matplotlib_converters() #否则Warning

def valid_date_str_fmt(date_str):
    """
    验证date_str形式格式是否正确
    date_str:<str> e.g. '20190723' YYYYMMDD
    return:<bool> True=valid
    """
    if isinstance(date_str,str):
        if len(date_str) == 8:
            return True
    return

def today_str():
    """
    return: <str> today in 'YYYYMMDD' e.g.'20190712'
    """
    dt = datetime.now()
    today_str = dt.strftime("%Y%m%d")
    return today_str

def valid_file_path(file_path):
    r"""
    检验file_path的形式有效性
    file_path:<str> e.g. '.\data_csv\daily_data\xxx.csv'
    return: Ture if valid, None if invalid
    """
    if isinstance(file_path,str):
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
    date = datetime.strptime(date_str,"%Y%m%d")
    return date

def download_data(ts_code,category,reload=False):
    r"""
    通过调用sgmt_daily_index_download下载资产数据到数据文e.g. daily_data\d_<ts_code>.csv文件
    需要在增加资产类别时改写
    ts_code: <str> '399001.SZ'
    category: <str> in READER e.g. 'Index.load_index_daily'
    reload: <bool> True=重头开始下载
    retrun: <df> if success, None if fail
    """
    global raw_data
    try:
        loader = LOADER[category]
        que_limit = QUE_LIMIT[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]download_data(). ts_code: "{0[0]}" category: "{0[1]}" incorrect',log_args)
        return
    def _category_to_file_name(ts_code,category):
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
        #----------其它类型时修改------------
        else:
            log_args = [category]
            add_log(20, '[fn]download_data(). invalid category: {0[0]}', log_args)
            return
        return file_name
    
    def _category_to_start_date_str(ts_code,category):
        """
        ts_code: <str> '399001.SZ'
        category: <str> e.g. 'adj_factor'
        return: <str> e.g. "20190402"
        """
        global raw_data
        #-----------index类别--------------
        if category == 'index_sw' or category == 'index_sse' or category == 'index_szse':
            start_date_str = str(raw_data.index.que_base_date(ts_code))
        #-----------stock类别--------------
        elif category == 'stock':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        #-----------stock每日指标类别--------------
        elif category == 'stock_daily_basic':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        #-----------复权因子--------------
        elif category == 'adj_factor':
            start_date_str = str(raw_data.stock.que_list_date(ts_code))
        #-----------其它类型(未完成)--------------
        else:
            log_args = [ts_code,category]
            add_log(20, '[fn]download_data() {0[0]} category:{0[1]} invalid', log_args)
            return
        return start_date_str

    if raw_data.valid_ts_code(ts_code):
        if reload == True: #重头开始下载
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
            start_date_str = _category_to_start_date_str(ts_code,category)
            if start_date_str == None:
                return
            end_date_str = today_str()
            df = sgmt_download(ts_code,start_date_str,end_date_str,que_limit,category)
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
                file_name = _category_to_file_name(ts_code,category)
                if file_name == None:
                    return
                df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                if logable(40):
                    number_of_items = len(df)
                    log_args = [ts_code, category, file_name, number_of_items]
                    add_log(40,"[fn]download_data() ts_code:{0[0]}, category:{0[1]}, file:{0[2]}, total items:{0[3]}", log_args)
                return df
            else:
                log_args = [ts_code,df]
                add_log(20, '[fn]download_data() fail to get DataFrame from Tushare. ts_code: "{0[0]}" df: ', log_args)
                return
        else: #reload != True 读入文件，看最后条目的日期，继续下载数据
            loader = LOADER[category]
            df = loader(ts_code)
            if isinstance(df, pd.DataFrame):
                try:
                    #last_date_str = df.iloc[0]['trade_date'] #注意是否所有类型都有'trade_date'字段
                    last_date_str = str(df.index.values[0])
                    #print('[L507] {}'.format(last_date_str))
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
                    last_date_str = _category_to_start_date_str(ts_code,category)
                    if last_date_str == None:
                        return
                last_date = date_str_to_date(last_date_str)
                today_str_ = today_str()
                today = date_str_to_date(today_str_) #只保留日期，忽略时间差别
                start_date = last_date + timedelta(1)
                _start_str = date_to_date_str(start_date)
                _end_str = today_str_
                if last_date < today:
                    _df = sgmt_download(ts_code,_start_str,_end_str,que_limit,category)
                    _frames = [_df,df]
                    #df=pd.concat(_frames,ignore_index=True,sort=False)
                    df=pd.concat(_frames,sort=False)
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
                    file_name = _category_to_file_name(ts_code,category)
                    if file_name == None:
                        return
                    df.to_csv(sub_path + sub_path_2nd_daily + '\\' + file_name)
                    if logable(40):
                        number_of_items = len(df)
                        log_args = [ts_code, category,file_name, number_of_items]
                        add_log(40,"[fn]download_data() ts_code:{0[0]}, category:{0[1]}, file:{0[2]}, total items:{0[3]}", log_args)
                    return df
            else:
                log_args = [ts_code]
                add_log(20, '[fn]download_data() ts_code "{0[0]}" load csv fail', log_args)
                return                
    else:
        log_args = [ts_code]
        add_log(20, '[fn]download_data(). ts_code "{0[0]}" invalid', log_args)
        return

def sgmt_download(ts_code,start_date_str,end_date_str,size,category):
    """
    通过TuShare API分段下载数据
    ts_code: <str> 对象的tushare代码 e.g. '399001.SZ'
    start_date_str: <str> 开始时间字符串 YYYYMMDD '19930723'
    end_date_str: <str> 结束时间字符串 YYYYMMDD '20190804'
    size: <int> 每个分段的大小 1 to xxxx
    category: <str> listed in HANDLER e.g. ts_pro.index_daily, ts_pro.sw_daily
    retrun: <df> if success, None if fail
    """
    TRY_TIMES = 20
    SLEEP_TIME = 20 #in seconds
    try:
        getter = GETTER[category]
    except KeyError:
        log_args = [ts_code, category]
        add_log(20, '[fn]sgmt_download(). ts_code: "{0[0]}" category: "{0[1]}" incorrect',log_args)
        return
    df = None
    start_date = date_str_to_date(start_date_str)
    end_date = date_str_to_date(end_date_str)
    duration = (end_date-start_date).days
    _start_str = start_date_str
    while duration > size:
        _end_time = date_str_to_date(_start_str) + timedelta(size)
        _end_str = date_to_date_str(_end_time)
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
            except Exception as e: #ConnectTimeout, 每分钟200:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
        if not isinstance(df,pd.DataFrame):
            df = _df
        else:
            _frames = [_df,df]
            df=pd.concat(_frames,ignore_index=True,sort=False)
        _start_time = _end_time + timedelta(1)
        _start_str = date_to_date_str(_start_time)
        duration = duration - size
    else:
        _end_str = end_date_str
        _try = 0
        while _try < TRY_TIMES:
            try:
                _df = getter(ts_code=ts_code,start_date=_start_str,end_date=_end_str)
            except Exception as e: #ConnectTimeout:
                time.sleep(SLEEP_TIME)
                _try += 1
                log_args = [ts_code, _try, e.__class__.__name__, e]
                add_log(30, '[fn]sgmt_download(). ts_code:{0[0]} _try: {0[1]}', log_args)
                add_log(30, '[fn]sgmt_download(). except_type:{0[2]}; msg:{0[3]}', log_args)
                continue
            break
        if not isinstance(df,pd.DataFrame):
            df = _df
        else:
            _frames = [_df,df]
            df=pd.concat(_frames,ignore_index=True)
    df.set_index('trade_date',inplace=True)
    return df

def bulk_download(al_file, reload=False):
    r"""
    根据资产列表文件，批量下载数据到csv文件
    需要在增加资产类别时改写
    al_file:<str> path for al file e.g. r'.\data_csv\assets_lists\al_<al_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    file_path = None
    if isinstance(al_file,str):
        if len(al_file)>0:        
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path == None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_download(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_download(). al_file "{0[0]}" not exist',log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_download(). df_al loaded -sucess, items:"{0[0]}"',log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category == None:
                continue
            # name,_type,stype1,stype2 = raw_data.all_assets_list.loc[ts_code][['name','type','stype1','stype2']]
            # category = None #资产的类别，传给下游[fn]处理
            # #--------------申万指数---------------
            # if _type == 'index' and stype1=='SW':
            #     category = 'index_sw'
            # #--------------上证指数---------------
            # if _type == 'index' and stype1=='SSE':
            #     category = 'index_sse'
            # #--------------深圳指数---------------
            # if _type == 'index' and stype1=='SZSE':
            #     category = 'index_szse'
            # #--------------个股---------------
            # if _type == 'stock':
            #     category = 'stock'
            # #--------------其它类型(未完成)----------
            # if category == None:
            #     log_args = [ts_code]
            #     add_log(20, '[fn]bulk_download(). No matched category for "{0[0]}"',log_args)
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            if reload==True or (not os.path.exists(file_path)):
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
    if isinstance(al_file,str):
        if len(al_file)>0:        
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path == None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_dl_appendix(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_dl_appendix(). al_file "{0[0]}" not exist',log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_dl_appendix(). df_al loaded -sucess, items:"{0[0]}"',log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category == None:
                continue
            elif category == 'stock':
                #-------------每日指标--------------
                category = 'stock_daily_basic' #股票每日指标
                file_name = 'db_' + ts_code + '.csv'
                file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
                if reload==True or (not os.path.exists(file_path)):
                    _reload = True
                else:
                    _reload = False
                download_data(ts_code, category, _reload)
                #-------------复权因子--------------
                category = 'adj_factor' #股票复权
                file_name = 'fq_' + ts_code + '.csv'
                file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
                if reload==True or (not os.path.exists(file_path)):
                    _reload = True
                else:
                    _reload = False
                download_data(ts_code, category, _reload)
            else:
                log_args = [ts_code,category]
                add_log(40, '[fn]bulk_dl_appendix(). {0[0]} category:{0[1]} skip',log_args)

def bulk_calc_dfq(al_file, reload=False):
    r"""
    根据资产列表文件，对category='stock'的，进行批量计算复权后的开收盘高低价
    al_file:<str> path for al file e.g. '.\data_csv\assets_lists\al_<al_file>.csv'
    reload:<bool> True重新下载完整文件
    """
    file_path = None
    if isinstance(al_file,str):
        if len(al_file)>0:        
            file_name = 'al_' + al_file + '.csv'
            file_path = sub_path + sub_path_al + '\\' + file_name
    if file_path == None:
        log_args = [al_file]
        add_log(10, '[fn]bulk_calc_dfq(). invalid al_file string: {0[0]}', log_args)
        return
    try:
        df_al = pd.read_csv(file_path, index_col='ts_code')
    except FileNotFoundError:
        log_args = [file_path]
        add_log(10, '[fn]bulk_calc_dfq(). al_file "{0[0]}" not exist',log_args)
        return
    log_args = [len(df_al)]
    add_log(40, '[fn]bulk_calc_dfq(). df_al loaded -sucess, items:"{0[0]}"',log_args)
    for index, row in df_al.iterrows():
        if row['selected'] == 'T' or row['selected'] == 't':
            ts_code = index
            category = All_Assets_List.query_category_str(ts_code)
            if category == None:
                continue
            elif category == 'stock':
                Stock.calc_dfq(ts_code, reload=reload)
            else:
                log_args = [ts_code,category]
                add_log(40, '[fn]bulk_calc_dfq(). {0[0]} category:{0[1]} skip',log_args)

def take_head_n(df, nrows):
    """
    df: <DataFrame>实例
    nrows: <int>需要提取的前nrows行
    retrun: <DataFrame>实例, None = failed
    """
    if not isinstance(df, pd.DataFrame):
        add_log(20, '[fn]take_head_n() df is not an instance of <DataFrame>')
        return
    nrows=int(nrows)
    result = df.head(nrows)
    return result

def load_source_df(ts_code,source,nrows=None):
    """
    根据source来选择不同数据源，返回df；数据源需要事先下载到csv，本函数不做补全
    source:<str> e.g. 'close_hfq' defined in SOURCE
    nrows: <int> 指定读入最近n个周期的记录,None=全部
    retrun:<df> trade_date(index); close; high...
    """
    try:
        #SOURCE[source]
        column_name = SOURCE_TO_COLUMN[source]
    except KeyError:
        log_args = [ts_code,source]
        add_log(20, '[fn]load_source_df ts_code:{0[0]}; source:{0[1]} not valid', log_args)
        return
    #---------------close_hfq 收盘后复权---------------
    if source == 'close_hfq':
        result = Stock.load_stock_dfq(ts_code=ts_code,nrows=nrows)[[column_name]]
        return result
    #---------------无匹配，报错---------------
    else:
        log_args = [ts_code,source]
        add_log(30, '[fn]load_source_df ts_code:{0[0]}; source:{0[1]} not matched', log_args)
        return

class All_Assets_List():
    """处理全资产列表"""

    @staticmethod
    def rebuild_all_assets_list(que_from_ts = False):
        """
        重头开始构建全资产列表
        que_from_ts: <bool> F：从文件读 T:从tushare 接口读
        """
        global raw_data
        file_name = "all_assets_list_rebuild.csv" #不同名避免误操作
        file_path_al = sub_path + sub_path_config + '\\' + file_name
        df_al = pd.DataFrame(columns=['ts_code','valid','selected','name','type','stype1','stype2'])
        df_al = df_al.set_index('ts_code')
        #--------------SW 指数---------------
        if que_from_ts == True:
            df_l1,df_l2,df_l3 = Index.get_sw_index_classify()
            df_l1=df_l1[['index_code','industry_name']]
            df_l2=df_l2[['index_code','industry_name']]
            df_l3=df_l3[['index_code','industry_name']]
        else:     
            file_path_sw_l1 = sub_path + '\\' + 'index_sw_L1_list.csv'
            file_path_sw_l2 = sub_path + '\\' + 'index_sw_L2_list.csv'
            file_path_sw_l3 = sub_path + '\\' + 'index_sw_L3_list.csv'
            try:
                _file_path = file_path_sw_l1
                df_l1 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
                _file_path = file_path_sw_l2
                df_l2 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
                _file_path = file_path_sw_l3
                df_l3 = pd.read_csv(_file_path,usecols=['index_code','industry_name'])
            except FileNotFoundError:
                log_args = [_file_path]
                add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
                return
        df_l1.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l1['valid'] = 'T'
        df_l1['selected'] = 'T'
        df_l1['type'] = 'index'
        df_l1['stype1'] = 'SW'
        df_l1['stype2'] = 'L1'
        df_l1.set_index('ts_code',inplace=True)
        df_l2.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l2['valid'] = 'T'
        df_l2['selected'] = 'T'
        df_l2['type'] = 'index'
        df_l2['stype1'] = 'SW'
        df_l2['stype2'] = 'L2'
        df_l2.set_index('ts_code',inplace=True)
        df_l3.rename(columns={'index_code':'ts_code','industry_name':'name'},inplace = True)
        df_l3['valid'] = 'T'
        df_l3['selected'] = 'T'
        df_l3['type'] = 'index'
        df_l3['stype1'] = 'SW'
        df_l3['stype2'] = 'L3'
        df_l3.set_index('ts_code',inplace=True)
        _frame = [df_al,df_l1,df_l2,df_l3]
        df_al = pd.concat(_frame,sort=False)

        #--------------上交所指数---------------
        if que_from_ts == True:
            raw_data.index.get_index_basic()
        _file_path = sub_path + '\\' + 'index_basic_sse.csv'        
        try:
            df_sse = pd.read_csv(_file_path,usecols=['ts_code','name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df_sse['valid'] = 'T'
        df_sse['selected'] = 'T'
        df_sse['type'] = 'index'
        df_sse['stype1'] = 'SSE'
        df_sse['stype2'] = ''
        df_sse.set_index('ts_code',inplace=True)
        _file_path = sub_path + '\\' + 'index_basic_szse.csv'        
        try:
            df_szse = pd.read_csv(_file_path,usecols=['ts_code','name'])
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df_szse['valid'] = 'T'
        df_szse['selected'] = 'T'
        df_szse['type'] = 'index'
        df_szse['stype1'] = 'SZSE'
        df_szse['stype2'] = ''
        df_szse.set_index('ts_code',inplace=True)
        _frame = [df_al,df_sse,df_szse]
        df_al = pd.concat(_frame,sort=False)
        #--------------个股---------------
        if que_from_ts == True:
            raw_data.stock.get_stock_basic()
        file_name = 'stock_basic.csv'
        _file_path = sub_path + '\\' + file_name    
        try:
            df = pd.read_csv(_file_path,usecols=['ts_code','name'],index_col='ts_code')
        except FileNotFoundError:
            log_args = [_file_path]
            add_log(10, '[fn]rebuild_all_assets_list(). file "{0[0]}" not found',log_args)
            return
        df['valid'] = 'T'
        df['selected'] = 'T'
        df['type'] = 'stock'
        #df['stype1'] = ''
        #df['stype2'] = ''
        df.loc[df.index.str.startswith('600'),'stype1'] = 'SHZB' #上海主板
        df.loc[df.index.str.startswith('601'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('602'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('603'),'stype1'] = 'SHZB'
        df.loc[df.index.str.startswith('688'),'stype1'] = 'KCB' #科创板
        df.loc[df.index.str.startswith('000'),'stype1'] = 'SZZB' #深圳主板
        df.loc[df.index.str.startswith('001'),'stype1'] = 'SZZB'
        df.loc[df.index.str.startswith('002'),'stype1'] = 'ZXB' #中小板
        df.loc[df.index.str.startswith('003'),'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('004'),'stype1'] = 'ZXB'
        df.loc[df.index.str.startswith('300'),'stype1'] = 'CYB' #创业板
        df['stype2'] = ''
        _frame = [df_al,df]
        df_al = pd.concat(_frame,sort=False)
        #--------------结尾---------------
        df_al.to_csv(file_path_al,encoding="utf-8")    
        return
    
    @staticmethod
    def query_category_str (ts_code):
        """
        根据all_assets_list中的type, stype1, stype2字段来返回category
        ts_code: <str> e.g. '000001.SZ'
        return: None, if no match
                <str> e.g. 'index_sw'; 'stock'
        """
        global raw_data
        name,_type,stype1,stype2 = raw_data.all_assets_list.loc[ts_code][['name','type','stype1','stype2']]
        category = None #资产的类别，传给下游[fn]处理
        #--------------申万指数---------------
        if _type == 'index' and stype1=='SW':
            category = 'index_sw'
        #--------------上证指数---------------
        elif _type == 'index' and stype1=='SSE':
            category = 'index_sse'
        #--------------深圳指数---------------
        elif _type == 'index' and stype1=='SZSE':
            category = 'index_szse'
        #--------------个股---------------
        #'stock_daily_basic'和'adj_factor'不在此处理
        elif _type == 'stock': 
            category = 'stock'
        #--------------其它类型(未完成)----------
        else:
            log_args = [ts_code]
            add_log(20, '[fn]All_Assets_List.query_category_str(). No matched category for "{0[0]}"',log_args)
            return
        return category

class Asset():
    """
    资产的基类
    """
    def __init__(self,ts_code):
        self.ts_code = ts_code
    
    def add_indicator(self,idt_name,idt_class,**kwargs):
        """
        添加Indicator的实例
        idt_name: 指标名 <str> e.g. 'ma10'
        idt_class: 指标类 <Class> e.g. Ma
        """
        if isinstance(idt_name,str):
            par_asset = weakref.ref(self) #用par_asset()应用原对象
            #print("[L664]", par_asset())
            idt = idt_class(ts_code=self.ts_code, par_asset=par_asset(),**kwargs)
            setattr(self,idt_name,idt)
            #locals()[idt_name] = "hahaho"
            #locals()['self.'+idt_name] = locals()[cls_name](ts_code=self.ts_code,par_asset=par_asset,**kwargs)
            try:
                idt = getattr(self,idt_name)
                if isinstance(idt,Indicator):
                    pass
                else:
                    log_args = [self.ts_code, idt_name]
                    add_log(20, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed.', log_args)
            except Exception as e:
                print("[L670] 待用明确的except替换")
                log_args = [self.ts_code, idt_name,e.__class__.__name__]
                add_log(10, '[fn]Asset.add_indicator() ts_code:{0[0]}, add idt_name:{0[1]} failed. Except:{0[2]}', log_args)
        else:
            log_args = [self.ts_code, idt_name]
            add_log(20, '[fn]Asset.add_indicator() ts_code:{0[0]}, idt_name:{0[1]} invalid.', log_args)
    
    def valid_idt_utd(self,idt_name):
        """
        validate if self.[ins]Indicator up to df_source date
        idt_name: <str> e.g. 'ema12_close_hfq'
        return: True: up to date; 
                None: invalid or not uptodate
        """
        from indicator import Indicator
        if hasattr(self,idt_name):
            idt = getattr(self,idt_name)
            if isinstance(idt,Indicator):
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
    def __init__(self,ts_code):
        Asset.__init__(self, ts_code=ts_code)

    @staticmethod
    def load_adj_factor(ts_code,nrows=None):
        """
        从文件读入复权因子
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'fq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['ts_code','trade_date','adj_factor'],index_col='trade_date',nrows=nrows)
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_adj_factor() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def load_stock_daily(ts_code,nrows=None):
        """
        从文件读入股票日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['ts_code','trade_date','close','open','high','low','pre_close','change','pct_chg','vol','amount'],index_col='trade_date',nrows=nrows)
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_daily() ts_code:{0[0]} invalid', log_args)
            return
    
    @staticmethod
    def load_stock_daily_basic(ts_code,nrows=None):
        """
        从文件读入股票日线指标数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'db_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['ts_code','trade_date','close','turnover_rate','turnover_rate_f','volume_ratio','pe','pe_ttm','pb','ps','ps_ttm','total_share','float_share','free_share','total_mv','circ_mv'],index_col='trade_date',nrows=nrows)
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_daily_basic() ts_code "{0[0]}" invalid', log_args)
            return
    
    @staticmethod
    def load_stock_dfq(ts_code,nrows=None):
        """
        从文件读入后复权的股票日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'dfq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
            result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['trade_date','adj_factor','close','open','high','low'],index_col='trade_date',nrows=nrows)
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Stock.load_stock_dfq() ts_code "{0[0]}" invalid', log_args)
            return

    @staticmethod
    def calc_dfq(ts_code,reload=False):
        """
        计算后复权的日线数据
        ts_code: <str> e.g. '000001.SZ'
        reload: <bool> True重头创建文件
        """
        global raw_data
        df_fq = Stock.load_adj_factor(ts_code)[['adj_factor']]
        fq_head_index_str, =df_fq.head(1).index.values
        #print('[354] latest_date_str:{}'.format(fq_head_index_str))
        df_stock = Stock.load_stock_daily(ts_code)[['close','open','high','low','vol','amount']]
        def _create_dfq():
            #---[drop]通过将df_factor头部的index定位到df_stock中行号x；x=0 无操作；x>0 drop df_stock前x行; 无法定位，倒查定位到df_factor中y，y>=0 无操作，无法定位 报错
            fq_head_in_stock = None
            try:
                fq_head_in_stock = df_stock.index.get_loc(fq_head_index_str) #fq头在stock中的位置
            except KeyError:
                stock_head_index_str, =df_stock.head(1).index.values
                try:
                    df_fq.index.get_loc(stock_head_index_str)
                except KeyError:
                    log_args = [ts_code]
                    add_log(20, '[fn]calc_dfq() ts_code:{0[0]}; head_index mutually get_loc fail; unknown problem', log_args) #df_stock和df_fq(复权）相互查询不到第一条index的定位
                    return
            #print('[357] fq_head_in_stock position:{}'.format(fq_head_in_stock))
            if fq_head_in_stock is not None:
                if fq_head_in_stock > 0:
                    df_stock.drop(df_stock.index[:fq_head_in_stock],inplace=True)
            #---[/drop]
            with pd.option_context('mode.chained_assignment', None): #将包含代码的SettingWithCopyWarning暂时屏蔽
                df_stock.loc[:,'adj_factor']=df_fq['adj_factor']
                df_stock.loc[:,'dfq_cls']=df_stock['close']*df_stock['adj_factor']
                df_stock.loc[:,'dfq_open']=df_stock['open']*df_stock['adj_factor']
                df_stock.loc[:,'dfq_high']=df_stock['high']*df_stock['adj_factor']
                df_stock.loc[:,'dfq_low']=df_stock['low']*df_stock['adj_factor']
            df_dfq = df_stock[['adj_factor','dfq_cls','dfq_open','dfq_high','dfq_low']]
            df_dfq.rename(columns={'dfq_cls':'close','dfq_open':'open','dfq_high':'high','dfq_low':'low'},inplace=True)
            return df_dfq
        
        def _generate_from_begin():
            """
            从头开始创建dfq文件
            """
            result = _create_dfq()
            if isinstance(result,pd.DataFrame):
                result.to_csv(file_path,encoding="utf-8")
                log_args = [file_name]
                add_log(40, '[fn]:Stock.calc_dfq() file: "{0[0]}" reloaded".', log_args)

        if raw_data.valid_ts_code(ts_code):
            file_name = 'dfq_' + ts_code + '.csv'
            file_path = sub_path + sub_path_2nd_daily + '\\' + file_name
        else:
            log_args = [ts_code]
            add_log(10, '[fn]Stock.calc_dfq() ts_code:{0[0]} invalid', log_args)
            return
        if reload == True:
            _generate_from_begin()
            return
        else: #read dfq filek, calculate and fill back the new items
            try:
                df_dfq = Stock.load_stock_dfq(ts_code)
            except FileNotFoundError:
                log_args = [file_path]
                add_log(20, '[fn]Stock.calc_dfq() file "{0[0]}" not exist, regenerate', log_args)
                _generate_from_begin()
                return
            dfq_head_index_str,  = df_dfq.head(1).index.values
            try:
                dfq_head_in_stock = df_stock.index.get_loc(dfq_head_index_str)
            except KeyError:
                log_args = [ts_code,dfq_head_index_str]
                add_log(20, '[fn]calc_dfq() ts_code:{0[0]}; dfq_head_index_str:"{0[1]}" not found in df_stock, df_stock maybe not up to date', log_args)
                return
            if dfq_head_in_stock == 0:
                log_args = [ts_code]
                add_log(40, '[fn]calc_dfq() ts_code:{0[0]}; df_dfq up to df_stock date, no need to update', log_args)
                return
            elif dfq_head_in_stock > 0:
                df_stock = take_head_n(df_stock,dfq_head_in_stock)
            try:
                dfq_head_in_fq = df_fq.index.get_loc(dfq_head_index_str)
            except KeyError:
                log_args = [ts_code,dfq_head_index_str]
                add_log(20, '[fn]calc_dfq() ts_code:{0[0]}; dfq_head_index_str:"{0[1]}" not found in df_fq, df_fq maybe not up to date', log_args)
                return
            if dfq_head_in_fq == 0:
                log_args = [ts_code]
                add_log(40, '[fn]calc_dfq() ts_code:{0[0]}; df_dfq up to df_fq date, no need to update', log_args)
                return
            elif dfq_head_in_fq > 0:
                df_fq = take_head_n(df_fq,dfq_head_in_fq)
            _df_dfq = _create_dfq()
            _frames=[_df_dfq,df_dfq]
            result=pd.concat(_frames,sort=False)
            result.to_csv(file_path,encoding="utf-8")
            log_args = [file_name]
            add_log(40, '[fn]:Stock.calc_dfq() file: "{0[0]}" updated".', log_args)

class Index():
    """
    指数相关，包括行业板块指数等
    """

    @staticmethod
    def get_sw_index_classify(return_df = True):
        """
        从ts_pro获取申万行业指数的分类
        """
        #一级行业列表
        file_name = "index_sw_L1_list.csv"
        df_l1 = ts_pro.index_classify(level='L1', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l1.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #二级行业列表
        file_name = "index_sw_L2_list.csv"
        df_l2 = ts_pro.index_classify(level='L2', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l2.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        #三级行业列表
        file_name = "index_sw_L3_list.csv"
        df_l3 = ts_pro.index_classify(level='L3', src='SW',fields='index_code,industry_name,level,industry_code,src')
        df_l3.to_csv(sub_path + '\\' + file_name, encoding="utf-8")
        if return_df != True:
            return None
        return (df_l1, df_l2, df_l3)

    @staticmethod    
    def load_index_daily(ts_code,nrows=None):
        """
        从文件读入指数日线数据
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        return: <df>
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            file_name = 'd_' + ts_code + '.csv'
            result = pd.read_csv(sub_path + sub_path_2nd_daily + '\\' + file_name,dtype={'trade_date':str},usecols=['ts_code','trade_date','close','open','high','low','pre_close','change','pct_chg','vol','amount'],index_col='trade_date',nrows=nrows)
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
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
                result = pd.read_csv(file_path,dtype={'trade_date':str},usecols=['ts_code','trade_date','name','open','low','high','close','change','pct_change','vol','amount','pe','pb'],index_col='trade_date',nrows=nrows)
            except FileNotFoundError:
                log_args = [file_path]
                add_log(20, '[fn]Index.load_sw_daily() "{0[0]}" not exist', log_args)
                return
            result['vol']=result['vol'].astype(np.int64)
            #待优化，直接在read_csv用dtype指定‘vol’为np.int64
            return result
        else:
            log_args = [ts_code]
            add_log(20, '[fn]Index.load_sw_daily() ts_code "{0[0]}" invalid', log_args)
            return

#LOADER读入.csv数据的接口
LOADER = {'index_sse':Index.load_index_daily,
          'index_szse':Index.load_index_daily,
          'index_sw':Index.load_sw_daily,
          'stock':Stock.load_stock_daily,
          'stock_daily_basic':Stock.load_stock_daily_basic,
          'adj_factor':Stock.load_adj_factor}
#GETTER从Tushare下载数据的接口
GETTER = {'index_sse':ts_pro.index_daily,
          'index_szse':ts_pro.index_daily,
          'index_sw':ts_pro.sw_daily,
          'stock':ts_pro.daily,
          'stock_daily_basic':ts_pro.daily_basic,
          'adj_factor':ts_pro.adj_factor}
#QUE_LIMIT,Tushare接口的单查询条目上限
QUE_LIMIT = {'index_sse':8000,
             'index_szse':8000,
             'index_sw':1000,
             'stock':4000,
             'stock_daily_basic':8000,
             'adj_factor':8000}

class Plot_Utility():
    """存放绘图出报告用的公用工具"""
    @staticmethod
    def gen_al(al_name=None, ts_code=None,valid='T',selected='T',type_=None,stype1=None,stype2=None):
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
            _aal = raw_data.all_assets_list #all=all_assets_list
            if ts_code == None:
                al = _aal
            else: #[未完成] 考虑批量传入
                print("[not complete L285] 考虑批量传入")
                return
            #-------valid-----------
            if valid=='T':
                al = al[(al.valid=='T')]
            elif isinstance(valid,str):
                al = al[(al.valid==valid)]
            #-------selected-----------
            if selected=='T':
                al = al[(al.selected=='T')]
            elif isinstance(selected,str):
                al = al[(al.valid==selected)]
            #-------type-----------
            if isinstance(type_,str):
                al = al[(al.type==type_)]
            #-------stype1-----------
            if isinstance(stype1,str):
                al = al[(al.stype1==stype1)]
            #-------stype2-----------
            if isinstance(stype2,str):
                al = al[(al.stype2==stype2)]
            
            al = al['selected']
        else:
            add_log(10, '[fn]Plot_Utility.gen_al(). raw_data.all_assets_list is not valid')
            return
        if isinstance(al_name,str):
            if len(al_name) > 0:
                file_name = 'al_' + al_name + '.csv'
                file_path = sub_path + sub_path_al + '\\' + file_name
                al.to_csv(file_path,encoding='utf-8',header=True) #haeder=True是Series.to_csv的处理，否则Warning
                log_args = [al_name]
                add_log(40, '[fn]Plot_Utility.gen_al(). "al_{0[0]}.csv" generated', log_args)
            else:
                log_args = [al_name]
                add_log(20, '[fn]Plot_Utility.gen_al(). al_name invalid. "al_{0[0]}.csv" file not generated', log_args)
        return al

class Plot_Assets_Racing():
    """资产竞速图表：不同资产从同一基准起跑，一定时间内的价格表现
    """
    def __init__(self,al_file,period=30):
        """
        al_file: <str> 资产表的文件名e.g.'al_SW_Index_L1.csv'
        period: <int> 比较的周期
        """
        global raw_data
        al_path = sub_path + sub_path_al + '\\' + al_file
        try:
            al_df = pd.read_csv(al_path)
            #print("[debug L335] al_df:{}".format(al_df))
        except FileNotFoundError:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). file "{0[0]}" not found',log_args)
            return
        al_df.set_index('ts_code',inplace=True)
        #print("[debug L341] al_df:{}".format(al_df))
        self.al = al_df[al_df=='T'].index #index of ts_code
        if len(self.al) == 0:
            log_args = [al_path]
            add_log(10, '[fn]Plot_Assets_Racing.__init__(). no item in "{0[0]}"',log_args)
            return
        fig = plt.figure()
        ax = fig.add_subplot(111)
        _aal = raw_data.all_assets_list
        self.raw_data = pd.DataFrame(columns=['ts_code','name','base_close','last_chg','df'])
        self.raw_data.set_index('ts_code',inplace=True)
        for ts_code in self.al:
            #print("[debug L349] ts_code:{}".format(ts_code))
            name,_type, stype1, stype2 = _aal.loc[ts_code][['name','type','stype1','stype2']]
            handler = None
            #--------------申万指数---------------
            if _type=='index' and stype1=='SW':
                handler = Index.load_sw_daily
            #--------------上证及深圳指数---------------
            if _type=='index' and (stype1=='SSE' or stype1=='SZSE'):
                handler = Index.load_index_daily
            #--------------个股类型---------------
            if _type=='stock':
                handler = Stock.load_stock_daily
            #--------------handler = (未完成)----------
            if handler == None:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing.__init__(). No matched handler for "{0[0]}"',log_args)
                continue
            #print("[debug L777] df:{}".format(handler))
            df = handler(ts_code=ts_code, nrows=period)
            if isinstance(df,pd.DataFrame):
                log_args = [ts_code]
                add_log(40, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -success-', log_args)
            else:
                log_args = [ts_code]
                add_log(20, '[fn]Plot_Assets_Racing() ts_code: "{0[0]}"  df load -fail-', log_args)
                continue
            #df = df[['trade_date','close']]
            df = df[['close']] #创建单列df而不是serial
            #df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.index = pd.to_datetime(df.index.astype('str'))
            #df.set_index('trade_date', inplace=True)
            base_close, = df.tail(1)['close'].values
            df['base_chg_pct']=(df['close']/base_close-1)*100
            last_chg, = df.head(1)['base_chg_pct'].values
            row = pd.Series({'ts_code':ts_code,'name':name,'base_close':base_close,'last_chg':last_chg,'df':df},name=ts_code)
            #print("[L383] row:{}".format(row))
            self.raw_data=self.raw_data.append(row)
            #self.raw_data.loc[ts_code]=[name,base_close,last_chg,df]
        #print("[L383] self.raw_data:{}".format(self.raw_data))
        self.raw_data.sort_values(by='last_chg',inplace=True,ascending=False)
        result = self.raw_data[['name','last_chg']]
        print(result)
        file_name = str('资产竞速{}周期涨幅比较_{}'.format(period,al_file[3:-4])) + '_' + today_str() + '.csv'
        file_path = sub_path_result + r'\Plot_Assets_Racing' + '\\' + file_name
        result.to_csv(file_path,encoding="utf-8")
        log_args = [file_path]
        add_log(40, '[fn]:Plot_Assets_Racing() result saved to file "{}"', log_args)
        for ts_code, pen in self.raw_data.iterrows():
            name, last_chg, df = pen['name'],pen['last_chg'],pen['df']
            last_chg = str(round(last_chg,2))
            label = last_chg + '%  ' + name #  + '\n' + ts_code
            ax.plot(df.index,df['base_chg_pct'],label=label,lw=1)
        plt.legend(handles=ax.lines)
        plt.grid(True)
        mpl.rcParams['font.sans-serif'] = ['FangSong'] # 指定默认字体
        mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
        plt.xticks(df.index,rotation='vertical')
        plt.title('资产竞速{}周期涨幅比较 - {}'.format(period,al_file[3:-4]))
        plt.ylabel('收盘%')
        plt.subplots_adjust(left=0.03, bottom=0.11, right=0.85, top=0.97, wspace=0, hspace=0)
        plt.legend(bbox_to_anchor=(1, 1),bbox_transform=plt.gcf().transFigure)
        plt.show()

if __name__ == "__main__":
    #global raw_data
    start_time = datetime.now()
    #df = get_stock_list()
    #df = load_stock_list()
    #df = get_daily_basic()
    #cl = get_trade_calendar()
    #last_trad_day_str()
    #raw_data = Raw_Data(pull=False)
    #c = raw_data.trade_calendar
    #index = raw_data.index
    #zs = que_index_daily(ts_code="000009.SH",start_date="20031231")
    #ttt = index.get_index_daily('399003.SZ',reload=False)
    # #------------------------批量下载数据-----------------------
    #download_path = r"download_all"
    #download_path = r"dl_stocks"
    download_path = r"try_001"
    #download_path = r"user_001"
    #bulk_download(download_path,reload=True) #批量下载数据
    #download_path = r"dl_stocks"
    #bulk_dl_appendix(download_path,reload=True) #批量下载股票每日指标数据，及股票复权因子
    #ttt = ts_pro.index_daily(ts_code='801001.SI',start_date='20190601',end_date='20190731')
    #ttt = ts_pro.sw_daily(ts_code='950085.SH',start_date='20190601',end_date='20190731')
    #Plot.try_plot()
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
    #al_l1 = Plot_Utility.gen_al(al_name='SW_Index_L1',stype1='SW',stype2='L1') #申万一级行业指数
    # al_l2 = Plot_Utility.gen_al(al_name='SW_Index_L2',stype1='SW',stype2='L2') #申万二级行业指数
    # al_l3 = Plot_Utility.gen_al(al_name='SW_Index_L3',stype1='SW',stype2='L3') #申万二级行业指数
    # al_dl_stocks = Plot_Utility.gen_al(al_name='dl_stocks',selected=None,type_='stock')#全部valid='T'的资产
    # al_download = Plot_Utility.gen_al(al_name='download_all',selected=None)#全部valid='T'的资产
    # #-------------------Plot_Assets_Racing资产竞速-----------------------
    # plot_ar = Plot_Assets_Racing('al_SW_Index_L1.csv',period=5)
    # plot_ar = Plot_Assets_Racing('al_user_001.csv',period=5)
    # #-------------------Stock Class-----------------------
    #stock = Stock(pull=True)
    #stock = Stock()
    #-------------------复权测试-----------------------
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
    al_file_str = r"dl_stocks"
    #bulk_calc_dfq(al_file_str,reload=False) #批量计算复权
    # print("===================Indicator===================")
    from indicator import Indicator, Ma, Ema
    print('------stock1.ma10_close_hfq--------')
    stock1 = Stock(ts_code='000002.SZ')
    kwargs = {'idt_name': 'ma10_close_hfq',
              'idt_class': Ma,
              'period': 10}
    stock1.add_indicator(**kwargs)
    stock1.ma10_close_hfq.calc_idt()
    ma10 = stock1.ma10_close_hfq.df_idt
    print(ma10)

    print('------stock1.ema26_close_hfq--------')
    kwargs = {'idt_name': 'ema26_close_hfq',
              'idt_class': Ema,
              'period': 26}
    stock1.add_indicator(**kwargs)
    stock1.ema26_close_hfq.calc_idt()
    ema26 = stock1.ema26_close_hfq.df_idt
    print(ema26)

    print('------stock1.ema12_close_hfq--------')
    kwargs = {'idt_name': 'ema12_close_hfq',
              'idt_class': Ema,
              'period': 12}
    stock1.add_indicator(**kwargs)
    stock1.ema12_close_hfq.calc_idt()
    ema12 = stock1.ema12_close_hfq.df_idt
    print(ema12)

    print('------valid Indicator up to soruce date--------')
    print('uptodate ma10:',stock1.valid_idt_utd('ma10_close_hfq'))
    print('uptodate ema12:',stock1.valid_idt_utd('ema12_close_hfq'))
    print('uptodate ema26:',stock1.valid_idt_utd('ema26_close_hfq'))

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
    
