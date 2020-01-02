import tushare as ts
import pandas as pd
import numpy as np
from st_common import raw_data
from st_common import sub_path, sub_path_2nd_daily, sub_path_config, sub_path_al, sub_path_result, sub_idt
from st_common import SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
# from st_common import Raw_Data, raw_data_init, SUBTYPE, SOURCE, SOURCE_TO_COLUMN, STATUS_WORD, DOWNLOAD_WORD, DEFAULT_OPEN_DATE_STR
from datetime import datetime,timedelta
from XF_common.XF_LOG_MANAGE import add_log, logable, log_print
# from st_board import load_source_df, Stock, Index
import st_board
import weakref


def idt_name(pre_args):
    """
    pre_args: <dict> 创建para的必要输入参数
        e.g.
        {'idt_type': 'macd',
         'long_n1': 26,
         'short_n2': 12,
         'dea_n3': 9,
         'field': 'DEA'  # 在idt结果为多列，选取非默认列时需要填
         'source': 'close',
         'subtype': 'w',
         'update_csv': False}
         or
         {'idt_type': 'const',
          'const_value': 30}
         
    return: <dict> of attributes for initialize the indicator, called post_args, 函数的主要部分是对其中idt_name键值的计算
        e.g.
        {'idt_type': 'macd',
         'idt_name': 'macd_close_w_26_12_9',
         'source': 'close',
         'subtype': 'w',
         ‘field': 'DEA',
         'idt_class': Macd}
         or
         {'idt_type': 'const',
          'idt_name': 'const',
          'const_value': 30}
    """

    if isinstance(pre_args, dict):
        idt_type = pre_args["idt_type"]
        if idt_type == 'const':  # para为常量的情况
            pre_args['idt_name'] = 'const'
            return pre_args
        else:
            idt_name = ""
            idt_class = IDT_CLASS[idt_type]
            idt_name = idt_name + idt_type
            try:
                source = pre_args["source"]
                if source != 'close_hfq':
                    idt_name = idt_name + '_' + source
            except KeyError:
                pass
            try:
                subtype = pre_args["subtype"]
                if subtype.lower() != 'd':
                    idt_name = idt_name + '_' + subtype.lower()
            except KeyError:
                pass
            try:
                period = pre_args["period"]
                idt_name = idt_name + '_' + str(period)
            except KeyError:
                pass
            for k,v in pre_args.items():
                if k.endswith("_n1"):
                    idt_name = idt_name + '_' + str(v)
                if k.endswith("_n2"):
                    idt_name = idt_name + '_' + str(v)
                if k.endswith("_n3"):
                    idt_name = idt_name + '_' + str(v)
            pre_args['idt_name'] = idt_name
            pre_args['idt_class'] = idt_class
            return pre_args
    else:
        log_args = [type(pre_args)]
        add_log(20, '[fn]idt_name() input type:{0[0]} is not <dict>', log_args)


class Indicator:
    """
    指标的基本类
    """
    def __new__(cls, ts_code, par_asset, idt_type, reload=False, update_csv=True, subtype='D'):
        """
        检验ts_code的有效性
        """
        global raw_data
        if raw_data.valid_ts_code(ts_code):
            obj = super().__new__(cls)
            return obj
        else:
            log_args = [ts_code]
            add_log(10, '[fn]Indicator.__new__() ts_code "{0[0]}" invalid, instance not created', log_args)
            return

    def __init__(self, ts_code, par_asset, idt_type, reload=False, update_csv=True, subtype='D'):
        """
        ts_code:<str> e.g. '000001.SH'
        reload:<bool> True: igonre the csv, generate the df from the begining
        update_csv:<bool> True: update the csv; False: keep the original csv as it is
        """
        self.ts_code = ts_code
        self.subtype = subtype
        self.idt_type = idt_type
        self.df_idt = None  # <df>存放指标的结果数据
        self.source = None  # <str>数据源，如'close_hfq'收盘后复权
        self.update_csv = update_csv
        self.idt_name = None
        self.file_name = None
        self.file_path = None
        self.par_asset = weakref.ref(par_asset)  # <Asset>父asset对象

    def load_sources(self, nrows=None):
        """
        调用st_board.load_source_df()来准备计算用原数据
        ts_code:<str> e.g. '000001.SH'
        nrows: <int> 指定读入最近n个周期的记录,None=全部
        retrun:<df> trade_date(index); close; high..., None if failed
        """
        from st_board import load_source_df
        df_source = load_source_df(ts_code=self.ts_code, source=self.source)
        return df_source

    def load_idt(self, nrows=None):
        """
        将历史指标的数据载入;
        """
        file_path = self.file_path  # 用到的file_path需要在具体的继承子类中定义
        try:
            df_idt = pd.read_csv(file_path, dtype={'trade_date': str}, index_col='trade_date', nrows=nrows)
        except FileNotFoundError:
            log_args = [file_path]
            add_log(30, '[fn]Indicator.load_idt() file "{0[0]}" not exist', log_args)
            return None
        self.df_idt = df_idt
        return self.df_idt

    def valid_utd(self):
        """
        validate if self.df_idt up to df_source date
        return: True: up to date; 
                None: invalid or not up to date
        """
        if isinstance(self.df_idt,pd.DataFrame):
            df_source_head = self.load_sources(nrows=1)
            source_date = df_source_head.index[0]
            idt_date = self.df_idt.head(1).index
            if idt_date == source_date:
                return True
            else:
                log_args = [self.ts_code,self.idt_name,idt_date,source_date]
                add_log(40, '[fn]Indicator.valid_utd() ts_code:{0[0]} {0[1]} not uptodate. idt:{0[2]} source:{0[3]}', log_args)
        else:
            log_args = [self.ts_code,self.idt_name]
            add_log(40, '[fn]Indicator.valid_utd() ts_code:{0[0]} {0[1]} not loaded', log_args)

    def calc_idt(self):
        """
        调用self._calc_res()补完df_idt数据
        """
        df_append = self._calc_res()
        if isinstance(df_append, pd.DataFrame):
            if isinstance(self.df_idt, pd.DataFrame):
                _frames = [df_append, self.df_idt]
                df_idt = pd.concat(_frames, sort=False)
            else:
                df_idt = df_append
            if self.update_csv:
                if st_board.valid_file_path(self.file_path):
                    df_idt.to_csv(self.file_path)
                    log_args = [self.ts_code, self.file_path]
                    add_log(40, "[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, saved", log_args)
                else:
                    log_args = [self.ts_code, self.file_path]
                    add_log(20, "[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, invalid", log_args)
            self.df_idt = df_idt
            log_args = [self.ts_code, self.file_name[:-4], len(df_idt)]
            add_log(40, "[fn]Indicator.calc_idt() ts_code:{0[0]}, {0[1]} updated; items:{0[2]}", log_args)
        elif df_append is None:
            pass  # keep self.df_idt as it is
        else:
            log_args = [self.ts_code, type(self.df_idt)]
            add_log(10, "[fn]Indicator.calc_idt() ts_code:{0[0]}, type(df_append):{0[1]}, unknown problem", log_args)
            pass  # keep self.df_idt as it is

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        print('[Note] Indicator._calc_res() 需要分别在各指标类中重构')
        return False  # 清除VS_CODE报的问题

    # def _idt_name(self):
    #     """
    #     常规单周期参数indicator返回idt_name,非常规多周期参数命名需要在[ins]Indicator中重构
    #     """
    #     # e.g. ma_close_m_13 or ma_20
    #     if hasattr(self, 'period'):
    #         idt_name = self.idt_type
    #         if self.source != 'close_hfq':
    #             idt_name = idt_name + '_' + self.source
    #         if self.subtype.lower() != 'd':
    #             idt_name = idt_name + '_' + self.subtype.lower()
    #         idt_name = idt_name + '_' + str(self.period)
    #         return idt_name
    #     else:
    #         log_args = [self.subtype]
    #         add_log(20, '[fn]Indicator._idt_name() subtype:{0[0]} no period [fn]需要在[ins]重构', log_args)


class Ma(Indicator):
    """
    移动平均线
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Ma> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Ma.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        period = int(period)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'ma'
        self.period = period
        # print("[L97] 补period类型异常")
        self.source = source
        # self.idt_name = self._idt_name()
        self.idt_name = idt_name
        self.file_name = 'idt_' + ts_code + '_' + self.idt_name + '.csv'
        # self.file_name = 'idt_' + ts_code + '_' + self.source + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
        self.file_path = sub_path + sub_idt + '\\' + self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        if isinstance(df_idt, pd.DataFrame):
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[idt_head_in_source + period - 1:], inplace=True)
                values = []
                rvs_rslt = []
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [self.ts_code, self.source]
                    add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                    return
                for idx in reversed(df_source.index):
                    values.append(df_source[source_column_name][idx])
                    if len(values) > period:
                        del values[0]
                    if len(values) == period:
                        rvs_rslt.append(np.average(values))
        else:  # .csv file not exist
            try:
                source_column_name = SOURCE_TO_COLUMN[self.source]
            except KeyError:
                log_args = [self.ts_code, self.source]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                return
            values = []
            rvs_rslt = []
            for idx in reversed(df_source.index):
                values.append(df_source[source_column_name][idx])
                if len(values) > period:
                    del values[0]
                if len(values) == period:
                    rvs_rslt.append(np.average(values))
        iter_rslt = reversed(rvs_rslt)
        rslt = list(iter_rslt)
        index_source = df_source.index[:len(df_source)-period+1]
        idt_column_name = 'MA'
        data = {idt_column_name: rslt}
        df_idt_append = pd.DataFrame(data, index=index_source)
        return df_idt_append


class Ema(Indicator):
    """
    指数移动平均线
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Ema> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Ema.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        period = int(period)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset,idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'ema'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = 'idt_' + ts_code + '_' + self.idt_name + '.csv'
        # self.file_name = 'idt_' + ts_code + '_' + self.source + '_' + self.idt_type + '_' + subtype + str(period) + '.csv'
        self.file_path = sub_path + sub_idt + '\\' + self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        if isinstance(df_idt, pd.DataFrame):
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Ema._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[idt_head_in_source + 1:], inplace=True)
                rvs_rslt = []
                i = 0
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [self.ts_code, self.source]
                    add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                    return
                for idx in reversed(df_source.index):
                    if i == 0:
                        idt_column_name = 'EMA'
                        past_ema = df_idt[idt_column_name][idx]
                        rvs_rslt.append(df_idt[idt_column_name][idx])
                        i += 1
                    else:
                        # Y=[2*X+(N-1)*Y’]/(N+1)
                        today_ema = (2 * df_source[source_column_name][idx] + (period - 1) * past_ema) / (period + 1)
                        past_ema = today_ema
                        rvs_rslt.append(today_ema)
        else:  # .csv file not exist
            try:
                source_column_name = SOURCE_TO_COLUMN[self.source]
            except KeyError:
                log_args = [self.ts_code,self.source]
                add_log(20, '[fn]Ema._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                return
            rvs_rslt = []
            i = 0
            for idx in reversed(df_source.index):
                if i==0:
                    past_ema = df_source[source_column_name][idx]
                    rvs_rslt.append(df_source[source_column_name][idx])
                    i += 1
                else:
                    # Y=[2*X+(N-1)*Y’]/(N+1)
                    today_ema = (2 * df_source[source_column_name][idx] + (period - 1) * past_ema) / (period + 1)
                    past_ema = today_ema
                    rvs_rslt.append(today_ema)
        del rvs_rslt[0]
        iter_rslt = reversed(rvs_rslt)
        rslt = list(iter_rslt)
        index_source = df_source.index[:-1]
        idt_column_name = 'EMA'
        data = {idt_column_name: rslt}
        df_idt_append = pd.DataFrame(data, index=index_source)
        return df_idt_append

    @staticmethod
    def idt_bare(sr_data, period):
        """
        中间变量ema计算，
        sr_data: <pd.Series> 原数据，从新到旧排列；如果数据是截取的原数据的后段，sr_data的最老一个值要用ema老结果的最新值替换来保证衔接正确！
        period: <int> 周期数
        return: <pd.Series> 从新到旧排列
        """
        if isinstance(sr_data, pd.Series):
            rvs_rslt = []
            i = 0
            for idx in reversed(sr_data.index):
                if i == 0:
                    past_ema = sr_data[idx]
                    rvs_rslt.append(sr_data[idx])
                    i += 1
                else:
                    # Y=[2*X+(N-1)*Y’]/(N+1)
                    today_ema = (2 * sr_data[idx] + (period - 1) * past_ema) / (period + 1)
                    past_ema = today_ema
                    rvs_rslt.append(today_ema)
            del rvs_rslt[0]
            iter_rslt = reversed(rvs_rslt)
            rslt = list(iter_rslt)
            index_source = sr_data.index[:-1]
            sr_result = pd.Series(rslt, index=index_source)
            return sr_result
        else:
            log_args = [type(sr_data)]
            add_log(20, '[fn]idt_bare() sr_data type:{0[0]} is not pd.Series', log_args)


class Macd(Indicator):
    """
    MACD
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, long_n1=26, short_n2=12, dea_n3=9, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close_hfq' #SOURCE
        return:<ins Macd> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Macd.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        # long_n1 = int(long_n1)
        # short_n2 = int(short_n2)
        # dea_n3 = int(dea_n3)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset,idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, long_n1=26, short_n2=12, dea_n3=9, source='close_hfq', reload=False, update_csv=True, subtype='D'):
        """
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'macd'
        self.long_n1 = long_n1
        self.short_n2 = short_n2
        self.dea_n3 = dea_n3
        self.source = source
        # self.idt_name = self._idt_name()
        self.idt_name = idt_name
        self.file_name = 'idt_' + ts_code + '_' + self.idt_name + '.csv'
        self.file_path = sub_path + sub_idt + '\\' + self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        long_n1 = self.long_n1
        short_n2 = self.short_n2
        dea_n3 = self.dea_n3
        parent = self.par_asset()
        df_ema_long = None  # 前置idt
        df_ema_short = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_ema_long, df_ema_short
            """
            nonlocal df_ema_long, df_ema_short
            # 前置指标名idt_name计算
            _kwargs = {
                        'idt_type': 'ema',
                        'period': long_n1,
                        'source': self.source,
                        'update_csv': False}
            kwargs_long = idt_name(_kwargs)
            idt_ema_long_name = kwargs_long['idt_name']
            _kwargs = {
                        'idt_type': 'ema',
                        'period': short_n2,
                        'source': self.source,
                        'update_csv': False}
            kwargs_short = idt_name(_kwargs)
            idt_ema_short_name = kwargs_short['idt_name']

            # ------valid pre-idts uptodate------
            if hasattr(parent, idt_ema_long_name):
                idt_ema_long = getattr(parent, idt_ema_long_name)
                if idt_ema_long.valid_utd() is not True:
                    idt_ema_long.calc_idt()
            else:
                parent.add_indicator(**kwargs_long)
                idt_ema_long = getattr(parent, idt_ema_long_name)
                # idt_ema_long.calc_idt() #add_indicator()里自带calc_idt()
            # idt_ema_short
            if hasattr(parent, idt_ema_short_name):
                idt_ema_short = getattr(parent, idt_ema_short_name)
                if idt_ema_short.valid_utd() is not True:
                    idt_ema_short.calc_idt()
            else:
                parent.add_indicator(**kwargs_short)
                idt_ema_short = getattr(parent, idt_ema_short_name)
                # idt_ema_short.calc_idt(), add_indicator()里自带calc_idt()
            if n is None:
                df_ema_long = idt_ema_long.df_idt
                df_ema_short = idt_ema_short.df_idt
            else:
                df_ema_long = idt_ema_long.df_idt.head(n)
                df_ema_short = idt_ema_short.df_idt.head(n)

        # ---------主要部分开始------------
        last_dea = None
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Macd._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Macd._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source + 1)  # 前置指标处理
                last_dea = self.df_idt.iloc[0]['DEA']
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        sr_diff_append = pd.Series()
        short_col_name = 'EMA'
        long_col_name = 'EMA'
        # print('[L538]', df_ema_short)
        # print('[L539]', df_ema_long)
        sr_diff_append = df_ema_short[short_col_name] - df_ema_long[long_col_name]
        if last_dea is not None:
            sr_diff_append.iloc[-1] = last_dea  # 用原记录里最新的值替换待处理数据源的最老的值
        sr_dea_append = Ema.idt_bare(sr_diff_append, dea_n3)
        sr_diff_append = sr_diff_append[:-1]
        sr_macd_append = 2*(sr_diff_append - sr_dea_append)
        _frames = [sr_diff_append, sr_dea_append, sr_macd_append]
        df_append = pd.concat(_frames, sort=False, axis=1)
        df_append.columns = ('DIFF', 'DEA', 'MACD')
        return df_append

    # def _idt_name(self):
    #     """
    #     Macd重构返回idt_name
    #     """
    #     #e.g. macd_close_m_13 or macd_26_12_9
    #     idt_name = self.idt_type
    #     if self.source != 'close_hfq':
    #         idt_name = idt_name + '_' + self.source
    #     if self.subtype.lower() != 'd':
    #         idt_name = idt_name + '_' + self.subtype.lower()
    #     idt_name = idt_name + '_' + str(self.long_n1) + '_' + str(self.short_n2) + '_' + str(self.dea_n3)
    #     return idt_name


IDT_CLASS = {'ma': Ma,
             'ema': Ema,
             'macd': Macd}

if __name__ == '__main__':
    start_time = datetime.now()
    # raw_data_init()
    from st_board import Stock, Index
    global raw_data
    # ------------------test---------------------

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
