import pandas as pd
import numpy as np
from st_common import sub_path, sub_idt
from st_common import SUBTYPE, SOURCE_TO_COLUMN
from st_common import raw_data
from st_common import INDEX, FUND
from datetime import datetime
from XF_LOG_MANAGE import add_log
import weakref
from pathlib import PurePath


def idt_name(pre_args):
    """
    指标周期类的参数（period, long_n3, medium_n2...，即使有默认值，初始化时也不建议省略，省略的话idt_name会不同
    pre_args: <dict> 创建para的必要输入参数
        e.g.
        {'idt_type': 'macd',
         'long_n3': 26,
         'short_n2': 12,
         'dea_n3': 9,
         'field': 'DEA'  # 在idt结果为多列，选取非默认列时需要填
         'source': 'close',
         'subtype': 'w',
         'update_csv': False,  # 指标文件结果是否保存到csv文件
         'reload': False  # 功能待查看代码
         }

    return: <dict> post_args: attributes for initialize the indicator, called post_args, 函数的主要部分是对其中idt_name键值的计算
        e.g.
        {'idt_type': 'macd',
         'idt_name': 'macd_close_w_26_12_9',
         'source': 'close',
         'subtype': 'w',
         ‘field': 'DEA',
         'idt_class': Macd}
    """

    if isinstance(pre_args, dict):
        idt_type = pre_args["idt_type"]
        idt_name = ""
        idt_class = IDT_CLASS[idt_type]
        idt_name = idt_name + idt_type
        try:
            source = pre_args["source"]
            if source != 'close':
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
        for k, v in pre_args.items():
            if k.endswith("_n1"):
                idt_name = idt_name + '_' + str(v)
            if k.endswith("_n2"):
                idt_name = idt_name + '_' + str(v)
            if k.endswith("_n3"):
                idt_name = idt_name + '_' + str(v)
        pre_args['idt_name'] = idt_name
        pre_args['idt_class'] = idt_class
        post_args = pre_args
        return post_args
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
        # global raw_data
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
        reload:<bool> True: ignore the csv, generate the df from tushare interface
        update_csv:<bool> True: update the csv; False: keep the original csv as it is
        """
        self.ts_code = ts_code
        self.subtype = subtype
        self.idt_type = idt_type  # ?为什么把这参数放这，而不是用子类里的，忘记。暂留以后优化
        self.df_idt = None  # <df>存放指标的结果数据
        self.source = None  # <str>数据源，如'close'收盘后复权
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
        return:<sr> 从asset.daily_data中获得 trade_date(index); source,
               <df> 从csv中读取，很少情况
               None if failed
        """
        from st_board import LOADER, Stock, Fund
        asset = self.par_asset()
        category = asset.category
        source = self.source
        ts_code = asset.ts_code
        # 如果在asset的daily_data中直接有该源，则直接使用
        if hasattr(asset.daily_data, source):
            sr = getattr(asset.daily_data, source)
            sr.rename(source.upper())
            if isinstance(sr, pd.DataFrame):
                return sr
        # 直接从.csv中读入
        # ----确定column_name
        BASIC = {'close', 'open', 'high', 'low', 'vol', 'amount'}
        if self.source in BASIC:
            column_name = self.source
        # elif  其它source待完善
        else:
            log_args = [source]
            add_log(20, '[fn]Indicator.load_sources(). source:{0[0]} invalid', log_args)
            return

        # INDEX = {'index_sw', 'index_sse', 'index_szse'}
        if category == 'stock':
            loader = lambda ts_code, nrows: Stock.load_stock_dfq(ts_code=ts_code, nrows=nrows)[[column_name]]
        elif category in INDEX:
            loader = lambda ts_code, nrows: LOADER[category](ts_code=ts_code, nrows=nrows)[[column_name]]
        elif category in FUND:
            loader = lambda ts_code, nrows: Fund.load_fund_dfq(ts_code=ts_code, nrows=nrows)[[column_name]]
        else:
            log_args = [source]
            add_log(20, '[fn]Indicator.load_sources(). source:{0[0]} invalid', log_args)
            return
        df = loader(ts_code=ts_code, nrows=nrows)
        assert isinstance(df, pd.DataFrame)
        return df

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
        if isinstance(self.df_idt, pd.DataFrame):
            df_source_head = self.load_sources(nrows=1)
            source_date = df_source_head.index[0]
            idt_date = self.df_idt.head(1).index
            if idt_date == source_date:
                return True
            else:
                log_args = [self.ts_code, self.idt_name, idt_date, source_date]
                add_log(40, '[fn]Indicator.valid_utd() ts_code:{0[0]} {0[1]} not uptodate. idt:{0[2]} source:{0[3]}', log_args)
        else:
            log_args = [self.ts_code, self.idt_name]
            add_log(40, '[fn]Indicator.valid_utd() ts_code:{0[0]} {0[1]} not loaded', log_args)

    def calc_idt(self):
        """
        调用self._calc_res()补完df_idt数据
        指标的值会根据已下载的基础数据计算补完到可能的最新值；但不会触发基础数据的补完下载
        """
        from st_board import valid_file_path
        df_append = self._calc_res()
        if isinstance(df_append, pd.DataFrame):
            if isinstance(self.df_idt, pd.DataFrame):
                _frames = [df_append, self.df_idt]
                df_idt = pd.concat(_frames, sort=False)
            else:
                df_idt = df_append
            if self.update_csv:
                if valid_file_path(self.file_path):
                    df_idt.to_csv(self.file_path)
                    log_args = [self.ts_code, self.file_path]
                    add_log(40, "[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, saved", log_args)
                else:
                    log_args = [self.ts_code, self.file_path]
                    add_log(20, "[fn]Indicator.calc_idt() ts_code:{0[0]}, file_path:{0[1]}, invalid", log_args)
            self.df_idt = df_idt
            log_args = [self.ts_code, str(self.file_name)[:-4], len(df_idt)]
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
    #         if self.source != 'close':
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
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Ma> if valid; None if invalid 
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Ma.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'ma'
        self.period = period
        # print("[L97] 补period类型异常")
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
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
                df_source.drop(df_source.index[idt_head_in_source + period - 1:], inplace=True)  # 根据period计算保留哪些df_source记录用于计算
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
            else:
                log_args = [self.ts_code]
                add_log(20, '[fn]Ma._calc_res() ts_code:{0[0]}; idt_head unknown value', log_args)
                return
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

    @staticmethod
    def idt_bare(sr_data, period):
        """
        ma计算
        sr_data: <pd.Series> 原数据，从旧到新排列；
        period: <int> 周期数
        return: <pd.Series> 从新到旧排列
        """
        if isinstance(sr_data, pd.Series):
            sr_mean = sr_data.rolling(period).mean()
            sr_mean.dropna(inplace=True)
            sr_mean.sort_index(ascending=False, inplace=True)
            return sr_mean
        else:
            log_args = [type(sr_data)]
            add_log(20, '[fn]Ma.idt_bare() sr_data type:{0[0]} is not pd.Series', log_args)


class Ema(Indicator):
    """
    指数移动平均线
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
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

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'ema'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
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
            add_log(20, '[fn]Ema.idt_bare() sr_data type:{0[0]} is not pd.Series', log_args)


class Macd(Indicator):
    """
    MACD
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, long_n1=26, short_n2=12, dea_n3=9, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Macd> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Macd.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        # long_n3 = int(long_n3)
        # short_n2 = int(short_n2)
        # dea_n3 = int(dea_n3)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, long_n1=26, short_n2=12, dea_n3=9, source='close', reload=False, update_csv=True, subtype='D'):
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
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
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
            # idt_ema_long
            if hasattr(parent, idt_ema_long_name):
                idt_ema_long = getattr(parent, idt_ema_long_name)
                if idt_ema_long.valid_utd() is not True:
                    idt_ema_long.calc_idt()
            else:
                parent.add_indicator(**kwargs_long)
                idt_ema_long = getattr(parent, idt_ema_long_name)

            # idt_ema_short
            if hasattr(parent, idt_ema_short_name):
                idt_ema_short = getattr(parent, idt_ema_short_name)
                if idt_ema_short.valid_utd() is not True:
                    idt_ema_short.calc_idt()
            else:
                parent.add_indicator(**kwargs_short)
                idt_ema_short = getattr(parent, idt_ema_short_name)

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
                last_dea = self.df_idt.iloc[0]['MACD']
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        # sr_diff_append = pd.Series()
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
    #     if self.source != 'close':
    #         idt_name = idt_name + '_' + self.source
    #     if self.subtype.lower() != 'd':
    #         idt_name = idt_name + '_' + self.subtype.lower()
    #     idt_name = idt_name + '_' + str(self.long_n3) + '_' + str(self.short_n2) + '_' + str(self.dea_n3)
    #     return idt_name


class Majh(Indicator):
    """
    移动平均线ma的纠结程度
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, long_n3=30, medium_n2=10, short_n1=5, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins majh> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Majh.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, long_n3=30, medium_n2=10, short_n1=5, source='close', reload=False, update_csv=True, subtype='D'):
        """
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'majh'
        self.long_n3 = long_n3
        self.medium_n2 = medium_n2
        self.short_n1 = short_n1
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """

        df_source = self.load_sources()
        df_idt = self.load_idt()
        long_n3 = self.long_n3
        medium_n2 = self.medium_n2
        short_n1 = self.short_n1
        parent = self.par_asset()
        df_ma_long = None  # 前置idt
        df_ma_middle = None  # 前置idt
        df_ma_short = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_ma_long, , df_ma_middle, df_ma_short
            """
            nonlocal df_ma_long, df_ma_middle, df_ma_short
            # 前置指标名idt_name计算
            _kwargs = {
                        'idt_type': 'ma',
                        'period': long_n3,
                        'source': self.source,
                        'update_csv': False}
            kwargs_long = idt_name(_kwargs)
            idt_ma_long_name = kwargs_long['idt_name']
            _kwargs = {
                        'idt_type': 'ma',
                        'period': medium_n2,
                        'source': self.source,
                        'update_csv': False}
            kwargs_middle = idt_name(_kwargs)
            idt_ma_middle_name = kwargs_middle['idt_name']
            _kwargs = {
                        'idt_type': 'ma',
                        'period': short_n1,
                        'source': self.source,
                        'update_csv': False}
            kwargs_short = idt_name(_kwargs)
            idt_ma_short_name = kwargs_short['idt_name']

            # ------valid pre-idts uptodate------
            # idt_ma_long
            if hasattr(parent, idt_ma_long_name):
                idt_ma_long = getattr(parent, idt_ma_long_name)
                if idt_ma_long.valid_utd() is not True:
                    idt_ma_long.calc_idt()
            else:
                parent.add_indicator(**kwargs_long)
                idt_ma_long = getattr(parent, idt_ma_long_name)

            # idt_ma_middle
            if hasattr(parent, idt_ma_middle_name):
                idt_ma_middle = getattr(parent, idt_ma_middle_name)
                if idt_ma_middle.valid_utd() is not True:
                    idt_ma_middle.calc_idt()
            else:
                parent.add_indicator(**kwargs_middle)
                idt_ma_middle = getattr(parent, idt_ma_middle_name)

            # idt_ma_short
            if hasattr(parent, idt_ma_short_name):
                idt_ma_short = getattr(parent, idt_ma_short_name)
                if idt_ma_short.valid_utd() is not True:
                    idt_ma_short.calc_idt()
            else:
                parent.add_indicator(**kwargs_short)
                idt_ma_short = getattr(parent, idt_ma_short_name)

            if n is None:
                df_ma_long = idt_ma_long.df_idt
                df_ma_middle = idt_ma_middle.df_idt
                df_ma_short = idt_ma_short.df_idt
            else:
                df_ma_long = idt_ma_long.df_idt.head(n)
                df_ma_middle = idt_ma_middle.df_idt.head(n)
                df_ma_short = idt_ma_short.df_idt.head(n)

        # ---------主要部分开始------------
        # last_majh = None
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Majh._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Majh._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source)  # 前置指标处理，保留新增记录
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        short_col_name = 'MA'
        middle_col_name = 'MA'
        long_col_name = 'MA'

        sr_dif_long_middle = (df_ma_long[long_col_name] - df_ma_middle[middle_col_name]).abs()
        sr_dif_long_short = (df_ma_long[long_col_name] - df_ma_short[short_col_name]).abs()
        sr_dif_middle_short = (df_ma_middle[middle_col_name] - df_ma_short[short_col_name]).abs()
        sr_append = (sr_dif_long_middle + sr_dif_long_short + sr_dif_middle_short) / 3 / df_ma_short[short_col_name] * 100

        sr_append.dropna(inplace=True)
        sr_append.sort_index(ascending=False, inplace=True)
        df_append = sr_append.to_frame(name="MAJH")
        return df_append


class Maqs(Indicator):
    """
    MAQS: MA的变化率，也就是它校前值的变化率，或理解为ma的切线斜率
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Maqs> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Maqs.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period: <int> MA的周期
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'maqs'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        parent = self.par_asset()
        df_ma = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_ma
            """
            nonlocal df_ma
            # 前置指标名idt_name计算
            _kwargs = {
                        'idt_type': 'ma',
                        'period': period,
                        'source': self.source,
                        'update_csv': True}  # 考虑到此ma会是常用指标
            kwargs_qz = idt_name(_kwargs)
            idt_ma_name = kwargs_qz['idt_name']

            # ------valid pre-idts uptodate------
            if hasattr(parent, idt_ma_name):
                idt_ma = getattr(parent, idt_ma_name)
                if idt_ma.valid_utd() is not True:
                    idt_ma.calc_idt()
            else:
                parent.add_indicator(**kwargs_qz)
                idt_ma = getattr(parent, idt_ma_name)
            if n is None:
                df_ma = idt_ma.df_idt
            else:
                df_ma = idt_ma.df_idt.head(n)

        # ---------主要部分开始------------
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Maqs._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Maqs._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source + 1)  # 注意根据算法和周期保留多少条
                # last_dea = self.df_idt.iloc[0]['MAQS']
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        ma_col_name = 'MA'  # 前置指标csv的字段名
        sr_append = df_ma[ma_col_name].pct_change(periods=-1)  # <Series> maqs序列
        sr_append = sr_append[:-1]  # 去掉最后一条
        idt_column_name = 'MAQS'
        df_idt_append = sr_append.to_frame(name=idt_column_name)
        return df_idt_append


class Emaqs(Indicator):
    """
    EMAQS: EMA的变化率，也就是它校前值的变化率
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Emaqs> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Emaqs.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period: <int> EMA的周期
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'emaqs'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        parent = self.par_asset()
        df_ema = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_ema
            """
            nonlocal df_ema
            # 前置指标名idt_name计算
            _kwargs = {
                        'idt_type': 'ema',
                        'period': period,
                        'source': self.source,
                        'update_csv': True}  # 考虑到此ema会是常用指标
            kwargs_qz = idt_name(_kwargs)
            idt_ema_name = kwargs_qz['idt_name']

            # ------valid pre-idts uptodate------
            if hasattr(parent, idt_ema_name):
                idt_ema = getattr(parent, idt_ema_name)
                if idt_ema.valid_utd() is not True:
                    idt_ema.calc_idt()
            else:
                parent.add_indicator(**kwargs_qz)
                idt_ema = getattr(parent, idt_ema_name)
            if n is None:
                df_ema = idt_ema.df_idt
            else:
                df_ema = idt_ema.df_idt.head(n)

        # ---------主要部分开始------------
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Emaqs._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Emaqs._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source + 1)  # 注意根据算法和周期保留多少条
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        ema_col_name = 'EMA'  # 前置指标csv的字段名
        sr_append = df_ema[ema_col_name].pct_change(periods=-1)  # <Series> emaqs序列
        sr_append = sr_append[:-1]  # 去掉最后一条
        idt_column_name = 'EMAQS'
        df_idt_append = sr_append.to_frame(name=idt_column_name)
        return df_idt_append


class Madq(Indicator):
    """
    MA的n周期抵扣，用n个当前价抵扣 MA 周期里最老的n个值，计算ma
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, dq_n1, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Madq> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Madq.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        if dq_n1 >= period:
            log_args = [ts_code, period, dq_n1]
            add_log(10, '[fn]Madq.__new__() ts_code:{0[0]}; dq_n1:{0[2]} must < period:{0[1]}; instance not created', log_args)
            return
        # period = int(period)
        # dq_n1 = int(dq_n1)
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, dq_n1, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period:<int> 周期数
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        dq_n1: <int> 抵扣掉的周期数，要远小于period
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'madq'
        self.period = period
        self.dq_n1 = dq_n1
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        dq_n1 = self.dq_n1
        if isinstance(df_idt, pd.DataFrame):
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Madq._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Madq._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                df_source.drop(df_source.index[idt_head_in_source + period - 1 - dq_n1:], inplace=True)  # 根据period和dq_n1计算保留哪些df_source记录用于计算
                values = []
                rvs_rslt = []
                try:
                    source_column_name = SOURCE_TO_COLUMN[self.source]
                except KeyError:
                    log_args = [self.ts_code, self.source]
                    add_log(20, '[fn]Madq._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                    return
                for idx in reversed(df_source.index):
                    v = df_source[source_column_name][idx]
                    values.append(v)
                    if len(values) > period - dq_n1:  # 最近罗干天的值
                        del values[0]
                    if len(values) == period - dq_n1:  # 注意修改
                        dq_values = values.copy()
                        for _ in range(dq_n1):
                            dq_values.append(v)
                        rvs_rslt.append(np.average(dq_values))
        else:  # .csv file not exist
            try:
                source_column_name = SOURCE_TO_COLUMN[self.source]
            except KeyError:
                log_args = [self.ts_code, self.source]
                add_log(20, '[fn]Madq._calc_res() ts_code:{0[0]}; source:{0[1]} not valid', log_args)
                return
            values = []
            rvs_rslt = []
            for idx in reversed(df_source.index):
                v = df_source[source_column_name][idx]
                values.append(v)
                if len(values) > period - dq_n1:  # 最近罗干天的值
                    del values[0]
                if len(values) == period - dq_n1:  # 注意修改
                    dq_values = values.copy()
                    for _ in range(dq_n1):
                        dq_values.append(v)
                    rvs_rslt.append(np.average(dq_values))
        iter_rslt = reversed(rvs_rslt)
        rslt = list(iter_rslt)
        index_source = df_source.index[:len(df_source) - period + dq_n1 + 1]  # 注意修改
        idt_column_name = 'MADQ'
        data = {idt_column_name: rslt}
        df_idt_append = pd.DataFrame(data, index=index_source)
        return df_idt_append


class Jdxz(Indicator):
    """
    JDXZ: 绝对吸引资金比，成交额 / 两市总成交额
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='amount', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins jdxz> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Jdxz.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='close', reload=False, update_csv=True, subtype='D'):
        """
        period: <int> 周期数
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'jdxz'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        from st_board import Index
        dfsr_source = self.load_sources()  # 可能是<sr>或<df>
        df_idt = self.load_idt()
        period = self.period
        parent = self.par_asset()

        # ---------基础数据---------
        shzs = Index('000001.SH')  # 上证指数
        szzs = Index('399001.SZ')  # 深证成指
        sr_shzs_amt = shzs.daily_data['amount']
        sr_szzs_amt = szzs.daily_data['amount']
        sr_amt = parent.daily_data['amount']

        # ---------主要部分开始------------
        if isinstance(df_idt, pd.DataFrame):
            if len(df_idt) > 0:
                idt_head_index_str, = df_idt.head(1).index.values
                try:
                    idt_head_in_source = dfsr_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
                except KeyError:
                    log_args = [self.ts_code]
                    add_log(20, '[fn]Jdxz._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                    return
                if idt_head_in_source == 0:
                    log_args = [self.ts_code]
                    add_log(40, '[fn]Jdxz._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                    return
                elif idt_head_in_source > 0:
                    dfsr_source.drop(dfsr_source.index[idt_head_in_source + period - 1:], inplace=True)  # 根据period计算保留哪些df_source记录用于计算
                    sr_amt.drop(sr_amt.index[idt_head_in_source + period - 1:], inplace=True)  # 根据period计算保留哪些记录用于计算
                    # values = []
                    # rvs_rslt = []
                    # source_column_name = self.source  # 此处以之前的indicator不同
                    # for idx in reversed(df_source.index):
                    #     values.append(df_source[source_column_name][idx])
                    #     if len(values) > period:
                    #         del values[0]
                    #     if len(values) == period:
                    #         rvs_rslt.append(np.average(values))
                else:
                    log_args = [self.ts_code]
                    add_log(20, '[fn]Jdxz._calc_res() ts_code:{0[0]}; idt_head unknown value', log_args)
                    return
        # else:  # .csv file not exist
        #     values = []
        #     rvs_rslt = []
        #     source_column_name = self.source  # 此处以之前的indicator不同
        #     for idx in reversed(dfsr_source.index):
        #         values.append(df_source[source_column_name][idx])
        #         if len(values) > period:
        #             del values[0]
        #         if len(values) == period:
        #             rvs_rslt.append(np.average(values))
        # iter_rslt = reversed(rvs_rslt)
        # rslt = list(iter_rslt)
        sr_jdxz = sr_amt / (sr_shzs_amt + sr_szzs_amt) * 10000
        sr_jdxz.dropna(inplace=True)
        sr_mean = Ma.idt_bare(sr_jdxz, period)
        df_idt_append = sr_mean.to_frame(name='JDXZ')
        return df_idt_append


class Jdxzqs(Indicator):
    """
    Jdxzqs: JDXZ的变化率
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, period, source='amount', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE
        return:<ins Jdxzqs> if valid; None if invalid
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Jdxzqs.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, period, source='amount', reload=False, update_csv=True, subtype='D'):
        """
        period: <int> Jdxz的周期
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'jdxzqs'
        self.period = period
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        period = self.period
        parent = self.par_asset()
        df_pre = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_pre
            """
            nonlocal df_pre
            # 前置指标名idt_name计算
            _kwargs = {
                        'idt_type': 'jdxz',
                        'period': period,
                        'source': self.source,
                        'update_csv': True}  # 考虑此指标是否会是常用指标，平衡速度与硬盘
            kwargs_qz = idt_name(_kwargs)
            idt_pre_name = kwargs_qz['idt_name']

            # ------valid pre-idts uptodate------
            if hasattr(parent, idt_pre_name):
                idt_pre = getattr(parent, idt_pre_name)
                if idt_pre.valid_utd() is not True:
                    idt_pre.calc_idt()
            else:
                parent.add_indicator(**kwargs_qz)
                idt_pre = getattr(parent, idt_pre_name)
            if n is None:
                df_pre = idt_pre.df_idt
            else:
                df_pre = idt_pre.df_idt.head(n)

        # ---------主要部分开始------------
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Jdxzqs._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Jdxzqs._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source + 1)  # 注意根据算法和周期保留多少条
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        pre_col_name = 'JDXZ'  # 前置指标csv的字段名
        sr_append = df_pre[pre_col_name].pct_change(periods=-1)  # <Series> pre_qs序列
        sr_append = sr_append[:-1]  # 去掉最后一条
        idt_column_name = 'JDXZQS'
        df_idt_append = sr_append.to_frame(name=idt_column_name)
        return df_idt_append


class Dktp(Indicator):
    """
    多空头排列周期数，多头排列为正数，空头排列为负数
    """
    def __new__(cls, ts_code, par_asset, idt_type, idt_name, short_n1, medium_n2, long_n3, source='close', reload=False, update_csv=True, subtype='D'):
        """
        source:<str> e.g. 'close' #SOURCE

        return:<ins Dktp> >0 多头排列周期数
                          =0 散乱排列
                          <0 空头排列周期数
        """
        try:
            SUBTYPE[subtype]
        except KeyError:
            log_args = [ts_code, subtype]
            add_log(10, '[fn]Dktp.__new__() ts_code:{0[0]}; subtype:{0[1]} invalid; instance not created', log_args)
            return
        obj = super().__new__(cls, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type)
        return obj

    def __init__(self, ts_code, par_asset, idt_type, idt_name, short_n1, medium_n2, long_n3, source='close', reload=False, update_csv=True, subtype='D'):
        """
        subtype:<str> 'D'-Day; 'W'-Week; 'M'-Month #only 'D' yet
        short_n1, medium_n2, long_n3: <int>
        """
        Indicator.__init__(self, ts_code=ts_code, par_asset=par_asset, idt_type=idt_type, reload=reload, update_csv=update_csv)
        self.idt_type = 'dktp'
        self.short_n1 = short_n1
        self.medium_n2 = medium_n2
        self.long_n3 = long_n3
        self.source = source
        self.idt_name = idt_name
        self.file_name = PurePath('idt_' + ts_code + '_' + self.idt_name + '.csv')
        self.file_path = sub_path / sub_idt / self.file_name
        self.subtype = subtype

    def _calc_res(self):
        """
        计算idt_df要补完的数据
        """
        df_source = self.load_sources()
        df_idt = self.load_idt()
        short_n1 = self.short_n1
        medium_n2 = self.medium_n2
        long_n3 = self.long_n3
        parent = self.par_asset()
        df_ma_short = None  # 前置idt
        df_ma_medium = None  # 前置idt
        df_ma_long = None  # 前置idt

        def _pre_idt_hdl(n=None):
            """
            前置指标处理
            n:<int> keep first n records; None = keep all
            result: update df_ma_short, df_ma_medium, df_ma_long
            """
            nonlocal df_ma_short, df_ma_medium, df_ma_long
            # 前置指标名idt_name计算
            _kwargs = {
                'idt_type': 'ma',
                'period': short_n1,
                'source': self.source,
                'update_csv': True}
            kwargs_short = idt_name(_kwargs)
            idt_ma_short_name = kwargs_short['idt_name']

            _kwargs = {
                'idt_type': 'ma',
                'period': medium_n2,
                'source': self.source,
                'update_csv': True}
            kwargs_medium = idt_name(_kwargs)
            idt_ma_medium_name = kwargs_medium['idt_name']

            _kwargs = {
                'idt_type': 'ma',
                'period': long_n3,
                'source': self.source,
                'update_csv': True}
            kwargs_long = idt_name(_kwargs)
            idt_ma_long_name = kwargs_long['idt_name']

            # ------valid pre-idts uptodate------
            # idt_ma_short
            if hasattr(parent, idt_ma_short_name):
                idt_ma_short = getattr(parent, idt_ma_short_name)
                if idt_ma_short.valid_utd() is not True:
                    idt_ma_short.calc_idt()

            else:
                parent.add_indicator(**kwargs_short)
                idt_ma_short = getattr(parent, idt_ma_short_name)

            # idt_ma_medium
            if hasattr(parent, idt_ma_medium_name):
                idt_ma_medium = getattr(parent, idt_ma_medium_name)
                if idt_ma_medium.valid_utd() is not True:
                    idt_ma_medium.calc_idt()
            else:
                parent.add_indicator(**kwargs_medium)
                idt_ma_medium = getattr(parent, idt_ma_medium_name)

            # idt_ma_long
            if hasattr(parent, idt_ma_long_name):
                idt_ma_long = getattr(parent, idt_ma_long_name)
                if idt_ma_long.valid_utd() is not True:
                    idt_ma_long.calc_idt()
            else:
                parent.add_indicator(**kwargs_long)
                idt_ma_long = getattr(parent, idt_ma_long_name)

            # 获取df
            if n is None:
                df_ma_short = idt_ma_short.df_idt
                df_ma_medium = idt_ma_medium.df_idt
                df_ma_long = idt_ma_long.df_idt
            else:
                df_ma_short = idt_ma_short.df_idt.head(n)
                df_ma_medium = idt_ma_medium.df_idt.head(n)
                df_ma_long = idt_ma_long.df_idt.head(n)

        # ---------主要部分开始------------
        last_dktp = None  # 前次更新指标的最后一个指标值
        if isinstance(df_idt, pd.DataFrame):
            # ------df_idt有效-----
            idt_head_index_str, = df_idt.head(1).index.values
            try:
                idt_head_in_source = df_source.index.get_loc(idt_head_index_str)  # idt head position in df_source
            except KeyError:
                log_args = [self.ts_code]
                add_log(20, '[fn]Dktp._calc_res() ts_code:{0[0]}; idt_head not found in df_source', log_args)
                return
            if idt_head_in_source == 0:
                log_args = [self.ts_code]
                add_log(40, '[fn]Dktp._calc_res() ts_code:{0[0]}; idt_head up to source date, no need to update', log_args)
                return
            elif idt_head_in_source > 0:
                _pre_idt_hdl(idt_head_in_source)  # 前置指标处理
                last_dktp = self.df_idt.iloc[0]['DKTP']
        else:
            # ----------重头生成df_idt----------
            _pre_idt_hdl()
        short_col_name = 'MA'
        medium_col_name = 'MA'
        long_col_name = 'MA'

        if not isinstance(df_ma_long, pd.DataFrame):
            log_args = [self.ts_code]
            add_log(20, '[fn]Dktp._calc_res() ts_code:{0[0]}; df_ma_long is not available', log_args)
            return
        if len(df_ma_long) < 1:
            log_args = [self.ts_code]
            add_log(20, '[fn]Dktp._calc_res() ts_code:{0[0]}; df_ma_long is empty', log_args)
            return
        df_ma_long = df_ma_long.copy()  # deepcopy
        df_ma_long.loc[:, 'DKTP'] = np.nan
        for idx in reversed(df_ma_long.index):
            short = df_ma_short.MA[idx]
            medium = df_ma_medium.MA[idx]
            long = df_ma_long.MA[idx]

            if (short > medium) and (medium > long):  # 多头排列
                if (last_dktp is None) or (last_dktp <= 0):
                    df_ma_long.loc[idx].DKTP = 1
                elif last_dktp > 0:
                    df_ma_long.loc[idx].DKTP = last_dktp + 1
            elif (short < medium) and (medium < long):  # 空头排列
                if (last_dktp is None) or (last_dktp >= 0):
                    df_ma_long.loc[idx].DKTP = -1
                elif last_dktp < 0:
                    df_ma_long.loc[idx].DKTP = last_dktp - 1
            else:
                df_ma_long.loc[idx].DKTP = 0
            last_dktp = df_ma_long.DKTP[idx]

        df_ma_long.drop(['MA'], axis=1, inplace=True)
        df_append = df_ma_long

        return df_append


IDT_CLASS = {'ma': Ma,
             'maqs': Maqs,  # ma的趋势变化率
             'madq': Madq,  # ma抵扣x周期
             'ema': Ema,
             'emaqs': Emaqs,  # ema的趋势变化率
             'macd': Macd,
             'majh': Majh,
             'jdxz': Jdxz,  # 吸引资金绝对占比、成交额占两市比例
             'jdxzqs': Jdxzqs,  # jdxz的趋势变化率
             'dktp': Dktp,  # 多空头排列周期数
             }

if __name__ == '__main__':
    start_time = datetime.now()
    # raw_data_init()
    from st_board import Stock, Index
    # global raw_data
    # ------------------test---------------------

    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
