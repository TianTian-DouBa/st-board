"""
爬虫，抓取网页的数据
"""
import requests
import re
from XF_LOG_MANAGE import add_log
from pathlib import PurePath, Path
import pandas as pd
import os

# 改变当前目录导入raw_data
original_dir = os.getcwd()
assert original_dir[-6:] == 'spider'
parent_dir = original_dir[:-7]
# print(parent_dir)
os.chdir(parent_dir)
from st_common import raw_data
os.chdir(original_dir)


def get_518880_net():
    """
    通过华安基金官网获取 518880 华安黄金易ETF 的最新每股净值（前一交易日）
    return: (<str>trade_date, <float>net, <float>change)
    """
    url = 'http://www.huaan.com.cn/funds/518880/index.shtml'
    try:
        html = requests.get(url)
    except Exception as e:
        log_args = [type(e), e]
        add_log(20, '[fn]get_518880_net(). except type: {0[0]}, {0[1]}', log_args)
        return
    html.encoding = 'utf-8'
    text = html.text
    # print(text)

    # ----验证获取的基金代码是否为518880
    pattern = re.compile(r'<li>基金代码：  <span class="f_333 f_16">.*</span></li>')  # 忽略格式报错
    match = pattern.search(text)
    if match:
        code = match.group()[36: -12]
    else:
        code = None
    if code != '518880':
        log_args = [code]
        add_log(20, '[fn]get_518880_net(). returned code:{} mismatch. website may have updated', log_args)
        return

    # ----获取净值net, 变化chang, 净值日期trade_date
    pattern_str = r'<li>最新净值：  <span class="f_red20">[\s\S]*</span> 元'
    pattern = re.compile(pattern_str)
    match = pattern.search(text)
    if match:
        net = match.group()[33: -9].strip()
        net = float(net)
    else:
        net = None

    pct_change = None
    # 升情况
    pattern_str = r'<span class="f_red">.*</span> <img src="/images/global/icon_sheng2.gif"'
    pattern = re.compile(pattern_str)
    match = pattern.search(text)
    if match:
        change = match.group()[20: -49].strip()
        print('[L54] change:{}'.format(change))
        value = float(change[1: -1])
        if change[0] == '+':
            pct_change = value / 100
        elif change[0] == '-':
            pct_change = - value / 100
        else:
            log_args = [change]
            add_log(20, '[fn]get_518880_net(). the format of change:{} not recognized.', log_args)
            pct_change = None
    # 降情况
    pattern_str = r'<span class="f_green">.*</span> <img src="/images/global/icon_jiang2.gif"'
    pattern = re.compile(pattern_str)
    match = pattern.search(text)
    if match:
        change = match.group()[22: -49].strip()
        # print('[L54] change:{}'.format(change))
        value = float(change[1: -1])
        if change[0] == '+':
            pct_change = value / 100
        elif change[0] == '-':
            pct_change = - value / 100
        else:
            log_args = [change]
            add_log(20, '[fn]get_518880_net(). the format of change:{} not recognized.', log_args)
            pct_change = None

    pattern_str = r'(净值日期：.*)'
    pattern = re.compile(pattern_str)
    match = pattern.search(text)
    if match:
        trade_date = match.group()[5: -3].replace('-', '')
    else:
        trade_date = None

    return trade_date, net, pct_change


def load_net_df(ts_code, nrows=None):
    """
    从./st_board/data_csv/daily_data/net_xxxxxx.etf.csv中读取净值数据到df
    ts_code: <str> e.g. "518880.etf"
    return: <df>
    """
    ts_code = ts_code.upper()
    this_folder = Path.cwd()
    parent_folder = this_folder.parent
    file_name = PurePath('net_' + str(ts_code) + '.csv')
    file_path = parent_folder / 'data_csv' / 'daily_data' / file_name
    try:
        df = pd.read_csv(file_path, dtype={'trade_date': str}, index_col='trade_date', nrows=nrows)
    except FileNotFoundError:
        log_args = [file_name]
        add_log(20, '[fn]load_net_df() ts_code:{} file not exist', log_args)
        return
    return df


def update_net_csv(ts_code, record):
    """
    将record的数据增加到ts_code的文件中
    ts_code: <str> e.g. 518880.etf
    record: <tuple> e.g. (<str>trade_date, <float>net, <float>pct_change)
    """
    ts_code = ts_code.upper()
    this_folder = Path.cwd()
    parent_folder = this_folder.parent
    file_name = PurePath('net_' + str(ts_code) + '.csv')
    file_path = parent_folder / 'data_csv' / 'daily_data' / file_name
    # print(file_path)
    exist = file_path.exists()
    if exist:  # 文件存在，load文件到df
        df = load_net_df(ts_code)
        last_date = str(df.index[0])
    else:
        df = pd.DataFrame(columns=['trade_date', 'net', 'pct_change'])
        df.set_index('trade_date', inplace=True)
        last_date = None
    if last_date == record[0]:
        log_args = [last_date]
        add_log(40, '[fn]:update_net_csv() record {0[0]} duplicated in df', log_args)
        return
    # print('[L132] trade_date type:{} value:{}'.format(type(record[0]), record[0]))
    row = pd.DataFrame({'trade_date': [record[0]], 'net': [record[1]], 'pct_change': [record[2]]})
    row.set_index('trade_date', inplace=True)
    print(row)
    df = pd.concat([row, df])
    df.to_csv(file_path, encoding='utf-8')
    log_args = [file_name]
    add_log(40, '[fn]:update_net_csv() {0[0]} exported', log_args)


def get_sh_gold(ts_code='AU9999.SH', trade_date=None):
    """
    通过nowapi获取上海交易所黄金历史数据
    ts_code: <str> e.g. 'au9999.sh'
    trade_date: None 返回含当日的最近交易日
                <str> e.g. '20200428'
    return: <tuple> of (trade_date_, ts_code, open_, high_, low_, close_, pre_close_, change_, pct_chg_, vol_, amount_, average_)
    """
    APPKEY = '50875'
    SIGN = 'fe1bd5728b343d7003575fe1578a95d1'
    GOLDID = {'AU_TD.SH': '1051',
              'AG_TD.SH': '1052',
              'AU9999.SH': '1053',
              'AU9995.SH': '1054',
              'PT9995.SH': '1056',  # 1058:金条100g, 1059:黄金T+N1, 1060:黄金T+N2, 1080:iAu9999, 1081:mAuT+D 未配置
              }
    url = r'http://api.k780.com'
    ts_code = ts_code.upper()
    try:
        goldid = GOLDID[ts_code]
    except KeyError:
        log_args = [ts_code]
        add_log(20, '[fn]get_sh_gold() ts_code:{} not recognized', log_args)
        return

    # 获取最新的交易日
    if trade_date is None:
        trade_date = raw_data.last_trade_day()
        if trade_date is None:
            add_log(20, '[fn]get_sh_gold() failed to get the last_trade_day, aborted')
            return

    params = {
        'app': 'finance.shgold_history',
        'goldid': goldid,
        'date': trade_date,
        'appkey': APPKEY,
        'sign': SIGN,
        'format': 'json',
    }

    r = requests.get(url, params=params)
    rslt = r.json()  # rslt is <dict>
    # print('[L193] r.json():{}'.format(rslt))

    if rslt:
        if rslt['success'] != '0':  # success
            data = rslt['result']['lists'][0]
            # print('[L198] data:{}'.format(data))
            trade_date_ = data['days'].replace('-', '')
            high_ = float(data['high_price'])
            low_ = float(data['low_price'])
            open_ = float(data['open_price'])
            close_ = float(data['yesy_price'])  # 元/克
            change_ = float(data['rise_fall'])
            pre_close_ = close_ - change_
            pct_chg_ = change_ / pre_close_
            average_ = float(data['vwap_price'])  # 成交加权平均价
            vol_ = float(data['volume'])  # kg
            amount_ = float(data['turn_volume']) /1000 # 千元
            # average_, volume, amount_的官网原始数据就不平，原因不明
            return trade_date_, ts_code, open_, high_, low_, close_, pre_close_, change_, pct_chg_, vol_, amount_, average_
        else:
            log_args = [rslt['msgid'], rslt['msg'], trade_date]
            add_log(20, '[fn]get_sh_gold() {0[2]} failed msg_id:{0[0]}; msg:{0[1]}', log_args)
            return
    else:
        add_log(20, '[fn]get_sh_gold() failed to get the result from api')
        return


def load_daily_df(ts_code, nrows=None):
    """
    从./st_board/data_csv/daily_data/d_<ts_code>.csv; 只适合位置在st_board下一级目录中用的.py文件调用
    ts_code: <str> e.g. "AU9999.SH"
    nrows: <int> 读头几条记录
    return: <df>
    """
    ts_code = ts_code.upper()
    this_folder = Path.cwd()
    parent_folder = this_folder.parent
    file_name = PurePath('d_' + str(ts_code) + '.csv')
    file_path = parent_folder / 'data_csv' / 'daily_data' / file_name
    try:
        df = pd.read_csv(file_path, dtype={'trade_date': str}, index_col='trade_date', nrows=nrows)
    except FileNotFoundError:
        log_args = [file_name]
        add_log(20, '[fn]load_daily_df() ts_code:{0[0]} file not exist', log_args)
        return
    return df


def update_gold_df(ts_code, start_date=None, end_date=None, reload=None):
    """
    通过重复调用get_sh_gold()获取数据，拼接更新到./st_board/data_csv/daily_data/d_<ts_code>.csv文件
    ts_code: <str> e.g. "AU9999.SH"
    start_date: None 接着现存的csv中的最近日期继续
                <str> e.g. "20200429"， 如果时间早于.csv的最新时间则忽略，继续csv时间做
    end_date: None 循环直到最近的交易日
              <str> e.g. "20200429"
    reload: True 重新生成.csv文件
    """
    # 暂存由get_sh_gold()获取的数据块结构
    l_trade_date = []
    l_ts_code = []
    l_open = []
    l_high = []
    l_low = []
    l_close = []
    l_pre_close = []
    l_change = []
    l_pct_chg = []
    l_vol = []
    l_amount = []
    l_average = []
    buff_dict = {
        'trade_date': l_trade_date,
        'ts_code': l_ts_code,
        'open': l_open,
        'high': l_high,
        'low': l_low,
        'close': l_close,
        'pre_close': l_pre_close,
        'change': l_change,
        'pct_chg': l_pct_chg,
        'vol': l_vol,
        'amount': l_amount,
        'average': l_average
    }
    setl = (l_trade_date, l_ts_code, l_open, l_high, l_low, l_close, l_pre_close,
            l_change, l_pct_chg, l_vol, l_amount, l_average)
    setn = len(setl)
    def _reset_lists():
        nonlocal l_trade_date
        nonlocal l_ts_code
        nonlocal l_open
        nonlocal l_high
        nonlocal l_low
        nonlocal l_close
        nonlocal l_pre_close
        nonlocal l_change
        nonlocal l_pct_chg
        nonlocal l_vol
        nonlocal l_amount
        nonlocal l_average
        l_trade_date = []
        l_ts_code = []
        l_open = []
        l_high = []
        l_low = []
        l_close = []
        l_pre_close = []
        l_change = []
        l_pct_chg = []
        l_vol = []
        l_amount = []
        l_average = []

    ts_code = ts_code.upper()
    this_folder = Path.cwd()
    parent_folder = this_folder.parent
    file_name = PurePath('d_' + str(ts_code) + '.csv')
    file_path = parent_folder / 'data_csv' / 'daily_data' / file_name
    # print(file_path)
    exist = file_path.exists()
    df = None
    if exist and (reload is not True):  # 文件存在，load文件到df
        df = load_daily_df(ts_code)
        try:
            last_date = str(df.index[0])  # 空<df>的情况还为处理
        except IndexError:
            log_args = [file_name]
            add_log(10, '[fn]update_gold_df() {0[1]} empty, need to delete it', log_args)
        if start_date is None:
            _start = raw_data.next_trade_day(last_date)
        else:
            if int(start_date) < int(last_date):
                _start = raw_data.next_trade_day(last_date)
            else:
                _start = raw_data.next_trade_day(start_date)
    else:
        if start_date is None:
            start_date = "20161031"  # nowapi数据开始时间
        _start = raw_data.next_trade_day(start_date)

    # 检查_start有效性
    if _start is None:
        log_args = [ts_code]
        add_log(20, '[fn]update_gold_df() ts_code:{0[0]} invalid start date', log_args)
        return

    if end_date is None:
        _end = raw_data.last_trade_day()
    else:
        _end = raw_data.last_trade_day(end_date)

    # 检查_end有效性
    if _end is None:
        log_args = [ts_code, end_date]
        add_log(20, '[fn]update_gold_df() ts_code:{0[0]} invalid end_date:{0[1]}', log_args)
        return
    elif int(_end) < int(_start):
        log_args = [ts_code, _end, _start]
        add_log(30, '[fn]update_gold_df() ts_code:{0[0]} end_date:{0[1]} before start_date:{0[2]}', log_args)
        return

    # 重头创建df
    COLUMNS = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close',
               'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'average']
    if df is None:
        df = pd.DataFrame(columns=COLUMNS)
        df.set_index('trade_date', inplace=True)

    int_end = int(_end)
    by_date = _start
    _reset_lists()
    REPEAT_I = 100  # 内循环的次数，满则合并一波
    i = 0
    while int(by_date) <= int_end:
        if i >= REPEAT_I:  # 合并一批，随后清零
            df_buff = pd.DataFrame(buff_dict)
            df_buff.set_index('trade_date', inplace=True)
            df = pd.concat([df_buff, df])
            df.to_csv(file_path, encoding='utf-8')
            _reset_lists()
            i = 0
        record = get_sh_gold(ts_code=ts_code, trade_date=by_date)
        if record is not None:
            for n in range(setn):
                setl[n].insert(0, record[n])
        i += 1
        by_date = raw_data.next_trade_day(by_date)
    df_buff = pd.DataFrame(buff_dict)
    df_buff.set_index('trade_date', inplace=True)
    df = pd.concat([df_buff, df])
    df.to_csv(file_path, encoding='utf-8')
    log_args = [file_name, len(df)]
    add_log(40, '[fn]update_gold_df() {0[0]} updated, items:{0[1]}', log_args)
    _reset_lists()


if __name__ == '__main__':
    # data = get_518880_net()
    # update_net_csv('518880.etf', data)
    # rslt = get_sh_gold() # trade_date="20200427"
    # print(rslt)
    # update_gold_df('AU9999.SH', start_date='20161031', end_date='20171031', reload=None)
    update_gold_df('AU9999.SH')
    pass
