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
    this_folder = Path.cwd()
    parent_folder = this_folder.parent
    file_name = PurePath('net_' + str(ts_code) + '.csv')
    file_path = parent_folder / 'data_csv' / 'daily_data' / file_name
    # try
    df = pd.read_csv(file_path, dtype={'trade_date': str}, index_col='trade_date', nrows=nrows)
    return df


def update_net_csv(ts_code, record):
    """
    将sr_data的数据增加到ts_code的文件中
    ts_code: <str> e.g. 518880.etf
    record: <tuple> e.g. (<str>trade_date, <float>net, <float>pct_change)
    """
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


def get_sh_gold(ts_code='au9999.sh', trade_date=None):
    """
    通过nowapi获取上海交易所黄金历史数据
    ts_code: <str> e.g. 'au9999.sh'
    trade_date: None 返回含当日的最近交易日
                <str> e.g. '20200428'
    """
    APPKEY = '50875'
    SIGN = 'fe1bd5728b343d7003575fe1578a95d1'
    GOLDID = {'au_td.sh': '1051',
              'ag_td.sh': '1052',
              'au9999.sh': '1053',
              'au9995.sh': '1054',
              'pt9995.sh': '1056',  # 1058:金条100g, 1059:黄金T+N1, 1060:黄金T+N2, 1080:iAu9999, 1081:mAuT+D 未配置
              }
    url = r'http://api.k780.com'
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
    # r.encoding = 'utf-8'
    print(r.json())


if __name__ == '__main__':
    data = get_518880_net()
    update_net_csv('518880.etf', data)
    get_sh_gold()
