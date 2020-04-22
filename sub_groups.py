"""
测试用文件
"""
import tushare as ts
from pathlib import PurePath
from st_common import sub_path

ts.set_token('c42bfdc5a6b4d2b1078348ec201467dec594d3a75a4a276e650379dc')
ts_pro = ts.pro_api()


def get_sw_cfg(index_code):
    """
    获取申万成分股
    """
    file_name = PurePath('al_' + str(index_code) + '.csv')
    file_path = sub_path / 'assets_lists' / file_name
    # file_path = r'.\data_csv\assets_lists' + '\\' + file_name
    df = ts_pro.index_member(index_code=index_code)
    df.loc[:, "selected"] = 'T'
    df = df[['con_code', 'selected']]
    df.rename(columns={'con_code': 'ts_code'}, inplace=True)
    df.set_index("ts_code", inplace=True)
    print(df)
    df.to_csv(file_path, encoding='utf-8')
    

def get_index_cfg(index_code):
    """
    获取申万成分股
    """
    print('[L26] not complete')
    file_name = PurePath('al_' + str(index_code) + '.csv')
    file_path = sub_path / 'assets_lists' / file_name
    # file_path = r'.\data_csv\assets_lists' + '\\' + file_name
    df = ts_pro.index_weight(index_code=index_code)
    trade_date = df.head(1)['trade_date']
    print(trade_date)
    trade_date = int(trade_date)
    df = df[df.trade_date == trade_date]['con_code']
    df.to_csv(file_path, encoding="utf-8")
    

if __name__ == "__main__":
    # df = ts_pro.concept(src='ts')
    # df.to_csv('.\\data_csv\\概念列表.csv', encoding="utf-8")
    index_code = '801130.SI'
    get_sw_cfg(index_code)






