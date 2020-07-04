from datetime import datetime
from st_common import  Fund_Basic
from st_board import All_Assets_List, Concept, Hsgt, bulk_download, bulk_dl_appendix, bulk_calc_dfq

if __name__ == "__main__":
    from st_common import Raw_Data
    # global raw_data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)
    funds = Fund_Basic()
    funds.load_fund_basic(market='O')
    e = funds.basic_o
    funds.get_fund_basic(market='O')
    g = e.groupby('management')
    g.groups
    for name, group in g:
        print('----' + name + '----')
        print(group)
    df_iv = g['issue_amount'].agg(['sum'])
    df_iv.sort_values(by='sum')  # DataFrame排序
    pass

