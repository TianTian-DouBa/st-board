from datetime import datetime
from st_board import All_Assets_List, Concept, Hsgt, bulk_download, bulk_dl_appendix, bulk_calc_dfq

if __name__ == "__main__":
    from st_common import Raw_Data
    # global raw_data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)

    # #------------------------更新基础数据-----------------------

    # 股票列表
    st_basic = raw_data.stock.get_stock_basic()
    if st_basic is not None:
        print('[msg] stock_basic.csv updated items: {}'.format(len(st_basic)))

    # 指数列表
    n_sse, n_szse, n_sw = raw_data.index.get_index_basic()
    print('[msg] index_basic_sse.csv:{}, index_basic_szse.csv:{}, index_basic_sw.csv:{} updated'.format(n_sse, n_szse, n_sw))

    # 全资产列表
    n = All_Assets_List.rebuild_all_assets_list(True)
    if n is not None:
        print('[msg] config.all_assets_list.csv updated, items:{}'.format(n))

    # 概念列表
    n = Concept.get_concept()
    if n is not None:
        print('[msg] concept.csv updated, items:{}'.format(n))

    # #------------------------更新资产列表al-----------------------

    # 沪深300成分股
    n = All_Assets_List.update_hs300_al()
    print(('[msg] al_HS300成分股.csv updated, items:{}'.format(n)))

    # 申万L1 L2 L3指数列表
    n_l1, n_l2, n_l3 = All_Assets_List.update_swl123_al()
    print(('[msg] al_SW_Index_L1.csv updated, items:{}'.format(n_l1)))
    print(('[msg] al_SW_Index_L2.csv updated, items:{}'.format(n_l2)))
    print(('[msg] al_SW_Index_L3.csv updated, items:{}'.format(n_l3)))

    # 上证50成分股
    n = All_Assets_List.update_sz50_al()
    print(('[msg] al_上证50成分股.csv updated, items:{}'.format(n)))

    # 中证500成分股
    n = All_Assets_List.update_zz500_al()
    print(('[msg] al_中证500成分股.csv updated, items:{}'.format(n)))

    # download_all
    n = All_Assets_List.update_download_all()
    if n is not None:
        print(('[msg] al_download_all.csv updated, items:{}'.format(n)))

    # dl_stocks
    n = All_Assets_List.update_dl_stocks()
    if n is not None:
        print(('[msg] al_dl_stocks.csv updated, items:{}'.format(n)))

    # dl_indexes
    n = All_Assets_List.update_dl_indexes()
    if n is not None:
        print(('[msg] al_dl_indexes.csv updated, items:{}'.format(n)))

    # dl_sh_sz_indexes
    n = All_Assets_List.update_dl_sh_sz_indexes()
    if n is not None:
        print(('[msg] al_dl_sh_sz_indexes.csv updated, items:{}'.format(n)))

    # #------------------------批量下载数据-----------------------
    df = Hsgt.get_moneyflow()  # 沪深港股通资金流向
    if df is not None:
        print('[msg] d_hsgt_flow.csv updated, items:{}'.format(len(df)))

    # download_path = r"download_all"
    # download_path = r"try_001"
    download_path = r"dl_stocks"
    bulk_download(download_path, reload=False)  # 批量下载数据
    download_path = r"dl_sh_sz_indexes"
    bulk_download(download_path, reload=False)  # 批量下载数据
    # download_path = r"special001"
    # bulk_download(download_path, reload=False)  # 批量下载数据

    # download_path = r"try_001"
    download_path = r"dl_stocks"
    bulk_dl_appendix(download_path, reload=False)  # 批量下载股票每日指标数据，及股票复权因子
    # download_path = r"special001"
    # bulk_dl_appendix(download_path, reload=False)  # 批量下载股票每日指标数据，及股票复权因子

    al_file_str = r"dl_stocks"
    # al_file_str = r"try_001"
    bulk_calc_dfq(al_file_str, reload=False)  # 批量计算复权
    # al_file_str = r"special001"
    # bulk_calc_dfq(al_file_str, reload=False)  # 批量计算复权

    # #------------------------收尾-----------------------
    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
