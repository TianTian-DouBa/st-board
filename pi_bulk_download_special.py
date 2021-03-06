from datetime import datetime
from st_board import All_Assets_List, Concept, Hsgt, bulk_download, bulk_dl_appendix, bulk_calc_dfq, bulk_pad_fq

if __name__ == "__main__":
    from st_common import Raw_Data
    # global raw_data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)


    # #------------------------批量下载数据-----------------------
    download_path = r"special001"
    bulk_download(download_path, reload=False)  # 批量下载数据

    download_path = r"special001"
    bulk_dl_appendix(download_path, reload=False)  # 批量下载股票每日指标数据，及股票复权因子
    bulk_pad_fq(download_path)  # 补全原数据中缺失的复权因子

    al_file_str = r"special001"
    bulk_calc_dfq(al_file_str, reload=False)  # 批量计算复权

    # #------------------------收尾-----------------------
    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
