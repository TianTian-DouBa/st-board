from datetime import datetime
from st_board import bulk_download

if __name__ == "__main__":
    from st_common import Raw_Data
    start_time = datetime.now()
    raw_data = Raw_Data(pull=False)

    # #------------------------批量下载数据-----------------------
    download_path = r"SW_Index"
    # download_path = r"download_all"
    # download_path = r"try_001"
    # download_path = r"dl_stocks"
    bulk_download(download_path, reload=False)  # 批量下载数据

    # download_path = r"try_001"
    download_path = r"dl_stocks"
    # bulk_dl_appendix(download_path, reload=False)  # 批量下载股票每日指标数据，及股票复权因子

    al_file_str = r"dl_stocks"
    # al_file_str = r"try_001"
    # bulk_calc_dfq(al_file_str, reload=False)  # 批量计算复权

    # #------------------------收尾-----------------------
    end_time = datetime.now()
    duration = end_time - start_time
    print('duration={}'.format(duration))
