import pandas as pd
import numpy as np
from XF_common.XF_KXJS import reverse_df

if __name__ == "__main__":
    #print("Hello Raspberrypi")
    df0 = pd.DataFrame(columns=['trade_date','source'])
    df0.trade_date = ('a','b','c','d','e','f')
    df0.source = np.arange(6)
    df0.set_index('trade_date',inplace=True)
    print('-------------------df0-------------------')
    print(df0)
    df1 = pd.DataFrame(columns=['trade_date','MA5'])
    df1.trade_date = ('d','e','f')
    df1.MA5 = (7,8,9)
    df1.set_index('trade_date',inplace=True)
    print('-------------------df1-------------------')
    print(df1)
    head_df1_str, = df1.head(1).index.values
    print("head_df1_str=",head_df1_str)
    pos = df0.index.get_loc(head_df1_str)
    print("pos=",pos)
    print('-------------------drop not useful items-------------------')
    n = 2 #e.g. ma2
    df0.drop(df0.index[pos+n-1:],inplace = True)
    print("n={}".format(n))
    print(df0)
    print('-------------------reversed df-------------------')
    for idx in reversed(df0.index):
        print(idx, df0.source[idx])
    print("***")
    print(reverse_df(df0))
    print('-------------------calculate MA-------------------')
    df_source = df0
    values = []
    rvs_rslt = []
    for idx in reversed(df0.index):
        column_name = 'source'
        values.append(df0[column_name][idx])
        if len(values) > n:
            del values[0]
        if len(values) == n:
            rvs_rslt.append(np.average(values))
    iter_rslt = reversed(rvs_rslt)
    result = list(iter_rslt)
    print(result)
    #df_source = df_source.iloc[:len(df_source)-n+1]
    df_idx = df_source.index[:len(df_source)-n+1]
    print("***")
    #print(df_source)
    print(df_idx)
    _column_name = 'MA' + str(n)
    _data = {_column_name:result}
    df_idt = pd.DataFrame(_data,index=df_idx)
    print("***df_idt")
    print(df_idt)




