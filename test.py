import pandas as pd
import numpy as np

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



