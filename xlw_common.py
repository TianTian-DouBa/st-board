"""
xlsxwrite 的一些公共设置
"""

dfc_purple = {'font_color': 'purple'}  # 字体颜色
dfs_14 = {'font_size': 14}  # 字体大小
dtw = {'text_wrap': True}  # text_wrap
dnum_0 = {'num_format': '0'}  # 数字0位小数
dnum_1 = {'num_format': '0.0'}  # 数字1位小数
dnum_2 = {'num_format': '0.00'}  # 数字2位小数
dnum_3 = {'num_format': '0.000'}  # 数字2位小数
dpct_0 = {'num_format': '0%'}  # % 0位小数
dpct_1 = {'num_format': '0.0%'}  # % 1位小数
dpct_2 = {'num_format': '0.00%'}  # % 2位小数
dpct_3 = {'num_format': '0.000%'}  # % 3位小数
dal_left = {'align': 'left'}  # 横向排列
dal_center = {'align': 'center'}  # 横向排列
dal_center_across = {'center_across': True}
dval_center = {'valign': 'vcenter'}  # 纵向排列
dbd = {'border': 1}  # 单元格边框
# ----背景颜色
dbgc_light_green = {'bg_color': '#92D050'}  # 浅绿背景色


d_std = {**dbd}
d_wrap = {**dbd, **dtw}
d_rpt_title = {**dfc_purple, **dfs_14, **dval_center, **dal_left}
d_head0 = {**dbd, **dal_center, **dval_center, **dal_center_across}  # 表头第0行
d_head1 = {**dbd, **dal_center, **dval_center, **dbgc_light_green}  # 表头第1行
d_center = {**dbd, **dal_center, **dval_center}  # 居中
d_int = {**dbd, **dnum_0, **dval_center}  # 整数
d_f1d = {**dbd, **dnum_1, **dval_center}  # 1位小数
d_f2d = {**dbd, **dnum_2, **dval_center}  # 2位小数
d_f3d = {**dbd, **dnum_3, **dval_center}  # 3位小数
d_pct = {**dbd, **dpct_0, **dval_center}  # 0%
d_pct1d = {**dbd, **dpct_1, **dval_center}  # 1.1%
d_pct2d = {**dbd, **dpct_2, **dval_center}  # 2.22%
