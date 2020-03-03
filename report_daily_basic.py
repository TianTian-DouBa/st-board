from st_common import Raw_Data
from st_board import Select_Collect

al_file_name = Select_Collect.al_file_name


def rpt_d_basic(al_file):
    """
    报告：每日基本情况
    al_file: <str>
    """
    from st_board import Strategy, Pool, Condition, Filter

    stg = Strategy('report_daily_basic')
    stg.add_pool(desc='p10', al_file=al_file, del_trsfed=None)
    p10 = stg.pools[10]
    # ------condition_0
    pre_args1 = {'idt_type': 'ma',
                 'period': 20}
    pre_args2 = {'idt_type': 'maqs',
                 'period': 20}
    p10.add_condition(pre_args1, pre_args2, '>')
    # ------condition_1
    pre_args1 = {'idt_type': 'macd',  # 日线macd
                 'long_n1': 26,
                 'short_n2': 12,
                 'dea_n3': 9}
    pre_args2 = {'idt_type': 'macd',  # 周线macd
                 'long_n1': 130,
                 'short_n2': 60,
                 'dea_n3': 45}
    p10.add_condition(pre_args1, pre_args2, '>')
    # ------condition_2
    pre_args1 = {'idt_type': 'macd',  # 月线macd
                 'long_n1': 26,
                 'short_n2': 12,
                 'dea_n3': 9}
    pre_args2 = {'idt_type': 'const',
                 'const_value': 0}
    p10.add_condition(pre_args1, pre_args2, '>')


if __name__ == '__main__':
    raw_data = Raw_Data(pull=False)
    al_file = al_file_name()
    rpt_d_basic(al_file)

