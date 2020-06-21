from report_daily_basic import rpt_d_basic
from report_race import report

if __name__ == '__main__':
    al_str = 'dl_stocks'
    # al_str = 'try_002'
    rpt_d_basic(al_str)
    report(al_str)
    pass
