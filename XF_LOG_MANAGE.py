Log_Category = {10:'[Error]', 20:'[Warning]', 30:'[Info]' , 40:'[Trace]'} #used for add_log()
Log_Thread_Hold = 40 #to add the log if log_category <= Log_Thread_Hold

def add_log(log_category, log_string, *args):
    """追加log信息
    e.g:
    log_args = [path]
    add_log(10, 'fn:funchtion(). --"{0[0]}"', log_args)"""
    global Log_Thread_Hold
    if log_category > Log_Thread_Hold:
        return

    try:
        category = Log_Category[log_category]
    except KeyError:
        log_args = [log_category]
        add_log(40, "log_category key '{0[0]}' was not defined", log_args)
        category = '[Undefined]'
    print((category + " " + log_string).format(*args))

def logable(log_category):
    """判断是否需要log
    e.g:
    if logable(40):
        log_print("info to print")"""
    global Log_Thread_Hold
    if log_category <= Log_Thread_Hold:
        return True
    else:
        return

def log_print(info):
    """同print()，便于将来重写"""
    print(info)
