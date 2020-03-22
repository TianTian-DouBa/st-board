"""
运行来修改批注
"""
from st_common import sub_notes, sub_path
from XF_LOG_MANAGE import add_log
from st_board import Index, Stock, Concept
import xml.etree.ElementTree as ET


def load_xml(ts_code):
    """
    从.xml文件读入转化为<ElementTree> <et>
    ts_code: <str>
    return: <et>
            None, failed
    """
    file_name = 'an_' + ts_code + '.xml'
    file_path = sub_path + sub_notes + '\\' + file_name
    try:
        tree = ET.parse(file_path)
    except FileNotFoundError:
        log_args = [ts_code]
        add_log(20, '[fn]load_xml(). xml file for {0[0]} not exist', log_args)
        return
    return tree


def update_xml(ts_code, new=None, **kwargs):
    """
    文件存在的话则修改，文件不存在则重新创建
    ts_code: <str>
    new: True, 强制改新的
    kwargs:
        comment1: <str>
        comment2: <str>
    return:
    """
    template_name = 'template'
    file_name = 'an_' + ts_code + '.xml'
    file_path = sub_path + sub_notes + '\\' + file_name
    if new is True:
        tree = load_xml(template_name)
    else:
        tree = load_xml(ts_code)
        if tree is None:
            tree = load_xml(template_name)
    if tree is None:
        log_args = [file_path]
        add_log(20, '[fn]update_xml(). template:"{0[0]}" not exist. Aborted', log_args)
        return

    root = tree.getroot()
    ts_c = root.find('ts_code')
    ts_c.text = ts_code

    # comment1
    if 'comment1' in kwargs:
        str_c1 = str(kwargs['comment1'])
        cmt1 = root.find('comment1')
        el = cmt1.find('content')
        el.text = str_c1

    # comment2
    if 'comment2' in kwargs:
        str_c2 = str(kwargs['comment2'])
        cmt2 = root.find('comment2')
        el = cmt2.find('content')
        el.text = str_c2

    tree.write(file_path, encoding='utf-8')


def cmt(ts_code):
    """
    查看并更新指定资产的备注
    ts_code: None 手动输入ts_code
             <str> 直接用ts_code
    """

    if ts_code is None:
        _ts_code = input("Please input ts_code:")
        if not isinstance(_ts_code, str):
            log_args = [_ts_code]
            add_log(10, '[fn]cmt(). input value {0[0]} invalid, aborted', log_args)
            return
        if len(_ts_code) != 9:
            log_args = [_ts_code]
            add_log(10, '[fn]cmt(). input value {0[0]} invalid, aborted', log_args)
            return
        ts_code = _ts_code

    template_name = 'template'
    file_name = 'an_' + ts_code + '.xml'
    file_path = sub_path + sub_notes + '\\' + file_name

    tree = load_xml(ts_code)
    if tree is None:
        print('The file of {} is not exist, new file created'.format(ts_code))
        tree = load_xml(template_name)
    root = tree.getroot()
    print('[Note] Input "k" to keep the original comment.')

    # ts_code
    ts = root.find('ts_code')
    ts.text = ts_code

    # comment1
    cmt1 = root.find('comment1').find('content')
    print("Old comment1:\n{}\n".format(cmt1.text))
    lines = []
    while True:
        line = input()
        if line:
            lines.append(line)
        else:
            break
    new_cmt1 = '\n'.join(lines)
    if new_cmt1.strip().lower() != 'k':
        cmt1.text = new_cmt1

    # comment2
    cmt2 = root.find('comment2').find('content')
    print("Old comment2:\n{}\n".format(cmt2.text))
    lines = []
    while True:
        line = input()
        if line:
            lines.append(line)
        else:
            break
    new_cmt2 = '\n'.join(lines)
    if new_cmt2.strip().lower() != 'k':
        cmt2.text = new_cmt2
    if new_cmt2.lower() != 'k':
        cmt2.text = new_cmt2

    tree.write(file_path, encoding='utf-8')


def member(ts_code):
    """
    提取指标的成分股到al
    当下只支持申万指数，其它指数的ts_pro.index_weight接口还未实施
    """
    Index.update_sw_member_al(ts_code)


def subsw(ts_code, ex_l3=None):
    """
    提取申万指数L1, L2的下级指数到al
    """
    Index.update_subsw_al(ts_code, ex_l3)


def pledge(ts_code):
    """
    获取质押信息
    """
    Stock.get_pledge(ts_code)


def concept(concept_id):
    """
    获取概念的成分股
    """
    Concept.get_member(concept_id)


if __name__ == '__main__':
    # update_xml('000001.SH', new=True, comment1='abc<123', comment2='注释\n2')
    pass
