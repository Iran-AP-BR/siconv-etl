# coding: utf-8

from dateutil.parser import parse as date_parse


def datetime_validation(txt, logger=None, dayfirst=True):
    try:
        dtime = date_parse(txt, dayfirst=dayfirst).date()
    
    except Exception as e:
        if logger:
            logger.error(str(e))
        return None

    return dtime

def feedback(logger=None, label='', value=''):
    if logger is not None:
        label_length = 30
        value_length = 30
        label = label + ' ' + '-'*label_length
        value = '-'*value_length + ' ' + value
        logger.info(label[:label_length] + value[-value_length:])

def rows_print(table):
    rows = len(table)
    return f'{rows} {"linhas" if rows>1 else "linha"}'
