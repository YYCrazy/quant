# -*- coding: utf-8 -*-

import datetime


# 日期类函数

def createdate_list(start, end):
    date_list = []
    start_date = datetime.datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end, '%Y-%m-%d')

    while start_date.__le__(end_date):
        date_list.append(start_date.strftime('%Y-%m-%d'))
        start_date += datetime.timedelta(days=+1)

    return date_list
