from datetime import datetime, timedelta

import pytest

from datetime_parser import parse_datetime


def test_parse_datetime():
    t_data = (
        '25 Jan 15:30',
        'Сегодня 06:15',
        '2020-01-25 13:00',
        'Завтра 14:30'
    )

    today =datetime.now()
    year = today.year
    month = today.month
    day =  today.day

    tomorrow = datetime.now() + timedelta(days=1)
    t_year = tomorrow.year
    t_month = tomorrow.month
    t_day = tomorrow.day

    c_data = (
        datetime.strptime('%d 25 Jan 15:30' % year, '%Y %d %b %H:%M'),
        datetime.strptime('%d %d %d 06:15' % (year, month, day), '%Y %m %d %H:%M'),
        datetime.strptime('2020-01-25 13:00', '%Y-%m-%d %H:%M'),
        datetime.strptime('%d-%d-%d 14:30' % (t_year, t_month, t_day), '%Y-%m-%d %H:%M')
    )

    for j, i in enumerate(t_data):
        res = parse_datetime(i)
        assert res == c_data[j], 'Result <%s> != expectation <%s>' % (res, c_data[j])
