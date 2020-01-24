from datetime import datetime, timedelta
from dateutil.parser import parse

_today = datetime.now().strftime('%d %b')
_tomorrow = (datetime.now() + timedelta(days=1)).strftime('%d %b')
def parse_datetime(datetime_str):
    """Преобразует строку в py datetime.
    """
    if 'Live' in datetime_str:
        return datetime.now()

    for i in ['Сегодня', 'Today']:
        if i in datetime_str:
            return parse(datetime_str.replace(i, _today))

    for i in ['Завтра', 'Tomorrow']:
        if i in datetime_str:
            return parse(datetime_str.replace(i, _tomorrow))

    return parse(datetime_str)
