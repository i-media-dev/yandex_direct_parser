import datetime as dt

from parser.constants import DATE_FORMAT, DAYS_TO_GENERATE


def get_date_list() -> list[str]:
    """Функция генерирует список дат за указанное количество дней."""
    dates_list = []
    for i in range(DAYS_TO_GENERATE, 0, -1):
        tempday = dt.datetime.now()
        tempday -= dt.timedelta(days=i)
        tempday_str = tempday.strftime(DATE_FORMAT)
        dates_list.append(tempday_str)
    return dates_list
