import datetime as dt


def get_date_list():
    dates_list = []
    # for i in range(20, 0, -1):
    for i in range(45, 0, -1):
        tempday = dt.datetime.now()
        tempday -= dt.timedelta(days=i)
        tempday_str = tempday.strftime('%Y-%m-%d')
        dates_list.append(tempday_str)
    return dates_list
