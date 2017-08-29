import datetime as dt

DATE_FORMAT = "%Y-%m-%d"

def addDays(date, days):
    date = toDate(date)
    return date + dt.timedelta(days=days)

def toDate(date):
    if isinstance(date, str):
        date = dt.datetime.strptime(date, DATE_FORMAT)
    return date

def toString(date):
    return dt.datetime.strftime(date, DATE_FORMAT)
