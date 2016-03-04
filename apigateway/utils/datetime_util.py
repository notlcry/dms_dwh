from datetime import timedelta, datetime
from dateutil import tz

def convert_time(utcTime):
    fromZone = tz.gettz('UTC')
    toZone = tz.gettz('Asia/Shanghai')
    utcZone = datetime.strptime(utcTime,'%Y-%m-%dT%H:%M:%SZ');
    utcZone = utcZone.replace(tzinfo=fromZone)
    dt = utcZone.astimezone(toZone)

    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_begin_date(interval):
    nowDt = datetime.now()
    delta = timedelta(minutes=interval, seconds=nowDt.second, microseconds=nowDt.microsecond)
    dt = nowDt - delta
    beginType = 0
    if dt.day < nowDt.day or dt.month < nowDt.month or dt.year < nowDt.year:
        beginType = 1

    dateKey = str(dt.year) + ('0' if dt.month < 10 else '') + str(dt.month) + ('0' if dt.day < 10 else '') + str(dt.day)
    dateKey = int(dateKey)
    tableType = get_table_type(interval)
    if tableType == '1h':
        timeKey = ('0' if dt.hour < 10 else '') + str(dt.hour) + '0000'
    else:
        timeKey = ('0' if dt.hour < 10 else '') + str(dt.hour) + ('0' if dt.minute < 10 else '') + str(dt.minute) + ('0' if dt.second < 10 else '') + str(dt.second)
    timeKey = int(timeKey)

    return (beginType, dateKey, timeKey,)

def get_table_type(interval):
    if interval == 0:
        return 0 #realtime table

    if interval <= 60:
        return '5min' # TBD : 1min table
    elif interval <= 12*60:
        return '5min' # 5min table
    elif interval <= 24*60:
        return '1h' # TBD : 15min table
    elif interval <= 7*24*60:
        return '1h' # 1hour table
    elif interval <= 30*24*60:
        return '1h' # TBD : 1day table
    elif interval <= 365*24*60:
        return '1h' # TBD : 1month table
    else:
        return '1h' # TBD : 1year table

def get_next_hour(dateStr, hourVal):
    val = hourVal + 1
    if val >= 24:
        valStr = ('0' if val < 10 else '') + str(hourVal)
        res = str(dateStr) + ' ' + valStr + ':00:00'
        dt = datetime.strptime(res,'%Y-%m-%d %H:%M:%S');
        delta = timedelta(hours=1)
        dt += delta
        res = str(dt.year) + '-' + ('0' if dt.month < 10 else '') + str(dt.month) + '-' + ('0' if dt.day < 10 else '') + str(dt.day) + ' 00:00:00'
    else:
        valStr = ('0' if val < 10 else '') + str(val)
        res = str(dateStr) + ' ' + valStr + ':00:00'
    return res

if __name__ == '__main__':
    print convert_time('2016-02-26T09:00:00Z')
    print get_begin_date(720)
    print get_begin_date(1440)
    print get_table_type(60)
    print get_table_type(300)
    print get_table_type(1440)
    print get_next_hour('2016-02-27', 6)
    print get_next_hour('2016-02-27', 9)
    print get_next_hour('2016-02-27', 23)