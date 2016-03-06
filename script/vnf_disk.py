#!/usr/bin/python

import os
import sys
import json
from ConfigParser import SafeConfigParser
from influxdb import client as influxdb
from datetime import timedelta, datetime
import time
from dateutil import tz

def queryInfluxdb (sql):
    global influxdbIp
    global influxdbPort
    global database
    global username
    global password
    global notFound

    db = influxdb.InfluxDBClient(influxdbIp, influxdbPort, username, password, database, timeout=10)
    try:
        return db.query(sql)
    except:
        return None

def convertTime(endDt):
    dt = endDt
    date_key = str(dt.year) + ('0' if dt.month < 10 else '') + str(dt.month) + ('0' if dt.day < 10 else '') + str(dt.day)
    date_key = int(date_key)
    time_key = ('0' if dt.hour < 10 else '') + str(dt.hour) + ('0' if dt.minute < 10 else '') + str(dt.minute) + ('0' if dt.second < 10 else '') + str(dt.second)
    time_key = int(time_key)

    return (date_key, time_key,)

def normalizeData(statsData, endDt):
    stats = []
    keyMapping = {}
    if statsData.raw and statsData.raw['series']:
        for item in statsData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            maxIndex = statsColumn.index('maxvalue')
            avgIndex = statsColumn.index('avgvalue')
            minIndex = statsColumn.index('minvalue')
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            hostname = item['tags']['host']
            if hostname == notFound :
                continue

            entry = item['values'][0]
            maxvalue = entry[maxIndex]
            avgvalue = entry[avgIndex]
            minvalue = entry[minIndex]
            key = accountid + hostname
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['accountid'] = accountid
                inputData['hostname'] = hostname
                inputData['maxdisk'] = maxvalue
                inputData['avgdisk'] = avgvalue
                inputData['mindisk'] = minvalue
                keyMapping.update({key : inputData})
            else:
                inputData = keyData
                inputData['maxdisk'] = maxvalue
                inputData['avgdisk'] = avgvalue
                inputData['mindisk'] = minvalue


    for (k, v) in keyMapping.items():
        newTime = convertTime(endDt)
        maxdisk = v.get('maxdisk', None)
        if maxdisk is None:
            maxdisk = 0
        avgdisk = v.get('avgdisk', None)
        if avgdisk is None:
            avgdisk = 0
        mindisk = v.get('mindisk', None)
        if mindisk is None:
            mindisk = 0
        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'maxdisk': maxdisk,
                'avgdisk': avgdisk,
                'mindisk': mindisk
            }
        stats.append(res)

    return stats

"""
Main Part
"""
try:
    config = SafeConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), "system.conf")
    config.read(config_file)

    influxdbIp = config.get('InfluxDB', 'server_ip')
    influxdbPort = config.getint('InfluxDB', 'server_port')
    database = config.get('InfluxDB', 'database')
    username = config.get('InfluxDB', 'username')
    password = config.get('InfluxDB', 'password')
    notFound = config.get('InfluxDB', 'not_found')

    dt = datetime.now()
    discard = dt.minute % 5 
    delta = timedelta(minutes=discard, seconds=dt.second, microseconds=dt.microsecond)
    endDt = dt - delta
    endTS = endDt.strftime("%s")
    endTS += 's'
    beginDelta = timedelta(minutes=5)
    beginDt = endDt - beginDelta
    beginTS = beginDt.strftime("%s")
    beginTS += 's'

    sql = 'select max(value) as maxvalue, mean(value) as avgvalue, min(value) as minvalue from disk_stat where '
    sql += ' time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and type_instance = \'used\''
    sql += ' group by account_id,host;'
    
    # print sql
    statsData = queryInfluxdb(sql)
    # print statsData.raw

    if statsData  == None:
        sys.exit(1)

    vnfStats = normalizeData(statsData, endDt)
    if len(vnfStats) > 0 :
        print json.dumps({'result' : vnfStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




