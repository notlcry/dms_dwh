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

def convertTime(utcTime):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Shanghai')
    utcZone = datetime.strptime(utcTime,'%Y-%m-%dT%H:%M:%SZ');
    utcZone = utcZone.replace(tzinfo=from_zone)
    dt = utcZone.astimezone(to_zone)
    date_key = str(dt.year) + ('0' if dt.month < 10 else '') + str(dt.month) + ('0' if dt.day < 10 else '') + str(dt.day)
    date_key = int(date_key)
    time_key = ('0' if dt.hour < 10 else '') + str(dt.hour) + ('0' if dt.minute < 10 else '') + str(dt.minute) + ('0' if dt.second < 10 else '') + str(dt.second)
    time_key = int(time_key)

    return (date_key, time_key,)

def mergeData(maxData, avgData, minData):
    stats = []
    keyMapping = {}
    if maxData.raw and maxData.raw['series']:
        for item in maxData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            valueIndex = statsColumn.index('value')
            instancename = item['tags']['instance_name']
            if instancename == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            groupname = item['tags']['group_name']
            if groupname == notFound :
                continue
            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                key = accountid + groupname + instancename + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['instancename'] = instancename
                    inputData['groupname'] = groupname
                    inputData['accountid'] = accountid
                    inputData['maxmemory'] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData['maxmemory'] = value
                    inputData['time'] = timeVal

    if avgData.raw and avgData.raw['series']:
        for item in avgData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            valueIndex = statsColumn.index('value')
            instancename = item['tags']['instance_name']
            if instancename == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            groupname = item['tags']['group_name']
            if groupname == notFound :
                continue
            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                key = accountid + groupname + instancename + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['instancename'] = instancename
                    inputData['groupname'] = groupname
                    inputData['accountid'] = accountid
                    inputData['avgmemory'] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData['avgmemory'] = value
                    inputData['time'] = timeVal

    if minData.raw and minData.raw['series']:
        for item in minData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            valueIndex = statsColumn.index('value')
            instancename = item['tags']['instance_name']
            if instancename == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            groupname = item['tags']['group_name']
            if groupname == notFound :
                continue
            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                key = accountid + groupname + instancename + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['instancename'] = instancename
                    inputData['groupname'] = groupname
                    inputData['accountid'] = accountid
                    inputData['minmemory'] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData['minmemory'] = value
                    inputData['time'] = timeVal
   

    for (k, v) in keyMapping.items():
        newTime = convertTime(v['time'])
        maxmem = v.get('maxmemory', None)
        if maxmem is None:
            maxmem = 0
        avgmem = v.get('avgmemory', None)
        if avgmem is None:
            avgmem = 0
        minmem = v.get('minmemory', None)
        if minmem is None:
            minmem = 0
        res = {
                'instancename': v['instancename'],
                'groupname': v['groupname'],
                'accountid': v['accountid'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'maxmem': maxmem,
                'avgmem': avgmem,
                'minmem': minmem
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
    discard = dt.minute % 30 + 30 
    delta = timedelta(minutes=discard, seconds=dt.second, microseconds=dt.microsecond)
    endDt = dt - delta
    endTS = endDt.strftime("%s")
    endTS += 's'
    beginDelta = timedelta(minutes=30)
    beginDt = endDt - beginDelta
    beginTS = beginDt.strftime("%s")
    beginTS += 's'

    sql = 'select value from vpc_memory_util_stat_max where '
    sql += ' time > ' + beginTS + ' and time <= ' + endTS
    sql += ' group by account_id,group_name,instance_name;'
    
    # print sql
    maxStatsData = queryInfluxdb(sql)
    # print maxStatsData.raw

    if maxStatsData  == None:
        sys.exit(1)

    sql = 'select value from vpc_memory_util_stat_mean where '
    sql += ' time > ' + beginTS + ' and time <= ' + endTS
    sql += ' group by account_id,group_name,instance_name;'
    # print sql
    avgStatsData = queryInfluxdb(sql)
    # print avgStatsData.raw
    if avgStatsData == None:
        sys.exit(1)

    sql = 'select value from vpc_memory_util_stat_min where '
    sql += ' time > ' + beginTS + ' and time <= ' + endTS
    sql += ' group by account_id,group_name,instance_name;'
    # print sql
    minStatsData = queryInfluxdb(sql)
    # print minStatsData.raw
    if minStatsData == None:
        sys.exit(1)

    vpcStats = mergeData(maxStatsData, avgStatsData, minStatsData)
    if len(vpcStats) > 0 :
        print json.dumps({'result' : vpcStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




