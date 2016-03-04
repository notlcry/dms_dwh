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
    maxNameMapping = {'rx': 'maxrx', 'tx': 'maxtx'}
    if maxData.raw and maxData.raw['series']:
        for item in maxData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            typeIndex = statsColumn.index('type_instance')
            valueIndex = statsColumn.index('value')
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue

            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                typeVal = entry[typeIndex]
                key = hostname + accountid + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['hostname'] = hostname
                    inputData['accountid'] = accountid
                    inputData[maxNameMapping[typeVal]] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData[maxNameMapping[typeVal]] = value
                    inputData['time'] = timeVal

    avgNameMapping = {'rx': 'avgrx', 'tx': 'avgtx'}
    if avgData.raw and avgData.raw['series']:
        for item in avgData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            typeIndex = statsColumn.index('type_instance')
            valueIndex = statsColumn.index('value')
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue

            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                typeVal = entry[typeIndex]
                key = hostname + accountid + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['hostname'] = hostname
                    inputData['accountid'] = accountid
                    inputData[avgNameMapping[typeVal]] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData[avgNameMapping[typeVal]] = value
                    inputData['time'] = timeVal

    minNameMapping = {'rx': 'minrx', 'tx': 'mintx'}
    if minData.raw and minData.raw['series']:
        for item in minData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            typeIndex = statsColumn.index('type_instance')
            valueIndex = statsColumn.index('value')
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue

            for entry in item['values']:
                value = entry[valueIndex]
                if value == None:
                    value = 0
                timeVal = entry[timeIndex]
                typeVal = entry[typeIndex]
                key = hostname + accountid + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['hostname'] = hostname
                    inputData['accountid'] = accountid
                    inputData[minNameMapping[typeVal]] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData[minNameMapping[typeVal]] = value
                    inputData['time'] = timeVal
   

    for (k, v) in keyMapping.items():
        newTime = convertTime(v['time'])
        maxrx = v.get('maxrx', None)
        if maxrx is None:
            maxrx = 0
        else:
            maxrx = maxrx * 8
        maxtx = v.get('maxtx', None)
        if maxtx is None:
            maxtx = 0
        else:
            maxtx = maxtx * 8
        avgrx = v.get('avgrx', None)
        if avgrx is None:
            avgrx = 0
        else:
            avgrx = avgrx * 8
        avgtx = v.get('avgtx', None)
        if avgtx is None:
            avgtx = 0
        else:
            avgtx = avgtx * 8
        minrx = v.get('minrx', None)
        if minrx is None:
            minrx = 0
        else:
            minrx = minrx * 8
        mintx = v.get('mintx', None)
        if mintx is None:
            mintx = 0
        else:
            mintx = mintx * 8
        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'maxrx': maxrx,
                'maxtx': maxtx,
                'avgrx': avgrx,
                'avgtx': avgtx,
                'minrx': minrx,
                'mintx': mintx
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

    sql = 'select type_instance, value from interface_rate_stat_max where '
    sql += ' plugin_instance = \'eth1\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'if_octets\''
    sql += ' and (type_instance = \'tx\' or type_instance = \'rx\')'
    sql += ' group by host,account_id;'
    
    # print sql
    maxStatsData = queryInfluxdb(sql)
    # print maxStatsData.raw

    if maxStatsData  == None:
        sys.exit(1)

    sql = 'select type_instance, value from interface_rate_stat_mean where '
    sql += ' plugin_instance = \'eth1\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'if_octets\''
    sql += ' and (type_instance = \'tx\' or type_instance = \'rx\')'
    sql += ' group by host,account_id;'
    # print sql
    avgStatsData = queryInfluxdb(sql)
    # print avgStatsData.raw
    if avgStatsData == None:
        sys.exit(1)

    sql = 'select type_instance, value from interface_rate_stat_min where '
    sql += ' plugin_instance = \'eth1\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'if_octets\''
    sql += ' and (type_instance = \'tx\' or type_instance = \'rx\')'
    sql += ' group by host,account_id;'
    # print sql
    minStatsData = queryInfluxdb(sql)
    # print minStatsData.raw
    if minStatsData == None:
        sys.exit(1)

    fwStats = mergeData(maxStatsData, avgStatsData, minStatsData)
    if len(fwStats) > 0 :
        print json.dumps({'result' : fwStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




