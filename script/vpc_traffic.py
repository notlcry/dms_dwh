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

def mergeData(rxData, txData):
    global notFound

    keyMapping = {}
    if rxData.raw and rxData.raw['series']:
        for item in rxData.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('value')
            timeIndex = statsColumn.index('time')
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
                    inputData['accountid'] = accountid
                    inputData['groupname'] = groupname
                    inputData['rxbytes'] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    preVal = inputData.get('rxbytes', 0)
                    inputData['rxbytes'] = value + preVal
                    inputData['time'] = timeVal

    if txData.raw and txData.raw['series']:
        for item in txData.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('value')
            timeIndex = statsColumn.index('time')
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
                    inputData['accountid'] = accountid
                    inputData['groupname'] = groupname
                    inputData['txbytes'] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    preVal = inputData.get('txbytes', 0)
                    inputData['txbytes'] = value + preVal
                    inputData['time'] = timeVal

    stats = []
    for (k, v) in keyMapping.items():
        newTime = convertTime(v['time'])
        rxbytes = v.get('rxbytes', None)
        if rxbytes is None:
            rxbytes = 0
        txbytes = v.get('txbytes', None)
        if txbytes is None:
            txbytes = 0
        
        res = {
                'instancename': v['instancename'],
                'accountid': v['accountid'],
                'groupname': v['groupname'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'rxbytes': rxbytes,
                'txbytes': txbytes
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

    sql = 'select mac, value from vpc_network_incoming_stat_sum where '
    sql += ' type = \'network.incoming.bytes\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' group by account_id,group_name,instance_name;'
    rxStatsData = queryInfluxdb(sql)
    
    if rxStatsData  == None:
        sys.exit(1)

    # print rxStatsData.raw

    sql = 'select mac, value from vpc_network_outcoming_stat_sum where '
    sql += ' type = \'network.outcoming.bytes\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' group by account_id,group_name,instance_name;'
    print sql
    txStatsData = queryInfluxdb(sql)
    
    if txStatsData  == None:
        sys.exit(1)

    # print txStatsData.raw

    vpcStats = mergeData(rxStatsData, txStatsData)
    if len(vpcStats) > 0 :
        print json.dumps({'result' : vpcStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)



