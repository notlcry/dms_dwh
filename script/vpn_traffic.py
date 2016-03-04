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

def normalizeData(data):
    global notFound
    stats = []
    nameMapping = {'in_bytes': 'rxbytes', 'in_packets': 'rxpkts',
                    'out_bytes': 'txbytes', 'out_packets': 'txpkts',
                    'in_addtime': 'duration'}
    keyMapping = {}
    if data.raw and data.raw['series']:
        for item in data.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('value')
            timeIndex = statsColumn.index('time')
            typeIndex = statsColumn.index('type')
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            username = item['tags']['user_name']
            if username == notFound :
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
                typeVal = entry[typeIndex]
                key = hostname + accountid + groupname + username + timeVal
                keyData = keyMapping.get(key, None)
                if keyData == None:
                    inputData = {}
                    inputData['hostname'] = hostname
                    inputData['accountid'] = accountid
                    inputData['groupname'] = groupname
                    inputData['username'] = username
                    inputData[nameMapping[typeVal]] = value
                    inputData['time'] = timeVal
                    keyMapping.update({key : inputData})
                else:
                    inputData = keyData
                    inputData[nameMapping[typeVal]] = value
                    inputData['time'] = timeVal

    for (k, v) in keyMapping.items():
        newTime = convertTime(v['time'])
        rxbytes = v.get('rxbytes', None)
        if rxbytes is None:
            rxbytes = 0
        rxpkts = v.get('rxpkts', None)
        if rxpkts is None:
            rxpkts = 0
        txbytes = v.get('txbytes', None)
        if txbytes is None:
            txbytes = 0
        txpkts = v.get('txpkts', None)
        if txpkts is None:
            txpkts = 0
        duration = v.get('duration', None)
        if duration is None:
            duration = 0    
        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'groupname': v['groupname'],
                'username': v['username'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'rxbytes': rxbytes,
                'rxpkts': rxpkts,
                'txbytes': txbytes,
                'txpkts': txpkts,
                'duration': duration
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

    sql = 'select type, value from vpn_stat_sum where '
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'vpn\''
    sql += ' and (type = \'in_bytes\' or type = \'out_bytes\' or type = \'in_packets\' or type = \'out_packets\' or type = \'in_addtime\')'
    sql += ' group by host,account_id,group_name,user_name;'
    statsData = queryInfluxdb(sql)

    if statsData  == None:
        sys.exit(1)

    print statsData.raw

    fwStats = normalizeData(statsData)
    if len(fwStats) > 0 :
        print json.dumps({'result' : fwStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




