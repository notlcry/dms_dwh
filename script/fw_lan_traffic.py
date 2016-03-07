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

def convertTime(endDt):
    dt = endDt
    date_key = str(dt.year) + ('0' if dt.month < 10 else '') + str(dt.month) + ('0' if dt.day < 10 else '') + str(dt.day)
    date_key = int(date_key)
    time_key = ('0' if dt.hour < 10 else '') + str(dt.hour) + ('0' if dt.minute < 10 else '') + str(dt.minute) + ('0' if dt.second < 10 else '') + str(dt.second)
    time_key = int(time_key)

    return (date_key, time_key,)

def normalizeData(data, endDt):
    global notFound
    stats = []

    keyMapping = {}
    if statsData.raw and statsData.raw['series']:
        for item in statsData.raw['series']:
            statsColumn = item['columns']
            timeIndex = statsColumn.index('time')
            avgIndex = statsColumn.index('avg_value')
            minIndex = statsColumn.index('min_value')
            maxIndex = statsColumn.index('max_value')

            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            groupname = item['tags']['group_name']
            if groupname == notFound :
                continue
            username = item['tags']['user_name']
            if username == notFound :
                continue
            typeinstance = item['tags']['type']
            if typeinstance == notFound :
                continue

            entry = item['values'][0]
            # value = entry[valueIndex]

            avg_val = entry[avgIndex]
            min_val = entry[minIndex]
            max_val = entry[maxIndex]

            key = hostname + accountid + groupname + username
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['hostname'] = hostname
                inputData['accountid'] = accountid
                inputData['groupname'] = groupname
                inputData['username'] = username
                if typeinstance == 'dst_bytes':
                    inputData['rxbps_avg'] = avg_val
                    inputData['rxbps_min'] = min_val
                    inputData['rxbps_max'] = max_val
                elif typeinstance == 'dst_packets':
                    inputData['rxpps_avg'] = avg_val
                    inputData['rxpps_min'] = min_val
                    inputData['rxpps_max'] = max_val
                elif typeinstance == 'src_bytes':
                    inputData['txbps_avg'] = avg_val
                    inputData['txbps_min'] = min_val
                    inputData['txbps_max'] = max_val
                else:
                    inputData['txpps_avg'] = avg_val
                    inputData['txpps_min'] = min_val
                    inputData['txpps_max'] = max_val
                keyMapping.update({key : inputData})

            else:
                inputData = keyData
                if typeinstance == 'dst_bytes':
                    inputData['rxbps_avg'] = avg_val
                    inputData['rxbps_min'] = min_val
                    inputData['rxbps_max'] = max_val
                elif typeinstance == 'dst_packets':
                    inputData['rxpps_avg'] = avg_val
                    inputData['rxpps_min'] = min_val
                    inputData['rxpps_max'] = max_val
                elif typeinstance == 'src_bytes':
                    inputData['txbps_avg'] = avg_val
                    inputData['txbps_min'] = min_val
                    inputData['txbps_max'] = max_val
                else:
                    inputData['txpps_avg'] = avg_val
                    inputData['txpps_min'] = min_val
                    inputData['txpps_max'] = max_val

    for (k, v) in keyMapping.items():
        newTime = convertTime(endDt)

        rxbps_avg = v.get('rxbps_avg', None)
        if rxbps_avg is None:
            rxbps_avg = 0

        rxbps_min = v.get('rxbps_min', None)
        if rxbps_min is None:
            rxbps_min = 0

        rxbps_max = v.get('rxbps_max', None)
        if rxbps_max is None:
            rxbps_max = 0

        txbps_avg = v.get('txbps_avg', None)
        if txbps_avg is None:
            txbps_avg = 0

        txbps_min = v.get('txbps_min', None)
        if txbps_min is None:
            txbps_min = 0

        txbps_max = v.get('txbps_max', None)
        if txbps_max is None:
            txbps_max = 0

        rxpps_avg = v.get('rxpps_avg', None)
        if rxpps_avg is None:
            rxpps_avg = 0

        rxpps_min = v.get('rxpps_min', None)
        if rxpps_min is None:
            rxpps_min = 0

        rxpps_max = v.get('rxpps_max', None)
        if rxpps_max is None:
            rxpps_max = 0

        txpps_avg = v.get('txpps_avg', None)
        if txpps_avg is None:
            txpps_avg = 0

        txpps_min = v.get('txpps_min', None)
        if txpps_min is None:
            txpps_min = 0

        txpps_max = v.get('txpps_max', None)
        if txpps_max is None:
            txpps_max = 0

        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'groupname': v['groupname'],
                'username': v['username'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],

                'rxbps_avg': rxbps_avg,
                'rxbps_min': rxbps_min,
                'rxbps_max': rxbps_max,

                'txbps_avg': txbps_avg,
                'txbps_min': txbps_min,
                'txbps_max': txbps_max,

                'rxpps_avg': rxpps_avg,
                'rxpps_min': rxpps_min,
                'rxpps_max': rxpps_max,

                'txpps_avg': txpps_avg,
                'txpps_min': txpps_min,
                'txpps_max': txpps_max
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

    sql = 'select mean(value)*8 as avg_value, min(value)*8 as min_value, max(value)*8 as max_value from firewall_traffic_rate_stat where '
    sql += ' (plugin_instance=\'intergrp\' or plugin_instance=\'intragrp\')'
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and (type = \'dst_bytes\' or type = \'dst_packets\' or type = \'src_bytes\' or type = \'src_packets\')'
    sql += ' group by type,host,account_id,group_name,user_name;'

    print sql
    statsData = queryInfluxdb(sql)
    
    if statsData  == None:
        sys.exit(1)

    # print statsData.raw
    fwStats = normalizeData(statsData, endDt)
    if len(fwStats) > 0 :
        print json.dumps({'result' : fwStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




