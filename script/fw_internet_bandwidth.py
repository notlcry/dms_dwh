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
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            typeinstance = item['tags']['type_instance']
            if typeinstance == notFound :
                continue
            entry = item['values'][0]
            maxvalue = entry[maxIndex]
            avgvalue = entry[avgIndex]
            minvalue = entry[minIndex]
            key = hostname + accountid
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['hostname'] = hostname
                inputData['accountid'] = accountid
                if typeinstance == 'rx':
                    inputData['maxrx'] = maxvalue
                    inputData['avgrx'] = avgvalue
                    inputData['minrx'] = minvalue
                else:
                    inputData['maxtx'] = maxvalue
                    inputData['avgtx'] = avgvalue
                    inputData['mintx'] = minvalue
                keyMapping.update({key : inputData})
            else:
                inputData = keyData
                if typeinstance == 'rx':
                    inputData['maxrx'] = maxvalue
                    inputData['avgrx'] = avgvalue
                    inputData['minrx'] = minvalue
                else:
                    inputData['maxtx'] = maxvalue
                    inputData['avgtx'] = avgvalue
                    inputData['mintx'] = minvalue

    for (k, v) in keyMapping.items():
        newTime = convertTime(endDt)
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
    discard = dt.minute % 5
    delta = timedelta(minutes=discard, seconds=dt.second, microseconds=dt.microsecond)
    endDt = dt - delta
    endTS = endDt.strftime("%s")
    endTS += 's'
    beginDelta = timedelta(minutes=5)
    beginDt = endDt - beginDelta
    beginTS = beginDt.strftime("%s")
    beginTS += 's'

    sql = 'select max(value) as maxvalue, mean(value) as avgvalue, min(value) as minvalue from interface_rate_stat where '
    sql += ' plugin_instance = \'eth1\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'if_octets\''
    sql += ' and (type_instance = \'tx\' or type_instance = \'rx\')'
    sql += ' group by type_instance,host,account_id;'
    
    # print sql
    statsData = queryInfluxdb(sql)
    # print statsData.raw

    if statsData  == None:
        sys.exit(1)

    fwStats = normalizeData(statsData, endDt)
    if len(fwStats) > 0 :
        print json.dumps({'result' : fwStats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




