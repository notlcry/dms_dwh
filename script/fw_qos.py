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
            valueIndex = statsColumn.index('value')
            hostname = item['tags']['host']
            if hostname == notFound :
                continue
            accountid = item['tags']['account_id']
            if accountid == notFound :
                continue
            typename = item['tags']['type']
            if typename == notFound :
                continue
            typeinstance = item['tags']['type_instance']
            if typeinstance == notFound :
                continue

            entry = item['values'][0]
            value = entry[valueIndex]
            key = hostname + accountid + typeinstance
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['hostname'] = hostname
                inputData['accountid'] = accountid
                inputData['classname'] = typeinstance
                if typename == 'dropped':
                    inputData['dropped'] = value
                elif typename == 'sent_pkt':
                    inputData['txpkts'] = value
                else:
                    inputData['txbytes'] = value
                keyMapping.update({key : inputData})
            else:
                inputData = keyData
                if typename == 'dropped':
                    inputData['dropped'] = value
                elif typename == 'sent_pkt':
                    inputData['txpkts'] = value
                else:
                    inputData['txbytes'] = value

    for (k, v) in keyMapping.items():
        newTime = convertTime(endDt)
        dropped = v.get('dropped', None)
        if dropped is None:
            dropped = 0
        txpkts = v.get('txpkts', None)
        if txpkts is None:
            tkpkts = 0
        txbytes = v.get('txbytes', None)
        if txbytes is None:
            txbytes = 0
        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'classname': v['classname'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'dropped': dropped,
                'txpkts': txpkts,
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
    discard = dt.minute % 5
    delta = timedelta(minutes=discard, seconds=dt.second, microseconds=dt.microsecond)
    endDt = dt - delta
    endTS = endDt.strftime("%s")
    endTS += 's'
    beginDelta = timedelta(minutes=5)
    beginDt = endDt - beginDelta
    beginTS = beginDt.strftime("%s")
    beginTS += 's'

    sql = 'select sum(value) as value from firewall_qos_stat where '
    sql += ' plugin_instance = \'policy.sh\''
    sql += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and vm_type = \'firewall\''
    sql += ' and (type = \'dropped\' or type = \'sent_pkt\' or type = \'sent_bytes\')'
    sql += ' group by type,type_instance,host,account_id;'
    
    # print sql
    statsData = queryInfluxdb(sql)
    

    if statsData  == None:
        sys.exit(1)

    # print statsData.raw
    stats = normalizeData(statsData, endDt)
    if len(stats) > 0 :
        print json.dumps({'result' : stats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




