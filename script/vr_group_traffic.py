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
            groupname = item['tags']['group_name']
            if groupname == notFound :
                continue
            typeinstance = item['tags']['type']
            if typeinstance == notFound :
                continue

            entry = item['values'][0]
            value = entry[valueIndex]
            key = hostname + accountid + groupname
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['hostname'] = hostname
                inputData['accountid'] = accountid
                inputData['groupname'] = groupname
                if typeinstance == 'dl_src_n_bytes':
                    inputData['rxbytes'] = value
                elif typeinstance == 'mod_dl_dst_n_bytes':
                    inputData['txbytes'] = value
                elif typeinstance == 'dl_src_n_packets':
                    inputData['rxpkts'] = value
                else:
                    inputData['txpkts'] = value
                keyMapping.update({key : inputData})
            else:
                inputData = keyData
                if typeinstance == 'dl_src_n_bytes':
                    inputData['rxbytes'] = value
                elif typeinstance == 'mod_dl_dst_n_bytes':
                    inputData['txbytes'] = value
                elif typeinstance == 'dl_src_n_packets':
                    inputData['rxpkts'] = value
                else:
                    inputData['txpkts'] = value

    for (k, v) in keyMapping.items():
        newTime = convertTime(endDt)
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
        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'groupname': v['groupname'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'rxbytes': rxbytes,
                'rxpkts': rxpkts,
                'txbytes': txbytes,
                'txpkts': txpkts
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

    sql = 'select sum(value) as value from vrouter_traffic_stat where '
    sql += ' time > ' + beginTS + ' and time <= ' + endTS
    sql += ' and (type = \'dl_src_n_bytes\' or type = \'mod_dl_dst_n_bytes\' or type = \'dl_src_n_packets\' or type = \'mod_dl_dst_n_packets\')'
    sql += ' and user_or_group = \'group\''
    sql += ' group by type,host,account_id,group_name;'
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




