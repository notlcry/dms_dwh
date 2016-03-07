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


def normalizeData(stats_data, rate_data, endDt):
    global notFound
    stats = []
    keyMapping = {}
    if stats_data.raw and stats_data.raw['series']:
        for item in stats_data.raw['series']:
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


    if rate_data.raw and rate_data.raw['series']:
        for item in rate_data.raw['series']:
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
            typename = item['tags']['type']
            if typename == notFound :
                continue
            typeinstance = item['tags']['type_instance']
            if typeinstance == notFound :
                continue

            entry = item['values'][0]
            # value = entry[valueIndex]

            avg_val = entry[avgIndex]
            min_val = entry[minIndex]
            max_val = entry[maxIndex]

            key = hostname + accountid + typeinstance
            keyData = keyMapping.get(key, None)
            if keyData == None:
                inputData = {}
                inputData['hostname'] = hostname
                inputData['accountid'] = accountid
                inputData['classname'] = typeinstance
                if typename == 'sent_bytes':
                    inputData['txbps_avg'] = avg_val
                    inputData['txbps_min'] = min_val
                    inputData['txbps_max'] = max_val
                keyMapping.update({key : inputData})

            else:
                inputData = keyData
                if typename == 'sent_bytes':
                    inputData['txbps_avg'] = avg_val
                    inputData['txbps_min'] = min_val
                    inputData['txbps_max'] = max_val

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

        txbps_avg = v.get('txbps_avg', None)
        if txbps_avg is None:
            txbps_avg = 0

        txbps_min = v.get('txbps_min', None)
        if txbps_min is None:
            txbps_min = 0

        txbps_max = v.get('txbps_max', None)
        if txbps_max is None:
            txbps_max = 0

        res = {
                'hostname': v['hostname'],
                'accountid': v['accountid'],
                'classname': v['classname'],
                'datekey' : newTime[0],
                'timekey' : newTime[1],
                'dropped': dropped,
                'txpkts': txpkts,
                'txbytes': txbytes,
                'txbps_avg': txbps_avg,
                'txbps_min': txbps_min,
                'txbps_max': txbps_max
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

    sql_rate = 'select mean(value)*8 as avg_value, min(value)*8 as min_value, max(value)*8 as max_value from firewall_qos_rate_stat where '
    sql_rate += ' plugin_instance = \'policy.sh\''
    sql_rate += ' and time > ' + beginTS + ' and time <= ' + endTS
    sql_rate += ' and vm_type = \'firewall\''
    sql_rate += ' and (type = \'sent_bytes\')'
    sql_rate += ' group by type,type_instance,host,account_id;'

    rateData = queryInfluxdb(sql_rate)
    if statsData is None or rateData is None:
        sys.exit(1)

    # print statsData.raw
    stats = normalizeData(statsData, rateData, endDt)
    if len(stats) > 0 :
        print json.dumps({'result' : stats})
    else :
        sys.exit(1)
except Exception, e:
    print str(e)
    sys.exit(1)

sys.exit(0)




