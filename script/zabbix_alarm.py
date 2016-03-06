#!/usr/bin/python

from sqlalchemy import create_engine
import sys
import json
import traceback
from ConfigParser import SafeConfigParser
import os
from datetime import datetime


def query_data(sql):
    global server_ip
    global server_port
    global database
    global username
    global password

    url = 'mysql+pymysql://' + username + ':' + password + '@' + server_ip + ':' + str(server_port) + '/' + database
    # print url
    try:
        engine = create_engine(url, encoding='utf8', connect_args={'connect_timeout': 10})
        return engine.execute(sql)
    except Exception as error:
        print error
        return None

def getTime():
    dt = dt = datetime.now()
    date_key = str(dt.year) + ('0' if dt.month < 10 else '') + str(dt.month) + ('0' if dt.day < 10 else '') + str(dt.day)
    date_key = int(date_key)
    time_key = ('0' if dt.hour < 10 else '') + str(dt.hour) + ('0' if dt.minute < 10 else '') + str(dt.minute) + ('0' if dt.second < 10 else '') + str(dt.second)
    time_key = int(time_key)

    return (date_key, time_key,)

def get_alarm():
    sql = "select groups.accountid as accountId, hosts.name as hostname, gast.priority as priority, gast.pcout as pcout from\n"
    sql += "  (select ghp.groupid, ghp.hostid, ghp.priority, count(*) pcout from\n"
    sql += "  (select gteva.groupid groupid, gteva.hostid hostid, gteva.triggerid, gteva.priority priority,max(gteva.eventid) from (\n"
    sql += "    select gtvd.groupid groupid, gtvd.hostid hostid, gtvd.triggerid, evt.eventid, gtvd.priority priority from\n"
    sql += "      (select DISTINCT(gtv.itemid), gtv.groupid groupid, gtv.hostid hostid, gtv.triggerid triggerid,gtv.priority priority from\n"
    sql += "        (select gt.groupid groupid, gt.hostid hostid, gt.itemid,  gt.triggerid,trg.priority priority from(\n"
    sql += "             select gi.groupid groupid, gi.hostid hostid, gi.itemid itemid, fn.triggerid triggerid from  (select gs.groupid groupid, hg.hostid hostid, its.itemid from groups gs, hosts_groups hg, items its\n"
    sql += "               where gs.groupid = hg.groupid\n"
    sql += "                  and its.hostid = hg.hostid) gi left join functions fn on gi.itemid = fn.itemid) gt, triggers trg\n"
    sql += "                    where gt.triggerid is not null\n"
    sql += "                          and trg.triggerid = gt.triggerid\n"
    sql += "                          and trg.value = 1) gtv) gtvd, events evt\n"
    sql += "                where gtvd.triggerid = evt.objectid\n"
    sql += "                      and evt.acknowledged = 0\n"
    sql += "                      and evt.object = 0\n"
    sql += "                      and evt.source = 0\n"
    sql += "                      and evt.value = 1\n"
    sql += "              )gteva group by gteva.groupid, gteva.hostid, gteva.triggerid, gteva.priority) ghp\n"
    sql += "  GROUP BY ghp.groupid, ghp.hostid, ghp.priority) gast, groups, hosts\n"
    sql += "  where gast.groupid = groups.groupid and gast.hostid = hosts.hostid"

    # print sql

    data = query_data(sql)

    if data == None:
        print 'failed to query total user data.'
        return 'Failed to query total user data.'

    account_host_map = {}

    metaData = []
    for row in data:
        account_id = row['accountId']
        host_name = row['hostname']
        priority = row['priority']
        pcout = row['pcout']
        metaData.append(dict(account_id=account_id, host_name=host_name, priority=priority, pcout=pcout))
        if account_host_map.get(account_id + '@' + host_name, None) is None:
            pri_set=[]
            pri_set.append(dict(priority=priority, count=pcout))
            account_host_map[account_id + '@' + host_name] = pri_set
        else:
            account_host_map.get(account_id + '@' + host_name).append(metaData)

    resultSet = []
    dtVal = getTime()

    for (k, v) in account_host_map.items():
        pri_ary = [0, 0, 0, 0, 0, 0, 0]
        for e in v:
            if e.get('priority', None) is not None:
                pri_ary[e.get('priority')] = e.get('count')
        resultSet.append(merge(k, pri_ary, dtVal))

    return resultSet


def merge(account_host, pri_array, dtVal):
    account_id = account_host.split("@")[0]
    host_name = account_host.split("@")[1]
    result = dict(account_id=account_id, host_name=host_name,
                  critical=pri_array[5],
                  major=pri_array[4],
                  minor=pri_array[3],
                  warn=pri_array[2],
                  info=pri_array[1],
                  clear=pri_array[0],
                  datekey=dtVal[0],
                  timekey=dtVal[1])
    return result


"""
Main Part
"""
try:
    config = SafeConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), "system.conf")
    config.read(config_file)

    server_ip = config.get('Mysql', 'server_ip')
    server_port = config.get('Mysql', 'server_port')
    database = config.get('Mysql', 'database')
    username = config.get('Mysql', 'username')
    password = config.get('Mysql', 'password')

    resultSet = get_alarm()
    if resultSet is None:
        sys.exit(1)
    else:
        print json.dumps({'result': resultSet})
except Exception, e:
    print str(e)
    print traceback.print_exc()
    sys.exit(1)
sys.exit(0)
