from sqlalchemy import create_engine
import sys

def query_data(sql):
    # global server_ip
    # global server_port
    # global database
    # global username
    # global password

    server_ip = '172.19.10.15'
    server_port = '3306'
    database= 'zabbix'
    username = 'zabbix'
    password = 'zabbix'

    url = 'mysql+pymysql://' + username + ':' + password + '@' + server_ip + ':' + str(server_port) + '/' + database
    # sql = "select 67.4 as value, 'tx' as name, timestamp('2016-1-1') as time"
    print url
    try:
        engine = create_engine(url, encoding='utf8', connect_args={'connect_timeout': 10})
        return engine.execute(sql)
    except Exception as error:
        print error
        return None


def get_alarm():

    sql = "select groups.accountid accountId, hosts.name hostname, gast.priority priority, gast.pcout pcout from\n"
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
        account_id = row[0]
        host_name = row[1]
        priority = row[2]
        pcout = row[3]
        metaData.append(dict(account_id=account_id, host_name=host_name, priority=priority, pcout=pcout))
        if account_host_map.get(account_id+'@'+host_name, None) is None:
            account_host_map[account_id+'@'+host_name] = [].append(metaData)
        else:
            account_host_map.get(account_id+'@'+host_name).append(metaData)


    resultSet = []

    for (k, v) in account_host_map.items():
        pri_ary = [0, 0, 0, 0, 0, 0]
        for e in v:
            if e.get('priority', None) is not None:
                pri_ary[e.get('priority')] = e.get('pcout')




"""
Main Part
"""
try:
   result = get_alarm()
   print result
except Exception, e:
    print str(e)
    sys.exit(1)
sys.exit(0)
