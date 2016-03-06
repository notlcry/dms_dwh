from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from utils.json_response import JSONResponse
import logging
from utils.query_mysql import query_data
import query
from utils.datetime_util import *

LOG = logging.getLogger(__name__)

@api_view(['GET'])
def traffic_by_account(request, accountId, interval=1440):
    """
    Retrieve the stats summary for vpn traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    tableType = get_table_type(intVal)
    if tableType == '5min':
        sql =  'select dt.date_value as datevalue, tt.time_value as timevalue, sum(fact_table.vpn_traffic_tx_bytes) as tx, sum(fact_table.vpn_traffic_rx_bytes) as rx from fact_vpn_traffic fact_table' + '\n'
    else:
        sql =  'select dt.date_value as datevalue, tt.hours24 as hourvalue, sum(fact_table.vpn_traffic_tx_bytes) as tx, sum(fact_table.vpn_traffic_rx_bytes) as rx from fact_vpn_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    if tableType == '5min':
        sql += 'group by dt.date_key,tt.time_key' + '\n'
        sql += 'order by dt.date_key asc,tt.time_key asc'
    else:
        sql += 'group by dt.date_key,tt.hours24' + '\n'
        sql += 'order by dt.date_key asc,tt.hours24 asc'

    # print sql

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query vpn traffic stats data.')
        return JSONResponse('Failed to query vpn traffic stats data.', status=500)

    rxResult = []
    txResult = []
    for row in result:
        if tableType == '5min':
            timeStr = str(row['datevalue']) + ' ' + str(row['timevalue'])

        else:
            timeStr = get_next_hour(row['datevalue'], row['hourvalue'])
        item = ['rx', timeStr, int(row['rx'])]
        rxResult.append(item)
        item = ['tx', timeStr, int(row['tx'])]
        txResult.append(item)

    resultSet = rxResult + txResult

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "TrafficType"
    }, {
        "colIndex": 1,
        "colType": "String",
        "colName": "Date"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "Value"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def agg_active_user_by_group(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql = 'select grouptable.usergroup_groupname as name, count(distinct usertable.user_id) as value from fact_vpn_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_usergroup grouptable on fact_table.usergroup_key = grouptable.usergroup_key' + '\n'
    sql += 'inner join dim_user usertable on fact_table.user_key = usertable.user_key' + '\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    intVal = int(interval)
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    sql += 'group by grouptable.usergroup_groupname' + '\n'

    activeUser = query_data(sql, 'database_default')
    # print activeUser

    if activeUser == None:
        LOG.exception('failed to query vpn active user data.')
        return JSONResponse('Failed to query vpn active user data.', status=500)

    sql = 'select dim_table.usergroup_groupname as name, count(distinct usertable.user_id) as value from dim_usergroup dim_table' + '\n'
    sql += 'inner join dim_account acctable on (dim_table.account_id = acctable.account_id and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_user usertable on dim_table.usergroup_id = usertable.usergroup_id' + '\n'
    sql += 'group by dim_table.usergroup_groupname' + '\n'

    # print sql

    totalUser = query_data(sql, 'database_default')
    # print totalUser

    if totalUser == None:
        LOG.exception('failed to query vpn total user data.')
        return JSONResponse('Failed to query vpn total user data.', status=500)

    keyMapping = {}
    for row in totalUser:
        key = row['name']
        val = int(row['value'])
        inputData = {}
        inputData['total'] = val
        inputData['active'] = 0
        keyMapping.update({key : inputData})


    for row in activeUser:
        key = row['name']
        val = int(row['value'])
        keyData = keyMapping.get(key, None)
        if keyData is not None:
            totalUser = keyData['total']
            if val > totalUser:
                keyData['active'] = totalUser
            else:
                keyData['active'] = val

    resultSet = []
    for (k, v) in keyMapping.items():
        res = [k, v['active'], v['total']]
        resultSet.append(res)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "Group"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "ActiveUser"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "TotalUser"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def agg_traffic_by_group(request, accountId, interval=1440):
    """
    Retrieve the stats summary for vpn traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql =  'select grouptable.usergroup_groupname as name, sum(fact_table.vpn_traffic_tx_bytes) as tx, sum(fact_table.vpn_traffic_rx_bytes) as rx from fact_vpn_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_usergroup grouptable on fact_table.usergroup_key = grouptable.usergroup_key' + '\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    intVal = int(interval)
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    sql += 'group by grouptable.usergroup_groupname'

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query vpn group data.')
        return JSONResponse('Failed to query vpn group data.', status=500)

    resultSet = []
    for row in result:
        item = [row['name'], int(row['rx']), int(row['tx'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "Group"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "RxBytes"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "TxBytes"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def agg_traffic_by_user(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql =  'select usertable.user_username as name, sum(fact_table.vpn_traffic_tx_bytes) as tx, sum(fact_table.vpn_traffic_rx_bytes) as rx from fact_vpn_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_user usertable on fact_table.user_key = usertable.user_key' + '\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    intVal = int(interval)
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    sql += 'group by usertable.user_username'

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query top n user data.')
        return JSONResponse('Failed to query top n user data.', status=500)

    resultSet = []
    for row in result:
        item = [row['name'], int(row['rx']), int(row['tx'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "User"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "RxBytes"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "TxBytes"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def agg_traffic(request, accountId, interval=1440):
    """
    Retrieve the stats summary for vpn traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql =  'select sum(fact_table.vpn_traffic_tx_bytes) as tx, sum(fact_table.vpn_traffic_rx_bytes) as rx from fact_vpn_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    intVal = int(interval)
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2])
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')'

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query vpn traffic data.')
        return JSONResponse('Failed to query vpn traffic data.', status=500)

    resultSet = []
    for row in result:
        if row['rx'] is None or row['tx'] is None:
            continue
        item = [int(row['rx']), int(row['tx'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "Numeric",
        "colName": "RxBytes"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "TxBytes"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})


@api_view(['GET'])
def session_current(request, accountId, interval=1440):
    """
    Retrieve the stats summary for active session.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    sql = 'select type, last(value) from vpn_stat where '
    sql += ' time > now() - 10m'
    sql += ' and vm_type = \'vpn\''
    sql += ' and (type = \'in_bytes\' or type = \'out_bytes\' or type = \'in_packets\' or type = \'out_packets\' or type = \'in_addtime\')'
    sql += ' group by host,account_id,group_name,user_name;'
    # print sql
    statsData = query.queryInfluxdb(accountId, sql, 'database_raw')
    # print statsData
    if statsData  == None:
        LOG.exception('failed to query active session data.')
        return JSONResponse('Failed to query active session data.', status=500)

    notFound = 'not_found'
    nameMapping = {'in_bytes': 'rxbytes', 'in_packets': 'rxpkts',
                    'out_bytes': 'txbytes', 'out_packets': 'txpkts',
                    'in_addtime': 'duration'}
    keyMapping = {}
    if statsData.raw and statsData.raw['series']:
        for item in statsData.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('value')
            timeIndex = statsColumn.index('time')
            typeIndex = statsColumn.index('type')
            hostname = item['tags']['host']
            if hostname == notFound:
                continue
            username = item['tags']['user_name']
            if username == notFound:
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

    resultSet = []
    for (k, v) in keyMapping.items():
        item = [v['groupname'], v['username'], v['rxbytes'], v['txbytes'], v['rxpkts'], v['txpkts'], v['duration']]
        # resultSet += item
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "Group"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "User"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "RxBytes"
    }, {
        "colIndex": 3,
        "colType": "Numeric",
        "colName": "TxBytes"
    }, {
        "colIndex": 4,
        "colType": "Numeric",
        "colName": "RxPackets"
    }, {
        "colIndex": 5,
        "colType": "Numeric",
        "colName": "TxPackets"
    }, {
        "colIndex": 6,
        "colType": "Numeric",
        "colName": "Duration"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData
    }

    return JSONResponse({'stats': statsResult})
