from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from utils.json_response import JSONResponse
import logging
from utils.query_mysql import query_data
from utils.datetime_util import *

LOG = logging.getLogger(__name__)

@api_view(['GET'])
def traffic_by_account(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    tableType = get_table_type(intVal)
    if tableType == '5min':
        sql =  'select dt.date_value as datevalue, tt.time_value as timevalue, sum(fact_table.fw_internet_traffic_tx_bytes) as tx, sum(fact_table.fw_internet_traffic_rx_bytes) as rx from fact_fw_internet_traffic fact_table' + '\n'
    else:
        sql =  'select dt.date_value as datevalue, tt.hours24 as hourvalue, sum(fact_table.fw_internet_traffic_tx_bytes) as tx, sum(fact_table.fw_internet_traffic_rx_bytes) as rx from fact_fw_internet_traffic fact_table' + '\n'
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
        LOG.exception('failed to query internet traffic stats data.')
        return JSONResponse('Failed to query internet traffic stats data.', status=500)

    rxResult = []
    txResult = []
    for row in result:
        if tableType == '5min':
            dateVal = row['datevalue']
            timeVal = row['timevalue']
            if dateVal is None or timeVal is None:
                continue
            timeStr = str(dateVal) + ' ' + str(timeVal)

        else:
            dateVal = row['datevalue']
            timeVal = row['hourvalue']
            if dateVal is None or timeVal is None:
                continue
            timeStr = get_next_hour(dateVal, timeVal)
        rx = row['rx']
        tx = row['tx']
        if rx is None and tx is None:
            continue
        if rx is None:
            rx = 0
        else:
            rx = int(rx)
        if tx is None:
            tx = 0
        else:
            tx = int(tx)
        item = ['rx', timeStr, rx]
        rxResult.append(item)
        item = ['tx', timeStr, tx]
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
        'duration': int(interval)
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def bandwidth_by_account(request, accountId, interval=360):
    """
    Retrieve the stats summary for internet bandwidth.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    sql =  'select dt.date_value as datevalue, tt.time_value as timevalue,' + '\n'
    sql += ' fact_table.fw_internet_bandwidth_rx_max as rxmax, fact_table.fw_internet_bandwidth_rx_avg as rxavg, fact_table.fw_internet_bandwidth_rx_min as rxmin,' + '\n'
    sql += ' fact_table.fw_internet_bandwidth_tx_max as txmax, fact_table.fw_internet_bandwidth_tx_avg as txavg, fact_table.fw_internet_bandwidth_tx_min as txmin' + '\n'
    sql += ' from fact_fw_internet_bandwidth fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    sql += 'group by dt.date_key,tt.time_key' + '\n'
    sql += 'order by dt.date_key asc,tt.time_key asc'

    # print sql

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query internet bandwidth stats data.')
        return JSONResponse('Failed to query internet bandwidth stats data.', status=500)

    resultSet = []
    for row in result:
        timeStr = str(row['datevalue']) + ' ' + str(row['timevalue'])

        item = [timeStr, row['rxmax'], row['rxavg'], row['rxmin'], row['txmax'], row['txavg'], row['txmin']]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "Date"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "RxMax"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "RxAvg"
    }, {
        "colIndex": 3,
        "colType": "Numeric",
        "colName": "RxMin"
    }, {
        "colIndex": 4,
        "colType": "Numeric",
        "colName": "TxMax"
    }, {
        "colIndex": 5,
        "colType": "Numeric",
        "colName": "TxAvg"
    }, {
        "colIndex": 6,
        "colType": "Numeric",
        "colName": "TxMin"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval)
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def top5_group_traffic(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql =  'select grouptable.usergroup_groupname as name, sum(fact_table.fw_internet_traffic_tx_bytes) + sum(fact_table.fw_internet_traffic_rx_bytes) as value from fact_fw_internet_traffic fact_table' + '\n'
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
    sql += 'group by grouptable.usergroup_groupname' + '\n'
    sql += 'order by value desc' + '\n'
    sql += 'limit 5'

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query top n group data.')
        return JSONResponse('Failed to query top n group data.', status=500)

    resultSet = []
    for row in result:
        item = [row['name'], int(row['value'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "Group"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "TotalBytes"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})

@api_view(['GET'])
def top5_user_traffic(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql =  'select usertable.user_username as name, sum(fact_table.fw_internet_traffic_tx_bytes) + sum(fact_table.fw_internet_traffic_rx_bytes) as value from fact_fw_internet_traffic fact_table' + '\n'
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
    sql += 'group by usertable.user_username' + '\n'
    sql += 'order by value desc' + '\n'
    sql += 'limit 5'

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query top n user data.')
        return JSONResponse('Failed to query top n user data.', status=500)

    resultSet = []
    for row in result:
        item = [row['name'], int(row['value'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "User"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "TotalBytes"
    }]

    statsResult = {
        'resultset': resultSet,
        'metadata': metaData,
        'duration': int(interval),
    }

    return JSONResponse({'stats': statsResult})