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
        sql =  'select dt.date_value as datevalue, tt.time_value as timevalue, sum(fact_table.vpc_traffic_rx_bytes) as rx, sum(fact_table.vpc_traffic_tx_bytes) as tx from fact_vpc_traffic fact_table' + '\n'
    else:
        sql =  'select dt.date_value as datevalue, tt.hours24 as hourvalue, sum(fact_table.vpc_traffic_rx_bytes) as rx, sum(fact_table.vpc_traffic_tx_bytes) as tx from fact_vpc_traffic fact_table' + '\n'
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
        LOG.exception('failed to query vpc traffic stats data.')
        return JSONResponse('Failed to query vpc traffic stats data.', status=500)

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
def bandwidth_by_account(request, accountId, interval=60):
    pass

@api_view(['GET'])
def agg_traffic(request, accountId, interval=1440):
    """
    Retrieve the stats summary for vpc traffic.
    """

    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    tableType = get_table_type(intVal)
    sql =  'select sum(fact_table.vpc_traffic_rx_bytes) as rx, sum(fact_table.vpc_traffic_tx_bytes) as tx from fact_vpc_traffic fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'


    # print sql

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query vpc traffic stats data.')
        return JSONResponse('Failed to query vpc traffic stats data.', status=500)

    resultSet = []
    for row in result:
        rx = row['rx']
        tx = row['tx']
        if rx is None or tx is None:
            continue
        rx = int(rx)
        tx = int(tx)
        item = [rx, tx]
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