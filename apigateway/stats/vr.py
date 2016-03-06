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
    Retrieve the stats summary for vr traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    tableType = get_table_type(intVal)

    # if tableType == '5min':
    #     sql =  'select dt.date_value as datevalue, tt.time_value as timevalue, sum(fact_table.vr_user_traffic_tx_bytes) as tx, sum(fact_table.vr_user_traffic_rx_bytes) as rx from fact_vr_user_traffic fact_table' + '\n'
    # else:
    #     sql =  'select dt.date_value as datevalue, tt.hours24 as hourvalue, sum(fact_table.vr_user_traffic_tx_bytes) as tx, sum(fact_table.vr_user_traffic_rx_bytes) as rx from fact_vr_user_traffic fact_table' + '\n'
    # sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    # sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    # sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'




    if tableType == '5min':
        sql = "select traffic_acc.datevalue as datevalue, traffic_acc.timevalue as timevalue, sum(traffic_acc.tx) as tx , sum(traffic_acc.rx) as rx from("
        sql += 'select dt.date_value as datevalue, tt.time_value as timevalue, sum(fact_table_user.vr_user_traffic_tx_bytes) as tx, sum(fact_table_user.vr_user_traffic_rx_bytes) as rx from fact_vr_user_traffic fact_table_user' + '\n'
    else:
        sql = "select traffic_acc.datevalue as datevalue, traffic_acc.hourvalue as hourvalue, sum(traffic_acc.tx) as tx, sum(traffic_acc.rx) as rx from("
        sql += 'select dt.date_value as datevalue, tt.hours24 as hourvalue, sum(fact_table_user.vr_user_traffic_tx_bytes) as tx, sum(fact_table_user.vr_user_traffic_rx_bytes) as rx from fact_vr_user_traffic fact_table_user' + '\n'
    sql += 'inner join dim_account acctable on (fact_table_user.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table_user.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table_user.time_key = tt.time_key' + '\n'
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table_user.date_key = ' + str(beginDate[1]) + ' and fact_table_user.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table_user.date_key = ' + str(beginDate[1]) + ' and fact_table_user.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table_user.date_key > ' + str(beginDate[1]) + ')' + '\n'
    if tableType == '5min':
        sql += 'group by dt.date_key,tt.time_key' + '\n'
    else:
        sql += 'group by dt.date_key,tt.hours24' + '\n'

    sql += 'UNION ALL '
    # group
    if tableType == '5min':
        sql += 'select dt2.date_value as datevalue, tt2.time_value as timevalue, sum(fact_group_table.vr_group_traffic_tx_bytes) as tx, sum(fact_group_table.vr_group_traffic_rx_bytes) as rx from fact_vr_group_traffic fact_group_table' + '\n'
    else:
        sql += 'select dt2.date_value as datevalue, tt2.hours24 as hourvalue, sum(fact_group_table.vr_group_traffic_tx_bytes) as tx, sum(fact_group_table.vr_group_traffic_rx_bytes) as rx from fact_vr_group_traffic fact_group_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_group_table.account_key = acctable.account_key and acctable.account_id = '+ accountIdStr + ')\n'
    sql += 'inner join dim_date dt2 on fact_group_table.date_key = dt2.date_key' + '\n'
    sql += 'inner join dim_time tt2 on fact_group_table.time_key = tt2.time_key' + '\n'

    if beginDate[0] == 0:
        sql += 'where fact_group_table.date_key = ' + str(beginDate[1]) + ' and fact_group_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_group_table.date_key = ' + str(beginDate[1]) + ' and fact_group_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_group_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    if tableType == '5min':
        sql += ' group by dt2.date_key,tt2.time_key' + '\n'
    else:
        sql += ' group by dt2.date_key,tt2.hours24' + '\n'
    sql += ') traffic_acc'


    if tableType == '5min':
        sql += ' group by traffic_acc.datevalue, traffic_acc.timevalue'
        sql += ' order by datevalue asc, timevalue asc'
    else:
        sql += ' group by traffic_acc.datevalue, traffic_acc.hourvalue'
        sql += ' order by datevalue asc,hourvalue asc'

    # print sql

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query vr traffic stats data.')
        return JSONResponse('Failed to query vr traffic stats data.', status=500)

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