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
    Retrieve the stats summary for fw qos stats data.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    intVal = int(interval)
    sql =  'select fact_table.fw_qos_class_name as classname, sum(fact_table.fw_qos_sent_bytes) as txbytes, sum(fact_table.fw_qos_sent_packets) as txpkts, sum(fact_table.fw_qos_drop_packets) as droppkts from fact_fw_qos fact_table' + '\n'
    sql += 'inner join dim_account acctable on (fact_table.account_key = acctable.account_key and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_date dt on fact_table.date_key = dt.date_key' + '\n'
    sql += 'inner join dim_time tt on fact_table.time_key = tt.time_key' + '\n'
    beginDate = get_begin_date(intVal)
    if beginDate[0] == 0:
        sql += 'where fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + '\n'
    else:
        sql += 'where (fact_table.date_key = ' + str(beginDate[1]) + ' and fact_table.time_key >= ' + str(beginDate[2]) + ')'
        sql += ' or (fact_table.date_key > ' + str(beginDate[1]) + ')' + '\n'
    sql += 'group by fact_table.fw_qos_class_name'

    # print sql

    result = query_data(sql, 'database_default')

    if result == None:
        LOG.exception('failed to query fw qos stats data.')
        return JSONResponse('Failed to query fw qos stats data.', status=500)

    resultSet = []
    for row in result:
        item = [row['classname'], int(row['txbytes']), int(row['txpkts']), int(row['droppkts'])]
        resultSet.append(item)

    metaData = [{
        "colIndex": 0,
        "colType": "String",
        "colName": "ClassType"
    }, {
        "colIndex": 1,
        "colType": "Numeric",
        "colName": "SentBytes"
    }, {
        "colIndex": 2,
        "colType": "Numeric",
        "colName": "SentPkts"
    }, {
        "colIndex": 3,
        "colType": "Numeric",
        "colName": "DropPkts"
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