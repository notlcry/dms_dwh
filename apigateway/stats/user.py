
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
def active_user_by_group(request, accountId, interval=1440):
    """
    Retrieve the stats summary for internet traffic.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    accountIdStr = '\'' + str(accountId) + '\''
    sql = 'select grouptable.usergroup_groupname as name, count(distinct usertable.user_id) as value from fact_vr_user_traffic fact_table' + '\n'
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
        LOG.exception('failed to query active user data.')
        return JSONResponse('Failed to query active user data.', status=500)

    sql = 'select dim_table.usergroup_groupname as name, count(distinct usertable.user_id) as value from dim_usergroup dim_table' + '\n'
    sql += 'inner join dim_account acctable on (dim_table.account_id = acctable.account_id and acctable.account_id = ' + accountIdStr + ')\n'
    sql += 'inner join dim_user usertable on dim_table.usergroup_id = usertable.usergroup_id' + '\n'
    sql += 'group by dim_table.usergroup_groupname' + '\n'

    # print sql

    totalUser = query_data(sql, 'database_default')
    # print totalUser

    if totalUser == None:
        LOG.exception('failed to query total user data.')
        return JSONResponse('Failed to query total user data.', status=500)

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
        # res = [k, v['active'], v['total']]
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
