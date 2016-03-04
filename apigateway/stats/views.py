from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from utils.json_response import JSONResponse
import logging
import query

LOG = logging.getLogger(__name__)

def mergeData(rxData, txData):
    rxStats = {}
    if rxData.raw and rxData.raw['series']:
        for item in rxData.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('sum')
            hostname = item['tags']['host']
            value = item['values'][0][valueIndex]
            if value == None:
                value = 0
            rxStats.update({hostname : value})

    txStats = {}
    if txData.raw and txData.raw['series']:
        for item in txData.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('sum')
            hostname = item['tags']['host']
            value = item['values'][0][valueIndex]
            if value == None:
                value = 0
            txStats.update({hostname : value})

    mergeStats = []
    for (k, v) in rxStats.items():
        txVal = txStats.get(k, None)
        if txVal == None:
            txVal = 0
        else:
            txStats.pop(k)
        res = {
                'host': k,
                'unit': 'bytes',
                'rx': v,
                'tx': txVal
            }
        mergeStats.append(res)

    for (k, v) in txStats.items():
        res = {
                'host': k,
                'unit': 'bytes',
                'rx': 0,
                'tx': v
            }
        mergeStats.append(res)

    return mergeStats

def normalizeVPNStats(stats, topn):
    vpnStats = {}
    if stats.raw and stats.raw['series']:
        for item in stats.raw['series']:
            statsColumn = item['columns']
            valueIndex = statsColumn.index('sum')
            usergroup = item['tags']['user_group']
            user = item['tags']['type_instance']
            value = item['values'][0][valueIndex]
            groupData = vpnStats.get(usergroup, None)
            if groupData == None:
                data = {}
                data['group'] = usergroup
                data['unit'] = 'bytes'
                data['value'] = value
                data['users'] = [{'user': user, 'value': value}]
                vpnStats[usergroup] = data
            else:
                groupData['value'] += value
                groupData['users'].append([{'user': user, 'value': value}])

    result = []
    for (k, v) in vpnStats.items():
        data = v['users']
        sortData = sorted(data, key=lambda k: k['value'], reverse=True)
        v['users'] = sortData[:topn]
        result.append(v)

    return result

@api_view(['GET'])
def stats_summary(request, accountId, interval=1440):
    """
    Retrieve the stats summary for FW and VR.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    sql = 'select sum(value) from interface_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'rx_bytes\''
    sql += ' and type_instance = \'eth1\''
    sql += ' group by host;'
    # print sql
    rxStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print rxStatsData
    if rxStatsData  == None:
        LOG.exception('failed to query firewall rx_bytes.')
        return JSONResponse('Failed to query firewall rx_bytes.', status=500)

    sql = 'select sum(value) from interface_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'firewall\''
    sql += ' and type = \'tx_bytes\''
    sql += ' and type_instance = \'eth1\''
    sql += ' group by host;'
    # print sql
    txStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print txStatsData
    if txStatsData == None:
        LOG.exception('failed to query firewall tx_bytes.')
        return JSONResponse('Failed to query firewall tx_bytes.', status=500)

    fwStats = mergeData(rxStatsData, txStatsData)

    sql = 'select sum(value) from interface_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'vrouter\''
    sql += ' and type = \'rx_bytes\''
    sql += ' and type_instance = \'eth1\''
    sql += ' group by host;'
    # print sql
    rxVRStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print rxVRStatsData
    if rxStatsData  == None:
        LOG.exception('failed to query vrouter rx_bytes.')
        return JSONResponse('Failed to query vrouter rx_bytes.', status=500)

    sql = 'select sum(value) from interface_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'vrouter\''
    sql += ' and type = \'tx_bytes\''
    sql += ' and type_instance = \'eth1\''
    sql += ' group by host;'
    # print sql
    txVRStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print txVRStatsData
    if txStatsData  == None:
        LOG.exception('failed to query vrouter tx_bytes.')
        return JSONResponse('Failed to query vrouter tx_bytes.', status=500)

    vrStats = mergeData(rxVRStatsData, txVRStatsData)

    statsSummary = {
        'account_id': accountId,
        'duration': int(interval),
        'fw_stats': fwStats,
        'vr_stats': vrStats
    }

    return JSONResponse({'stats_summary': statsSummary})

@api_view(['GET'])
def vpn_stats_summary(request, accountId, interval=1440):
    """
    Retrieve the stats summary for FW and VR.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    sql = 'select sum(value) from vpn_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'vpn\''
    sql += ' and type = \'in_bytes\''
    sql += ' group by user_group,type_instance;'
    # print sql
    rxStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print rxStatsData
    if rxStatsData == None:
        LOG.exception('failed to query vpn rx_bytes.')
        return JSONResponse('Failed to query vpn rx_bytes.', status=500)

    rxData = normalizeVPNStats(rxStatsData, 10)

    sql = 'select sum(value) from vpn_stat_sum where account_id = '
    sql += '\'' + accountId + '\''
    sql += ' and time > now() - ' + str(interval) + 'm'
    sql += ' and vm_type = \'vpn\''
    sql += ' and type = \'out_bytes\''
    sql += ' group by user_group,type_instance;'
    # print sql
    txStatsData = query.queryInfluxdb(accountId, sql, 'database_default')
    # print txStatsData
    if txStatsData == None:
        LOG.exception('failed to query vpn tx_bytes.')
        return JSONResponse('Failed to query firewall tx_bytes.', status=500)

    txData = normalizeVPNStats(txStatsData, 10)

    statsSummary = {
        'account_id': accountId,
        'duration': int(interval),
        'rx_groups': rxData,
        'tx_groups': txData
    }

    return JSONResponse({'vpn_stats': statsSummary})


