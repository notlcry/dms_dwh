from rest_framework.decorators import api_view
from utils.json_response import JSONResponse
import query
import logging

LOG = logging.getLogger(__name__)

def normalizeServiceState(data):
    hosts = {}
    hosttype = {}
    accountName = ''
    for item in data['result']:
        groups = item['groups']
        if groups and len(groups) > 0:
            accountName = groups[0]['name']

        value = int(item['value'])
        priority = item['priority']
        severity = int(priority)
        if value == 0:
            severity = 7

        for host in item['hosts']:
            hosttype[host['host']] = host['description']
            each = hosts.get(host['host'], None)
            if not each:
                hosts[host['host']] = severity
            elif hosts[each] > severity:
                hosts[each] = severity

    result = []
    for (k, v) in hosts.items():
        item = {'host': k, 'severity': v, 'type': hosttype.get(k, '')}
        result.append(item)
    return (accountName, result)


@api_view(['GET'])
def fault_summary(request, accountId):
    """
    Retrieve the service state.
    """
    if not accountId or len(accountId) == 0:
        return JSONResponse('No input parameter account_id.', status=400)

    token = query.getZabbixToken()
    print token
    if not token:
        LOG.exception('failed to query zabbix for token.')
        return JSONResponse('Failed to query zabbix for token.', status=500)

    data = query.getServiceState(accountId, token)
    print data
    if data == None:
        LOG.exception('failed to query zabbix for service state.')
        return JSONResponse('Failed to query zabbix for service state.', status=500)

    serviceData = normalizeServiceState(data)
    print serviceData
    result = {'service_state': {
        'account_id': accountId,
        'account_name': serviceData[0],
        'hosts': serviceData[1]
    }}
    return JSONResponse(result)