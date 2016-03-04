import config as sysconf
import requests

def getZabbixToken ():
    cfgObj = sysconf.SystemConfig()
    config = cfgObj.getConfig()
    serverIp = config.get('Zabbix', 'server_ip')
    serverPort = config.getint('Zabbix', 'server_port')
    userName = config.get('Zabbix', 'username')
    password = config.get('Zabbix', 'password')
    authData = {'jsonrpc': '2.0', 'method': 'user.login',
                'params': {'user': userName, 'password': password}, 'id': 0}
    url = 'http://' + serverIp + '/zabbix/api_jsonrpc.php'
    try:
        r = requests.post(url, json=authData)
        if r.ok and len(r.text) > 0:
            return r.json()['result']
        else:
            return None
    except:
        return None

def getServiceState(accountId, token):
    cfgObj = sysconf.SystemConfig()
    config = cfgObj.getConfig()
    serverIp = config.get('Zabbix', 'server_ip')
    data = {'jsonrpc': '2.0', 'method': 'trigger.get', 'params': {'output': ['value', 'priority'],
            'accountid': accountId, 'selectHosts': ['host'], 'selectGroups': ['name']},
            'auth': token, 'id': 1}
    url = 'http://' + serverIp +'/zabbix/api_jsonrpc.php'
    try:
        r = requests.post(url, json=data)
        if r.ok and len(r.text) > 0:
            return r.json()
        else:
            return None
    except:
        return None
