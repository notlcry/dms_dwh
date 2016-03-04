from influxdb import client as influxdb
import config as sysconf

def queryInfluxdb (accountId, sql, databaseName):
    cfgObj = sysconf.SystemConfig()
    config = cfgObj.getConfig()
    serverIp = config.get('InfluxDB', 'server_ip')
    serverPort = config.getint('InfluxDB', 'server_port')
    database = config.get('InfluxDB', databaseName)
    userName = config.get('InfluxDB', 'username')
    password = config.get('InfluxDB', 'password')

    db = influxdb.InfluxDBClient(serverIp, serverPort, userName, password, database, timeout=10)
    try:
        return db.query(sql)
    except:
        return None