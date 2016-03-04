from sqlalchemy import create_engine
import config as sysconf

def query_data(sql, databaseName):
    cfgObj = sysconf.SystemConfig()
    config = cfgObj.getConfig()
    serverIp = config.get('Mysql', 'server_ip')
    serverPort = config.getint('Mysql', 'server_port')
    database = config.get('Mysql', databaseName)
    userName = config.get('Mysql', 'username')
    password = config.get('Mysql', 'password')

    url = 'mysql+pymysql://' + userName + ':' + password + '@' + serverIp + ':' + str(serverPort) + '/' + database
    # sql = "select 67.4 as value, 'tx' as name, timestamp('2016-1-1') as time"
    print url
    print sql

    try:
        engine = create_engine(url+'?charset=utf8', encoding='utf8', connect_args={'connect_timeout': 10})
        return engine.execute(sql)
    except Exception as error:
        print error
        return None