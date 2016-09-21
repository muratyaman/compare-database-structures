import sys
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection

class MyDb:

    def __init__ (self):
        self.sshServer    = None
        self.dbEngine     = None
        self.dbConnection = None
    # end constructor

    def connect(config, dbRef):
        defaults  = config['defaults']
        hosts     = config['hosts']
        databases = config['databases']
        users     = config['users']

        db = databases[dbRef]
        dbHostRef  = db['host']
        dbUserRef  = db['user']
        dbName     = db['name']
        dbHostIp   = hosts[dbHostRef]
        dbPort     = defaults['db_port']
        dbUser     = users[dbHostRef]
        dbUserName = dbUser['name']
        dbUserPass = dbUser['password']

        tunnel = db['tunnel']
        sshHostRef  = tunnel['host']
        sshUserRef  = tunnel['user']
        sshHostIp   = hosts[sshHostRef]
        sshPort     = defaults['ssh_port']
        sshUser     = users[sshUserRef]
        sshUserName = sshUser['name']
        sshUserPass = sshUser['password']

        # SSH connection
        self.sshServer = SSHTunnelForwarder(
            sshHostIp,
            ssh_username = sshUserName,
            ssh_password = sshUserPass,
            remote_bind_address=(dbHostIp, dbPort)
        )

        print('SSH tunnel: starting ...')
        self.sshServer.start()
        print('SSH tunnel: started.')

        print('SSH tunnel: ready ')
        print(self.sshServer.local_bind_port)

        # database connection
        dbType = 'mysql+mysqlconnector'
        dbPort = self.sshServer.local_bind_port
        dsn = '{}://{}:{}@{}:{}/{}'.format(
            dbType, dbUserName, dbUserPass, dbHostIp, dbPort, dbName
        )

        print('Database engine: starting ...')
        self.dbEngine = create_engine(dsn)
        print('Database engine: started.')

        print('Database connection: connecting ...')
        self.dbConnection = dbEngine.connect()
        print('Database connection: connected.')

        print('Database version: querying ...')
        result = self.dbConnection.execute("SELECT version() AS version")
        for row in result:
            print("Database version: ", row['version'])

    # end function

    def tables():
        print('Database metadata: loading ...')
        metadata = MetaData()
        metadata.reflect(bind=self.dbEngine)
        print('Database metadata: loaded.')

        inspector = inspect(dbEngine)

        for tbl in metadata.tables:
            print('Table ', tbl)
            columns = inspector.get_columns(tbl)
            pprint.pprint(columns)
    # end function

    def close():
        print('Database connection: closing ...')
        self.dbConnection.close()
        print('Database connection: closed.')

        print('SSH tunnel: stopping ...')
        self.sshServer.stop()
        print('SSH tunnel: stopped.')
    # end function

# end class MyDb

class MyApp:

    def __init__ (self, configFilePath):
        self.config = self.read(configFilePath)
    # end constructor

    def read(configFilePath):
        self.config = None
        print('Config file: opening ...')
        with open(configFilePath) as configFile:
            print('Config file: opened.')
            print('Config json: loading ...')
            self.config = json.load(configFile)
            print('Config json: loaded.')

        # pprint.pprint(config)
        # return self.config
    # end function

    def start():
        compareDbs = self.config['compare']
        for sourceDbRef, targetDbRef in compareDbs.items()
            try:
                sourceDb = MyDb()
                sourceDb.connect(self.config, sourceDbRef)
                sourceTables = sourceDb.tables()
            except Exception as err:
                print('Source database: error: ', err)
            else: # finally
                sourceDb.close()
            # end try catch

            try:
                targetDb = MyDb()
                targetDb.connect(self.config, targetDbRef)
                targetTables = targetDb.tables()
            except Exception as err:
                print('Target database: error: ', err)
            else: # finally
                targetDb.close()
            # end try catch
        # end for loop

    # end function

    def finish():
        self.config = None
    # end function

# end class MyApp

def main():

    try:
        configFilePath = './config.json'
        myApp = MyApp(configFilePath)
        myApp.start()
        myApp.finish()
    except Exception as err:
        print('App error: ', err)

    else: # finally
        print('The End!')
    # end try catch

# end function main

main()
sys.exit(0)
