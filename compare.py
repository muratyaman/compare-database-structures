import sys
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection

# START definitions ============================================================

class MyDb:

    def __init__ (self):
        self.sshServer    = None
        self.dbEngine     = None
        self.dbConnection = None
    # end constructor

    def connect(self, config, dbRef):
        defaults  = config['defaults']
        hosts     = config['hosts']
        databases = config['databases']
        users     = config['users']

        db = databases[dbRef]
        dbHostRef  = db['host']
        dbUserRef  = db['user']
        dbName     = db['name']
        dbHostIp   = hosts[dbHostRef]
        dbPort     = defaults['db_port'] #if 'db_port' in defaults else 22
        if 'port' in db:
            dbPort = db['port']
        # end if
        dbUser     = users[dbHostRef]
        dbUserName = dbUser['name']
        dbUserPass = dbUser['password']

        tunnel = db['tunnel']
        sshHostRef  = tunnel['host']
        sshUserRef  = tunnel['user']
        sshHostIp   = hosts[sshHostRef]
        sshPort     = defaults['ssh_port'] #if 'ssh_port' in defaults else 3306
        if 'port' in tunnel:
            sshPort = tunnel['port']
        # end if
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
            version = row['version'] if 'version' in row else 'N/A'
            print("Database version: ", version)
        # end for
    # end function

    def tables(self):
        print('Database metadata: loading ...')
        metadata = MetaData()
        metadata.reflect(bind=self.dbEngine)
        print('Database metadata: loaded.')

        inspector = inspect(dbEngine)

        for tbl in metadata.tables:
            print('Table ', tbl)
            columns = inspector.get_columns(tbl)
            pprint.pprint(columns)
        # end for
    # end function

    def close(self):
        print('Database connection: closing ...')
        try:
            self.dbConnection.close()
        except Exception as err:
            print('Database connection: error on close(): ', err)
        # end try catch
        print('Database connection: closed.')

        print('SSH tunnel: stopping ...')
        self.sshServer.stop()
        print('SSH tunnel: stopped.')
    # end function

# end class MyDb

class MyApp:

    def __init__ (self, configFilePath):
        print('MyApp: new instance.')
        self.read(configFilePath)
    # end constructor

    def read(self, configFilePath):
        self.config = None
        print('Config file: opening ...')
        with open(configFilePath) as configFile:
            print('Config file: opened.')
            print('Config json: loading ...')
            self.config = json.load(configFile)
            print('Config json: loaded.')
            # pprint.pprint(config)
        # end with
    # end function

    def start(self):
        compareDbs = self.config['compare']
        for key in compareDbs:
            sourceDbRef = key
            targetDbRef = compareDbs[key]
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

    def finish(self):
        self.config = None
    # end function

# end class MyApp

# END definitions ==============================================================

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
