import sys
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection

# START definitions ============================================================

class MyDb:

    def __init__ (self):
        print('MyDb new instance.')
        self.sshServer    = None
        self.dbEngine     = None
        self.dbConnection = None
        self.metadata     = None
    # end constructor

    def connect(self, config, dbRef):
        print('MyDb connect: reading config ...')
        defaults  = config['defaults']
        hosts     = config['hosts']
        databases = config['databases']
        users     = config['users']

        db = databases[dbRef]

        tunnel = db['tunnel']
        sshHostRef  = tunnel['host']
        sshUserRef  = tunnel['user']
        sshHost     = hosts[sshHostRef]
        sshHostAdrs = sshHost['address']
        sshPort     = defaults['ssh_port'] #if 'ssh_port' in defaults else 3306
        if 'port' in sshHost:
            sshPort = sshHost['port']
        # end if
        sshUser     = users[sshUserRef]
        sshUserName = sshUser['name']
        sshUserPass = sshUser['password']

        dbHostRef  = db['host']
        dbUserRef  = db['user']
        dbName     = db['name']
        dbHost     = hosts[dbHostRef]
        dbHostAdrs = dbHost['address']
        dbPort     = defaults['db_port'] #if 'db_port' in defaults else 22
        if 'port' in dbHost:
            dbPort = dbHost['port']
        # end if
        dbUser     = users[dbUserRef]
        dbUserName = dbUser['name']
        dbUserPass = dbUser['password']

        print('MyDb connect: SSH connection instance ...')
        # SSH connection
        self.sshServer = SSHTunnelForwarder(
            sshHostAdrs,
            ssh_username = sshUserName,
            ssh_password = sshUserPass,
            remote_bind_address=(dbHostAdrs, dbPort)
        )

        print('SSH tunnel: starting ...')
        self.sshServer.start()
        print('SSH tunnel: started.')

        print('SSH tunnel: ready port: ', self.sshServer.local_bind_port)

        # database connection
        dbType = 'mysql+mysqlconnector'
        dbPort = self.sshServer.local_bind_port
        dsn = '{}://{}:{}@{}:{}/{}'.format(
            dbType, dbUserName, dbUserPass, dbHostAdrs, dbPort, dbName
        )

        print('Database engine: starting ...')
        self.dbEngine = create_engine(dsn)
        print('Database engine: started.')

        print('Database connection: connecting ...')
        self.dbConnection = self.dbEngine.connect()
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
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.dbEngine)
        print('Database metadata: loaded.')

        for tblName in self.metadata.sorted_tables:
            self.table(tblName)
        # end for
    # end function

    def table(self, tableName):
        print('Table: ', tableName, ' columns listing ...')
        myTable = Table(tableName, self.metadata, autoload=True, autoload_with=self.dbEngine)
        for myColumn in myTable.columns:
            print('Table: ', tableName, ' Column: ', myColumn.name, ' Type: ', myColumn.type, 'Nullable: ', myColumn.nullable)
        # end for
        print('Table: ', tableName, ' columns listed.')
    # end function

    def storeTableColumnInfo(self, table, column):
        print('Store info ...')
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
            #try:
            if 1:
                sourceDb = MyDb()
                sourceDb.connect(self.config, sourceDbRef)
                sourceTables = sourceDb.tables()
            #except Exception as err:
            #    print('Source database: error: ', err)
            #else: # finally
                sourceDb.close()
            # end try catch

            #try:
            if 1:
                targetDb = MyDb()
                targetDb.connect(self.config, targetDbRef)
                targetTables = targetDb.tables()
            #except Exception as err:
            #    print('Target database: error: ', err)
            #else: # finally
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

    #try:
    if 1:
        configFilePath = './config.json'
        myApp = MyApp(configFilePath)
        myApp.start()
        myApp.finish()
    #except Exception as err:
    #    print('App error: ', err)
    #else: # finally
    #    print('The End!')
    # end try catch

# end function main

main()
sys.exit(0)
