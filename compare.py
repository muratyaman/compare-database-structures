import logging
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection

# START definitions ============================================================

class MyDb:

    def __init__ (self, dbRef):
        self.dbRef        = dbRef
        logging.debug('DB: ' + self.dbRef + ' > new instance.')
        self.sshServer    = None
        self.dbEngine     = None
        self.dbConnection = None
        self.metadata     = None
    # end constructor
    
    def connect(self, config):
        logging.debug('DB: ' + self.dbRef + ' > connect: reading config ...')
        defaults  = config['defaults']
        hosts     = config['hosts']
        databases = config['databases']
        users     = config['users']

        db = databases[self.dbRef]

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

        logging.debug('DB: ' + self.dbRef + ' > connect: SSH connection instance ...')
        # SSH connection
        self.sshServer = SSHTunnelForwarder(
            sshHostAdrs,
            ssh_username = sshUserName,
            ssh_password = sshUserPass,
            remote_bind_address=(dbHostAdrs, dbPort)
        )

        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: starting ...')
        self.sshServer.start()
        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: started.')

        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: ready port: ' + str(self.sshServer.local_bind_port))

        # database connection
        dbType = 'mysql+mysqlconnector'
        dbHostAdrs = '127.0.0.1'                # override db host
        dbPort = self.sshServer.local_bind_port # override db port
        dsn = '{}://{}:{}@{}:{}/{}'.format(
            dbType, dbUserName, dbUserPass, dbHostAdrs, dbPort, dbName
        )

        logging.debug('DB: ' + self.dbRef + ' > Database engine: starting ...')
        self.dbEngine = create_engine(dsn)
        logging.debug('DB: ' + self.dbRef + ' > Database engine: started.')

        logging.debug('DB: ' + self.dbRef + ' > Database connection: connecting ...')
        self.dbConnection = self.dbEngine.connect()
        logging.info('DB: ' + self.dbRef + ' > Database connection: connected.')

        logging.debug('DB: ' + self.dbRef + ' > Database version: querying ...')
        result = self.dbConnection.execute('SELECT version() AS version')
        for row in result:
            version = str(row['version']) if 'version' in row else 'N/A'
            logging.info('Database version: {}'.format(version))
        # end for
    # end function

    def tables(self):
        logging.debug('DB: ' + self.dbRef + ' > metadata: loading ...')
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.dbEngine)
        logging.debug('DB: ' + self.dbRef + ' > metadata: loaded.')

        logging.debug('DB: ' + self.dbRef + ' > tables: listing ...')
        print('')
        print('DB: ' + self.dbRef + ' > Tables: reading ', end='')
        for tbl in self.metadata.sorted_tables:
            print('.', end='') # continue on same line
            self.table(tbl)
        # end for
        print(' done.', end='')
        print('')
        logging.debug('DB: ' + self.dbRef + ' > tables: listed.')
    # end function

    def table(self, myTable):
        logging.debug('DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' - Columns listing ...')
        #myTable = Table(myTable.name, self.metadata, autoload=True, autoload_with=self.dbEngine)
        for myColumn in myTable.columns:
            logging.debug(
                'DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' Column: ' + myColumn.name +
                ' Type: ' + str(myColumn.type) + ' Nullable: ' + str(myColumn.nullable)
            )
        # end for
        logging.debug('DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' - Columns listed.')
    # end function

    def storeTableColumnInfo(self, table, column):
        logging.debug('DB: ' + self.dbRef + ' > Store info ...')
    # end function

    def close(self):
        logging.debug('DB: ' + self.dbRef + ' > Database connection: closing ...')
        try:
            self.dbConnection.close()
        except Exception as err:
            logging.warning('DB: ' + self.dbRef + ' > Database connection: error on close(): ')
            print(err)
        # end try catch
        logging.debug('DB: ' + self.dbRef + ' > Database connection: closed.')

        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: stopping ...')
        try:
            self.sshServer.stop()
        except Exception as err:
            logging.warning('DB: ' + self.dbRef + ' > SSH tunnel: error on close(): ')
            print(err)
        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: stopped.')
    # end function

# end class MyDb

class MyApp:

    def __init__ (self, configFilePath):
        logging.debug('MyApp: new instance.')
        self.read(configFilePath)
    # end constructor
    
    def read(self, configFilePath):
        self.config = None
        logging.debug('MyApp: Config file: opening ...')
        with open(configFilePath) as configFile:
            logging.debug('MyApp: Config file: opened.')
            logging.debug('MyApp: Config json: loading ...')
            self.config = json.load(configFile)
            logging.debug('MyApp: Config json: loaded.')
            # pprint.pprint(config)
        # end with
    # end function

    def start(self):
        logging.debug('MyApp: starting ...')
        compareDbs = self.config['compare']
        for key in compareDbs:
            sourceDbRef = key
            targetDbRef = compareDbs[key]
            logging.debug('MyApp: comparing: ' + sourceDbRef + ' with ' + targetDbRef)
            try:
                logging.debug('MyApp: loading: ' + sourceDbRef)
                sourceDb = MyDb(sourceDbRef)
                logging.debug('MyApp: loaded: ' + sourceDbRef)
                sourceDb.connect(self.config)
                logging.debug('MyApp: connected: ' + sourceDbRef)
                sourceTables = sourceDb.tables()
            except Exception as err:
                print('Source database: error: ', err)
            else: # finally
                logging.debug('MyApp: closing db: ' + sourceDbRef)
                sourceDb.close()
            # end try catch

            try:
                logging.debug('MyApp: loading: ' + targetDbRef)
                targetDb = MyDb(targetDbRef)
                targetDb.connect(self.config)
                targetTables = targetDb.tables()
            except Exception as err:
                print('Target database: error: ', err)
            else: # finally
                logging.debug('MyApp: closing db: ' + targetDbRef)
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
        print('Running ...')
        logging.basicConfig(filename='compare.log', level=logging.DEBUG)
        configFilePath = './config.json'
        logging.debug('App: loading...')
        myApp = MyApp(configFilePath)
        logging.debug('App: loaded.')
        logging.debug('App: starting...')
        myApp.start()
        logging.debug('App: started.')
        logging.debug('App: finishing...')
        myApp.finish()
        logging.debug('App: finished.')
    except Exception as err:
        print('App error: ', err)
    else: # finally
        print('The End!')
    # end try catch
# end function main

main()
print (exit)
