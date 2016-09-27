import sys
import argparse
import logging
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlite3 import dbapi2 as sqlite
#from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm import sessionmaker

# START definitions ============================================================

class MyDbLocal:
    
    def __init__ (self):
        self.dbEngine     = None
        self.dbConnection = None
        self.dbCursor     = None
    # end constructor
    
    def connect(self, dbFile):
        self.dbConnection = sqlite.connect(dbFile)
        self.dbCursor     = self.dbConnection.cursor()
    # end function
    
    
    def resetAll(self):
        logging.debug('resetAll()...')
        self.dbConnection.execute('DELETE FROM my_tables;')
        self.dbConnection.execute('DELETE FROM my_databases;')
        self.dbConnection.execute('DELETE FROM my_hosts;')
        self.dbConnection.commit()
    # end function
    
    def hostReset(self, hostRef):
        logging.debug('hostReset({})...'.format(hostRef))
        deleteSql = 'DELETE FROM my_hosts WHERE host_ref = ?;'
        params = (hostRef,)
        result = self.dbCursor.execute(deleteSql, params)
        self.dbConnection.commit()
    # end function
    
    def hostConnecting(self, hostRef):
        logging.debug('hostConnecting({}) ...'.format(hostRef))
        insertSql = '''
            INSERT INTO my_hosts
                   (host_ref)
            VALUES (?);
        '''
        params = (hostRef,)
        result = self.dbCursor.execute(insertSql, params)
        self.dbConnection.commit()
    # end function
    
    def hostConnected(self, hostRef):
        logging.debug('hostConnected({}) ...'.format(hostRef))
        updateSql = '''
            UPDATE my_hosts
            SET last_connected = DATETIME('now')
            WHERE host_ref = ?;
        '''
        params = (hostRef,)
        result = self.dbCursor.execute(updateSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbReset(self, dbRef):
        logging.debug('dbReset({}) ...'.format(dbRef))
        deleteSql = 'DELETE FROM my_databases WHERE db_ref = ?;'
        params = (dbRef,)
        result = self.dbCursor.execute(deleteSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbConnecting(self, dbRef):
        logging.debug('dbConnecting({}) ...'.format(dbRef))
        insertSql = '''
            INSERT INTO my_databases
                   (db_ref, last_connected, success)
            VALUES (?, NULL, 0);
        '''
        params = (dbRef,)
        result = self.dbCursor.execute(insertSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbConnected(self, dbRef):
        logging.debug('dbConnected({}) ...'.format(dbRef))
        insertSql = '''
            UPDATE my_databases
            SET last_connected = DATETIME('now')
            WHERE db_ref = ?;
        '''
        params = (dbRef,)
        result = self.dbCursor.execute(insertSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbSuccess(self, dbRef):
        logging.debug('dbSuccess({}) ...'.format(dbRef))
        updateSql = '''
            UPDATE my_databases
            SET success = 1
            WHERE db_ref = :db_ref;
        '''
        params = (dbRef,)
        result = self.dbCursor.execute(updateSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbIsCached(self, dbRef):
        logging.debug('dbIsCached({}) ...'.format(dbRef))
        isCached = 0
        selectSql = '''
            SELECT * FROM my_databases
            WHERE (db_ref = :db_ref)
              AND (success = 1)
              AND (DATETIME('now', '-1 day') < last_connected);
        '''
        params = (dbRef,)
        self.dbCursor.execute(selectSql, params)
        row = self.dbCursor.fetchone()
        if row:
            isCached = 1
        # end if
        return isCached;
    # end function
    
    def dbResetTables(self, dbRef):
        logging.debug('dbResetTables({}) ...'.format(dbRef))
        deleteSql = 'DELETE FROM my_tables WHERE db_ref = ?;'
        params = (dbRef,)
        result = self.dbCursor.execute(deleteSql, params)
        self.dbConnection.commit()
    # end function
    
    def dbSaveTableColumns(self, dbRef, schemaName, table):
        logging.debug('dbSaveTableColumns({}) ...'.format(dbRef))
        insertSql = '''
            INSERT INTO my_tables
                   (db_ref, schema_name, table_name, column_name, column_type, column_nullable)
            VALUES (?, ?, ?, ?, ?, ?);
        '''
        params = []
        for column in table.columns:
            logging.debug('DB: ' + dbRef + ' >> Table: ' + table.name + ' Column: ' + column.name + ' Type: ' + str(column.type))
            row = (dbRef, schemaName, table.name, column.name, str(column.type), int(1 if column.nullable else 0))
            params.append(row)
        # end for
        
        result = self.dbCursor.executemany(insertSql, params)
        self.dbConnection.commit()
    # end function
    
    def compare(self, sourceDbRef, targetDbRef):
        print('Compare: ' + sourceDbRef + ' vs ' + targetDbRef)
        print('db_ref, schema, table, column, type, target_column_type')
        selectSql = '''
        SELECT s.db_ref, s.schema_name, s.table_name, s.column_name, s.column_type, t.column_type as target_column_type
        FROM (
          SELECT s0.*
          FROM my_tables s0
          WHERE s0.db_ref = ?
        ) s
        LEFT JOIN (
          SELECT t0.*
          FROM my_tables t0
          WHERE t0.db_ref = ?
        ) t ON s.schema_name = t.schema_name AND s.table_name = t.table_name AND s.column_name = t.column_name
        WHERE t.id IS NULL OR s.column_type <> t.column_type
        ORDER BY 1, 2, 3, 4
        '''
        rows = self.dbConnection.execute(selectSql, (sourceDbRef, targetDbRef))
        for row in rows:
            print(row)
        # end for loop
    # end function
    
    def close(self):
        logging.debug('Local database connection: closing ...')
        try:
            self.dbCursor.close()
            self.dbConnection.close()
            logging.debug('Local database connection: closed.')
        except Exception as err:
            logging.warning('Local database connection: error on close(): ')
            print(err)
        # end try catch
        self.dbEngine     = None
        self.dbConnection = None
        self.dbCursor     = None
    # end function
# end class MyDbLocal

class MyDb:
    '''
    To read database structure: tables and columns
    '''
    
    # constructor
    def __init__ (self, dbRef, dbLocal):
        self.dbRef        = dbRef
        logging.debug('DB: ' + self.dbRef + ' > new instance.')
        self.sshServer    = None
        self.dbEngine     = None
        self.dbConnection = None
        self.metadata     = None
        self.dbLocal      = dbLocal
    # end constructor
    
    # connect to SSH and DB
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
        sshPort     = defaults['ssh_port']
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
        dbPort     = defaults['db_port']
        if 'port' in dbHost:
            dbPort = dbHost['port']
        # end if
        dbUser     = users[dbUserRef]
        dbUserName = dbUser['name']
        dbUserPass = dbUser['password']

        self.dbLocal.hostReset(sshHostRef) # ** local cache **
        self.dbLocal.dbReset(self.dbRef)   # ** local cache **
        
        self.dbLocal.hostConnecting(sshHostRef) # ** local cache **
        self.dbLocal.dbConnecting(self.dbRef)   # ** local cache **
        
        logging.debug('DB: ' + self.dbRef + ' > SSH connection instance ...')
        # SSH connection
        self.sshServer = SSHTunnelForwarder(
            sshHostAdrs,
            ssh_username = sshUserName,
            ssh_password = sshUserPass,
            remote_bind_address=(dbHostAdrs, dbPort)
        )
        
        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: starting ...')
        self.sshServer.start()
        logging.info('DB: ' + self.dbRef + ' > SSH tunnel: started - port: ' + str(self.sshServer.local_bind_port))
        print('DB: ' + self.dbRef + ' > SSH tunnel: started - port: ' + str(self.sshServer.local_bind_port))
        self.dbLocal.hostConnected(sshHostRef) # ** local cache **
        
        # database connection
        dbType     = 'mysql+mysqlconnector'
        dbHostAdrs = '127.0.0.1'                # override db host
        dbPort     = self.sshServer.local_bind_port # override db port
        dsn        = '{}://{}:{}@{}:{}/{}'.format(
            dbType, dbUserName, dbUserPass, dbHostAdrs, dbPort, dbName
        )

        logging.debug('DB: ' + self.dbRef + ' > Database engine: starting ...')
        self.dbEngine = create_engine(dsn)
        logging.debug('DB: ' + self.dbRef + ' > Database engine: started.')
        logging.debug('DB: ' + self.dbRef + ' > Database connection: connecting ...')
        self.dbConnection = self.dbEngine.connect()
        logging.info('DB: ' + self.dbRef + ' > Database connection: connected.')
        print('DB: ' + self.dbRef + ' > Database connection: connected.')
        self.dbLocal.dbConnected(self.dbRef) # ** local cache **
        
        '''
        logging.debug('DB: ' + self.dbRef + ' > Database version: querying ...')
        result = self.dbConnection.execute('SELECT version() AS version')
        for row in result:
            version = str(row['version']) if 'version' in row else 'N/A'
            logging.info('Database version: {}'.format(version))
        # end for
        '''
    # end function

    # read tables
    def tables(self):
        logging.debug('MyApp: reset table info for ' + self.dbRef)
        self.dbLocal.dbResetTables(self.dbRef) # ** local cache **
        logging.debug('MyApp: reset table info for ' + self.dbRef + ' done')
        
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
        
        self.dbLocal.dbSuccess(self.dbRef) # ** local cache **
    # end function

    # read table columns
    def table(self, myTable):
        logging.debug('DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' - Columns listing ...')
        self.dbLocal.dbSaveTableColumns(self.dbRef, 'public', myTable) # ** local cache **
        logging.debug('DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' - Columns listed.')
    # end function

    def close(self):
        logging.debug('DB: ' + self.dbRef + ' > Database connection: closing ...')
        try:
            self.dbConnection.close()
            logging.debug('DB: ' + self.dbRef + ' > Database connection: closed.')
        except Exception as err:
            logging.warning('DB: ' + self.dbRef + ' > Database connection: error on close(): ')
            print(err)
        # end try catch

        logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: stopping ...')
        try:
            self.sshServer.stop()
            logging.debug('DB: ' + self.dbRef + ' > SSH tunnel: stopped.')
        except Exception as err:
            logging.warning('DB: ' + self.dbRef + ' > SSH tunnel: error on close(): ')
            print(err)
        # end try catch
    # end function

# end class MyDb

class MyApp:

    def __init__ (self, configFilePath):
        logging.debug('MyApp: new instance.')
        self.read(configFilePath)
        self.dbLocal = None
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

    def connectToLocalDb(self):
        self.dbLocal = MyDbLocal()
        logging.debug('MyApp: connecting to local db ...')
        self.dbLocal.connect('./db.sqlite3');
        logging.debug('MyApp: connected to local db.')
    # end function
    
    def loadDbStructure(self, dbRef):
        try:
            dbIsCached = self.dbLocal.dbIsCached(dbRef)
            if dbIsCached:
                logging.debug('MyApp: DB: ' + dbRef + ' is cached already')
                return False
            # end if
            logging.debug('MyApp: loading: ' + dbRef)
            db = MyDb(dbRef, self.dbLocal)
            logging.debug('MyApp: loaded: ' + dbRef)
            
            logging.debug('MyApp: connecting: ' + dbRef)
            db.connect(self.config)
            logging.info('MyApp: connected: ' + dbRef)
            db.tables()
        except Exception as err:
            print('Database: error: ', err)
        else: # finally
            logging.debug('MyApp: closing db: ' + dbRef)
            db.close()
        # end try catch
        db = None
        return True
    # end function
    
    def compareDbStructure(self, sourceDbRef, targetDbRef):
        self.dbLocal.compare(sourceDbRef, targetDbRef)
    # end function
    
    def cmdClearCache(self):
        self.dbLocal.resetAll()
    # end function
    
    def cmdLoadAll(self):
        dbs = self.config['databases']
        for dbRef in dbs:
            self.loadDbStructure(dbRef)
        # end for loop
    # end function
    
    def cmdCompareAll(self):
        compareDbs = self.config['compare']
        for key in compareDbs:
            sourceDbRef = key
            targetDbRef = compareDbs[key]
            logging.debug('MyApp: comparing: ' + sourceDbRef + ' with ' + targetDbRef)
            self.compareDbStructure(sourceDbRef, targetDbRef)
        # end for loop
    # end function
    
    # start running app
    def start(self, command):
        logging.debug('MyApp: starting ...')
        
        self.connectToLocalDb()
        
        # get command from user
        if command == 'clear-cache':
            self.cmdClearCache()
        # end if
        
        if command == 'load-all':
            self.cmdLoadAll()
        # end if
        
        if command == 'compare-all':
            self.cmdCompareAll()
        # end if
        
        self.dbLocal.close()
    # end function

    # finish and tidy up
    def finish(self):
        self.config = None
        self.dbLocale = None
    # end function

# end class MyApp

# END definitions ==============================================================

def main(args):
    try:
        command = args.command if args.command else ''
        if command != '':
            print('Running ...')
            logging.basicConfig(filename='compare.log', level=logging.DEBUG)
            configFilePath = './config.json'
            logging.debug('App: loading...')
            myApp = MyApp(configFilePath)
            logging.debug('App: loaded.')
            logging.debug('App: starting...')
            myApp.start(command)
            logging.debug('App: started.')
            logging.debug('App: finishing...')
            myApp.finish()
            logging.debug('App: finished.')
        # end if
    except Exception as err:
        print('App error: ', err)
    else: # finally
        print('The End!')
    # end try catch
    sys.exit()
# end function main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare database structures.')
    parser.add_argument('command', help='Command to execute', choices=['clear-cache', 'load-all', 'compare-all'])
    args = parser.parse_args()
    main(args)
# end if
