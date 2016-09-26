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

'''
Base = declarative_base()
class MyDbLocalTableColumnModel(Base):
    __table__ = 'my_tables'
    
    id              = Column(Integer, primary_key=True)
    db_ref          = Column(String)
    schema_name     = Column(String)
    table_name      = Column(String)
    column_name     = Column(String)
    column_type     = Column(String)
    column_nullable = Column(Integer)
    column_attrs    = Column(String)

    def __repr__(self):
        return "<MyDbLocalTableColumnModel (db_ref='%s', schema_name='%s', table_name='%s', column_name='%s')>" % (
            self.db_ref, self.schema_name, self.table_name, self.column_name
        )
    # end function
    
# end class MyDbLocalTableColumnModel
'''

class MyDbLocal:
    
    def __init__ (self):
        self.dbEngine     = None
        self.dbConnection = None
        self.dbCursor     = None
    # end constructor
    
    def connect(self, dbFile):
        '''
        dbType = 'sqlite+pysqlite'
        #dbFile = './db.sqlite3'
        dsn = '{}:///{}'.format(
            dbType, dbFile
        )

        logging.debug('Local database engine: starting ...')
        self.dbEngine = create_engine(dsn, module=sqlite)
        logging.debug('Local database engine: started.')

        logging.debug('Local database connection: connecting ...')
        self.dbConnection = self.dbEngine.connect()
        logging.info('Local database connection: connected.')
        
        logging.debug('Local database: new session ...')
        self.session = sessionmaker()
        self.session.configure(bind=self.dbEngine)
        logging.debug('Local database: new session ready.')
        '''
        
        self.dbConnection = sqlite.connect(dbFile)
        self.dbCursor     = self.dbConnection.cursor()
    # end function
    
    def resetTablesOfDb(self, dbRef):
        deleteSql = "DELETE FROM my_tables WHERE db_ref = '{}';".format(dbRef)
        result = self.dbConnection.execute(deleteSql)
        self.dbConnection.commit()
    # end function
    
    def saveTableColumns(self, dbRef, schemaName, table):
        '''
        myTableCol = MyDbLocalTableColumnModel(
            db_ref=dbRef, schema_name=schemaName, table_name=table.name, column_name=column.name,
            column_type=str(column.type), column_nullable=1 if column.nullable else 0
        )
        self.session.add(myTableCol)
        self.session.commit()
        '''
        
        insertSql = '''
            INSERT INTO my_tables
            (db_ref, schema_name, table_name, column_name, column_type, column_nullable)
            VALUES (?, ?, ?, ?, ?, ?);
        '''
        params = []
        for column in table.columns:
            logging.debug(
                'DB: ' + dbRef + ' >> Table: ' + table.name + ' Column: ' + column.name +
                ' Type: ' + str(column.type) + ' Nullable: ' + str(column.nullable)
            )
            row = (dbRef, schemaName, table.name, column.name, str(column.type), int(1 if column.nullable else 0))
            params.append(row)
        # end for
        
        result = self.dbCursor.executemany(
            insertSql,
            params
        )
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
        print('DB: ' + self.dbRef + ' > Database connection: connected.')

        logging.debug('DB: ' + self.dbRef + ' > Database version: querying ...')
        result = self.dbConnection.execute('SELECT version() AS version')
        for row in result:
            version = str(row['version']) if 'version' in row else 'N/A'
            logging.info('Database version: {}'.format(version))
        # end for
    # end function

    # read tables
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

    # read table columns
    def table(self, myTable):
        logging.debug('DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' - Columns listing ...')
        #myTable = Table(myTable.name, self.metadata, autoload=True, autoload_with=self.dbEngine)
        self.dbLocal.saveTableColumns(self.dbRef, 'public', myTable) # schema = 'public'
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
            logging.debug('MyApp: reset table info for ' + dbRef)
            self.dbLocal.resetTablesOfDb(dbRef)
            logging.debug('MyApp: reset table info for ' + dbRef + ' done')
            
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
    
    # end function
    
    def compareDbStructure(self, sourceDbRef, targetDbRef):
        self.dbLocal.compare(sourceDbRef, targetDbRef)
    # end function
    
    # start running app
    def start(self):
        logging.debug('MyApp: starting ...')
        
        self.connectToLocalDb()
        
        # TODO: get command from user
        command = 'compare'
        
        if command == 'loadall':
            dbs = self.config['databases']
            for dbRef in dbs:
                self.loadDbStructure(dbRef)
            # end for loop
        # end if
        
        if command == 'compare':
            compareDbs = self.config['compare']
            for key in compareDbs:
                sourceDbRef = key
                targetDbRef = compareDbs[key]
                logging.debug('MyApp: comparing: ' + sourceDbRef + ' with ' + targetDbRef)
                self.compareDbStructure(sourceDbRef, targetDbRef)
            # end for loop
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
