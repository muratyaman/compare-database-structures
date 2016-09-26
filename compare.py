import logging
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlite3 import dbapi2 as sqlite
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# START definitions ============================================================

'''
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
    
    def saveTableColumn(self, dbRef, schemaName, table, column):
        '''
        myTableCol = MyDbLocalTableColumnModel(
            db_ref=dbRef, schema_name=schemaName, table_name=table.name, column_name=column.name,
            column_type=str(column.type), column_nullable=1 if column.nullable else 0
        )
        self.session.add(myTableCol)
        self.session.commit()
        '''
        
        '''
        insertSql = ('INSERT INTO my_tables ' +
            '(db_ref, schema_name, table_name, column_name, column_type, column_nullable) ' +
            'VALUES (:db_ref, :schema_name, :table_name, :column_name, :column_type, :column_nullable)'
        )
        params = {
            "db_ref"          : dbRef,
            "schema_name"     : schemaName,
            "table_name"      : table.name,
            "column_name"     : column.name,
            "column_type"     : str(column.type),
            "column_nullable" : 1 if column.nullable else 0
        }
        result = self.dbCursor.execute(
            insertSql,
            params
        )
        '''
        
        insertSql = ('INSERT INTO my_tables ' +
            '(db_ref, schema_name, table_name, column_name, column_type, column_nullable) ' +
            'VALUES (?, ?, ?, ?, ?, ?)'
        )
        params = [
            (dbRef, schemaName, table.name, column.name, str(column.type), int(1 if column.nullable else 0)),
        ]
        result = self.dbCursor.executemany(
            insertSql,
            params
        )
        self.dbConnection.commit()
        
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
        for myColumn in myTable.columns:
            logging.debug(
                'DB: ' + self.dbRef + ' >> Table: ' + myTable.name + ' Column: ' + myColumn.name +
                ' Type: ' + str(myColumn.type) + ' Nullable: ' + str(myColumn.nullable)
            )
            self.dbLocal.saveTableColumn(self.dbRef, 'public', myTable, myColumn) # schema = 'public'
        # end for
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

    def start(self):
        logging.debug('MyApp: starting ...')
        
        self.dbLocal = MyDbLocal()
        self.dbLocal.connect('./db.sqlite3');
        
        compareDbs = self.config['compare']
        for key in compareDbs:
            sourceDbRef = key
            targetDbRef = compareDbs[key]
            logging.debug('MyApp: comparing: ' + sourceDbRef + ' with ' + targetDbRef)
            try:
                logging.debug('MyApp: loading: ' + sourceDbRef)
                sourceDb = MyDb(sourceDbRef, self.dbLocal)
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
                targetDb = MyDb(targetDbRef, self.dbLocal)
                targetDb.connect(self.config)
                targetTables = targetDb.tables()
            except Exception as err:
                print('Target database: error: ', err)
            else: # finally
                logging.debug('MyApp: closing db: ' + targetDbRef)
                targetDb.close()
            # end try catch
        # end for loop
        
        self.dbLocal.close()
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
