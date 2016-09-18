import sys
import json
import pprint
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import *
from sqlalchemy.engine import reflection

config = None
configFilePath = './config.json'
print('Config file: opening ...')
with open(configFilePath) as configFile:
    print('Config file: opened.')
    print('Config json: loading ...')
    config = json.load(configFile)
    print('Config json: loaded.')

# pprint.pprint(config)
hosts     = config['hosts']
databases = config['databases']
compare   = config['compare']

# todo: get connection details from config
myHost = ''
myUser = ''
myPass = ''
myPort = 3306

dbType = 'mysql+mysqlconnector'
dbPort = server.local_bind_port
dsn = dbType + '://user:pass@127.0.0.1:port/dbname'

server = SSHTunnelForwarder(
    myHost,
    ssh_username = myUser,
    ssh_password = myPass,
    remote_bind_address=('127.0.0.1', myPort)
)

print('SSH tunnel: starting ...')
server.start()
print('SSH tunnel: started.')

print('SSH tunnel: ready ')
print(server.local_bind_port)

try:
  print('Database engine: creating ...')
  dbEngine = create_engine(dsn)
  print('Database engine: created.')

  print('Database connection: connecting ...')
  connection = dbEngine.connect()
  print('Database connection: connected.')

  result = connection.execute("SELECT version() AS version")
  for row in result:
    print("Database version :", row['version'])


  print('Database metadata: loading ...')
  metadata = MetaData()
  metadata.reflect(bind=dbEngine)
  print('Database metadata: loaded.')

  inspector = inspect(dbEngine)

  for tbl in metadata.tables:
    print('Table ', tbl)
    columns = inspector.get_columns(tbl)
    pprint.pprint(columns)


except Exception as err:
  print('Database: error: ', err)

else:
  connection.close()


print('SSH tunnel: stopping ...')
server.stop()
print('SSH tunnel: stopped.')

print('The End!')
sys.exit(0)
