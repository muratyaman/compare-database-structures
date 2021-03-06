# compare database structures
python tool to compare structures of databases in a distributed mysql data warehouse.

config.json file is for configuring the options. please check example.config.json. connection is established via SSH tunnels. SQLAlchemy is used for reading the database structure. SQLite3 database is used to store info locally in order to query it to find differences.

## requirements

### SQLite3 database

`sudo apt-get install sqlite3`

### SSL library

`sudo apt-get install libssl-dev`

### FFI library

`sudo apt-get install libffi-dev`

### Python 2.x * * *

`sudo apt-get install python-dev`

`sudo apt-get install python-pip`

#### sshtunnel
[https://pypi.python.org/pypi/sshtunnel](https://pypi.python.org/pypi/sshtunnel)

`sudo pip install sshtunnel`

#### SQLAlchemy
[http://www.sqlalchemy.org/](http://www.sqlalchemy.org/)

`sudo pip install SQLAlchemy`

#### MySQL connector
[https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html](https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html)

`sudo apt-get install python-mysql.connector`

### Python 3.x * * *

`sudo apt-get install python3-dev`

`sudo apt-get install python3-pip`

#### sshtunnel
[https://pypi.python.org/pypi/sshtunnel](https://pypi.python.org/pypi/sshtunnel)

`sudo pip3 install sshtunnel`

#### SQLAlchemy
[http://www.sqlalchemy.org/](http://www.sqlalchemy.org/)

`sudo pip3 install SQLAlchemy`

#### MySQL connector
[https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html](https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html)

`sudo apt-get install python3-mysql.connector`

## installation

### create local database to store collected info

`sqlite3 db.sqlite3`

On SQLite console:

`.read db.sql`

`.exit`

### edit configuration file

`cp example.config.json config.json`

`nano config.json`

## usage

`python3 compare.py -h`

`python3 compare.py clear-cache`

`python3 compare.py load-all`

`python3 compare.py compare-all`
