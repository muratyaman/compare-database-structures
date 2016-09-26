# compare database structures
python tool to compare structures of databases in a distributed mysql data warehouse.

config.json file is for configuring the options. please check example.config.json. connection is established via SSH tunnels. SQLAlchemy is used for reading the database structure. SQLite3 database is used to store info locally in order to query it to find differences.

## requirements

### sshtunnel
[https://pypi.python.org/pypi/sshtunnel](https://pypi.python.org/pypi/sshtunnel)

`pip install sshtunnel`

### SQLAlchemy
[http://www.sqlalchemy.org/](http://www.sqlalchemy.org/)

`pip install SQLAlchemy`

### MySQL connector
[https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html](https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html)

`apt-get install python3-mysql.connector`

## installation

### create local database to store collected info

`sqlite3 db.sqlite3 < db.sql`

### edit configuration file

`cp example.config.json config.json; nano config.json`

## usage

`python3 compare.py`


