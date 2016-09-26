# compare database structures
python tool to compare structures of databases in a distributed mysql data warehouse.

config.json file is for configuring the options. please check example.config.json. connection is established via SSH tunnels. SQLAlchemy is used for reading the database structure. SQLite3 database is used to store info locally in order to query it to find differences.

## requirements

###sshtunnel
[https://pypi.python.org/pypi/sshtunnel](https://pypi.python.org/pypi/sshtunnel)

`pip install sshtunnel`

###SQLAlchemy
[http://www.sqlalchemy.org/](http://www.sqlalchemy.org/)

`pip install SQLAlchemy`

