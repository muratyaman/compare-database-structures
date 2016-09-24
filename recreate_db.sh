rm -f db.sqlite3
cat db.sql | sqlite3 db.sqlite3
