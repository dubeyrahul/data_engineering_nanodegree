To set up psql locally on Ubuntu:
1. Install postgres and psycopg2-binary
2. make sure that postgresql.conf points to the port number and hostname present in create_table.py script
3. you can verify 2 by doing a netstat check (netstat -na | grep postgres)
4. once you change port/host in the conf file stop and restart the postgres service using: `service postgres stop` and `service postgres start`
