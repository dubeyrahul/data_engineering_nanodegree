Installation instructions:
Step 1:
Install Cassandra as per: http://cassandra.apache.org/doc/latest/getting_started/installing.html

Step 2:
Start cassandra:
Use `sudo service cassandra start` to start and `sudo service cassandra stop` to stop it. However, normally the service will start automatically. For this reason be sure to stop it if you need to make any configuration changes.

Step 3:
Install the Python driver with:
$ pip3 install cassandra-driver
