## Installation instructions:
### Step 1: Install Cassandra
Follow instructions at: http://cassandra.apache.org/doc/latest/getting_started/installing.html

### Step 2: Start Cassandra
Use `sudo service cassandra start` to start and `sudo service cassandra stop` to stop it. However, normally the service will start automatically. For this reason be sure to stop it if you need to make any configuration changes.
Check the port number on which Cassandra is running (verify that it is 9042) (hint: check this file: `/etc/cassandra/cassandra.yaml`

### Step 3: Install Python driver
```sh
$ pip3 install cassandra-driver
```
Or refer instructions at: https://docs.datastax.com/en/developer/python-driver/3.22/installation/
