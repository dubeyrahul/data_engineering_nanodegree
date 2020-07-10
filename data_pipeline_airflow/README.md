Following installation instructions from:
https://airflow.readthedocs.io/en/latest/installation.html
Prerequisites:
On Debian based Linux OS:

sudo apt-get update
sudo apt-get install build-essential

Installing just airflow

pip install \
 apache-airflow==1.10.10 \
 --constraint \
        https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.6.txt

Verified with: `airflow version` that 1.10.10 is installed

Initialize meta-db to store airflow dag runs info:
Ran `airflow initdb` (note official doc says: `airflow db init` which is not a valid command)

Now `airflow webserver` to start the airflow UI
