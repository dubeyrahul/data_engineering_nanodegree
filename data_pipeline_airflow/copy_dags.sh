rm -rf  ~airflow/plugins
cp dags/sparkify_etl.py ~/airflow/dags/
cp create_tables.sql ~/airflow/dags/
cp -r plugins/* ~/airflow/plugins
