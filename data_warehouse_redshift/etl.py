import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

separator = "-"*30
def load_staging_tables(cur, conn):
    """
    Loads data from S3 to the staging tables
    :param postgres-cursor cur: Postgres DB cursor
    :param postgres-connection conn: Postgres DB connection
    :return: None
    :rtype: None
    """
    print("Load staging tables from S3")
    for query in copy_table_queries:
        print("Running:\n"+query)
        cur.execute(query)
        conn.commit()
        print("done")
        print(separator)


def insert_tables(cur, conn):
    """
    Loads and transforms data from staging tables to dimensional model
    :param postgres-cursor cur: Postgres DB cursor
    :param postgres-connection conn: Postgres DB connection
    :return: None
    :rtype: None
    """
    print("Inserting into tables from staging tables")
    for query in insert_table_queries:
        print("Running:\n"+query)
        cur.execute(query)
        conn.commit()
        print("done")
        print(separator)


def main():
    # Loading the dwh.cfg file to load Redshift cluster config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Setup connection to the redshift db
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # Load data from S3 to staging_tables
    load_staging_tables(cur, conn)
    # Load and transform data from staging_tables to dimesional model
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
