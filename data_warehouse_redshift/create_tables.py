import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops the tables as per queries in drop_table_queries
    :param postgres-cursor cur: Postgres DB cursor
    :param postgres-connection conn: Postgres DB connection
    :return: None
    :rtype: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables as per queries in create_table_queries
    :param postgres-cursor cur: Postgres DB cursor
    :param postgres-connection conn: Postgres DB connection
    :return: None
    :rtype: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Loading the dwh.cfg file to load Redshift cluster config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Setup connection to the redshift db
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # Drop existing tables
    drop_tables(cur, conn)
    # Create tables
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
