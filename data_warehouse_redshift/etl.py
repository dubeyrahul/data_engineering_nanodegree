import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

separator = "-"*30
def load_staging_tables(cur, conn):
    print("Load staging tables from S3")
    for query in copy_table_queries:
        print("Running:\n"+query)
        cur.execute(query)
        conn.commit()
        print("done")
        print(separator)


def insert_tables(cur, conn):
    print("Inserting into tables from staging tables")
    for query in insert_table_queries:
        print("Running:\n"+query)
        cur.execute(query)
        conn.commit()
        print("done")
        print(separator)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
