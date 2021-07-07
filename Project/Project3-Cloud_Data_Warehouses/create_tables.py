import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Description: This function is responsible for droping tables.

        Arguments:
            cur: the cursor object
            conn: the connection object
        
        Returns:
            None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: This function is responsible for creating tables.

        Arguments:
            cur: the cursor object
            conn: the connection object
        
        Returns:
            None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connect to the redshift")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Drop tables")
    drop_tables(cur, conn)

    print("Create tables")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()