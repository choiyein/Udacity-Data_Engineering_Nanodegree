import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description: This function is responsible for loading values into staging tables in the Redshift cluster.

        Arguments:
            cur: the cursor object
            conn: the connection object
        
        Returns:
            None
    """
    for query in copy_table_queries:
        print("Load staging tables: {}".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Description: This function is responsible for inserting values into 
        songplays, songs, users, artists, and time tables in the Redshift cluster.

        Arguments:
            cur: the cursor object
            conn: the connection object
        
        Returns:
            None
    """    
    for query in insert_table_queries:
        print("Insert tables: {}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connect Redshift")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Load staging tables")
    load_staging_tables(cur, conn)
    print("Insert tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()