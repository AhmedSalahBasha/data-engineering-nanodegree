import configparser
import psycopg2
import time
from sql_queries import create_table_queries, drop_table_queries
import aws
import aws_helper as helper


def drop_tables(cur, conn):
    """
    Function to drop all tables from the redshift cluster
    @param cur: database cursor object
    @param conn: database connection object
    return: void
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables have been deleted successfully!")


def create_tables(cur, conn):
    """
    Function to create all tables on the redshift cluster
    @param cur: database cursor object
    @param conn: database connection object
    return: void
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables have been created successfully!")



def main():
    """
    This main function is creating the whole infrastructure on AWS cloud.
    It also creates all tables (staging tables + star-schema tables) on the Redshift cluster
    """
    config = helper.read_config_file('dwh.cfg')
    
    # create aws inftrastructure
    aws.create_aws_infrastructure()
    
    # create redshift connection
    conn = helper.create_redshift_conn(config)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("#### Tables creations is on the Redshift cluster is DONE ####")

if __name__ == "__main__":
    main()