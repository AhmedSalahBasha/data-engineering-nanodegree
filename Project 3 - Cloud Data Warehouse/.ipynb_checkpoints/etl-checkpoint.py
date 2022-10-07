import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries
from aws import AWS_CREDENTIALS
import aws_helper as helper


def load_staging_tables(cur, conn):
    """
    Function to load staging tables on the redshift cluster from the S3 bucket
    @param cur: database cursor object
    @param conn: database connection object
    return: void
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Done Query: \n {0}".format(query))
    print("Both staging tables have been loaded with the raw data from S3 Bucket successfully!")


def insert_tables(cur, conn):
    """
    Function to insert data from staging tables to the star-schema tables on the cluster
    @param cur: database cursor object
    @param conn: database connection object
    return: void
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Done Query: \n {0}".format(query))
    print("All fact and dimension tables have been loaded with the data from the staging tables successfully!")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = helper.create_redshift_conn(config)
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("sleeping 5 seconds before deleting the cluster...")
    time.sleep(5)

    conn.close()
    
    AWS_KEY = helper.decode_credential(AWS_CREDENTIALS['AWS_KEY'], AWS_CREDENTIALS['ENC_KEY'])
    AWS_SECRET = helper.decode_credential(AWS_CREDENTIALS['AWS_SECRET'], AWS_CREDENTIALS['ENC_KEY'])
    redshift = helper.create_aws_client('redshift', AWS_KEY, AWS_SECRET, config.get('S3', 'REGION'))
    helper.delete_redshift_cluster(redshift, config)
    print("#### ETL Pipeline is finished successfully! ####")


if __name__ == "__main__":
    main()