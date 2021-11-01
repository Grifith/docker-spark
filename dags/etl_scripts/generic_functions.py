"""
This file contains generic function which can be used across all ETL modules . As of now for purpose of demo I have kept this as a
file . In ideal scenario it should be hosted and  imported from the python core utilites
"""
import os
import psycopg2

"""
Removing file from local filesystem
"""

def remove_file(file_name):
    try:
        if os.path.exists(file_name):
            print('Deleting the {} from the local node.........'.format(file_name))
            os.remove(file_name)
        else:
            print("Can not delete the file as it doesn't exists")
    except:
        print("Not a valid file")
"""
This method will create and return a connection to specific s3 bucket for all s3 operations to perform
"""

def get_postgres_con(v_con_name='ODS'):

    conn = psycopg2.connect(host=os.environ['POSTGRES_ETL_HOST'],
                            port=os.environ['POSTGRES_ETL_PORT'],
                            user=os.environ['POSTGRES_ETL_USER'],
                            password=os.environ['POSTGRES_ETL_PASSWORD'],
                            dbname=os.environ['POSTGRES_ETL_DB'])
    print (" Connected to  " + str(os.environ['POSTGRES_ETL_HOST']))
    return conn


def execute_on_ods(sql,table_name=None):

    con=get_postgres_con()
    cur=con.cursor()
    if table_name:
        cur.execute("drop table if exists {}".format(table_name))
    print("Executing.....{}".format(sql))
    cur.execute(sql)
    con.commit()
    con.close()
