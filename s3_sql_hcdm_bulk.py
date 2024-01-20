from concurrent import futures
import csv
import queue
import pandas as pd
import numpy as np
import datacompy
#import pyodbc
#from pymongo import MongoClient
#import dateutil
#from dateutil import parser
from pandas import DataFrame
import pandas as pd
import sys
import snowflake.connector as sf_c
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
#import cx_Oracle
import os
#import sweetviz as sv
import configparser
import pyodbc
import re
import datetime as dt
import logging
import re
import sqlalchemy
import pyodbc
from pypika import Column, Query, Table
import time
import boto3
import fnmatch
start_time = time.time()

print("--- %s seconds ---" % (time.time() - start_time))

#import sqlactions


MULTI_ROW_INSERT_LIMIT = 1000
WORKERS = 6

TABLE_REFRESH_WHITE_LIST_ID = sys.argv[1]



config = configparser.ConfigParser()

config.read_file(open(r'setup_prd_migration.ini'))
oracleHostName = config.get('oracle-configuration', 'oracleHostName')
sqlserverHostName_HCHB = config.get('sqlserver-configuration', 'sqlServerHostName_HCHB')
snowflakeAccount = config.get('snowflake-configuration', 'snowflakeAccount')

credentialsconfig = configparser.ConfigParser()
credentialsconfig.read_file(open(r'configurations_setup_prd_migration.ini'))
oracleUserName = credentialsconfig.get('oracle-credentials', 'oracleUserName')
oraclePassword = credentialsconfig.get('oracle-credentials', 'oraclePassword')
dbName = credentialsconfig.get('oracle-credentials', 'dbName')

sqlserverUserName_HCHB = 'hchbetl'
sqlserverPassword_HCHB = credentialsconfig.get('sqlserver-credentials', 'sqlserverPassword_HCHB')
sqlserverDbName_HCHB = credentialsconfig.get('sqlserver-credentials', 'sqlserverDbName_HCHB')
sqlserverSchemaName_HCHB = credentialsconfig.get('sqlserver-credentials', 'sqlserverSchemaName_HCHB')


snowflakeUserName_sit = credentialsconfig.get('snowflake-credentials', 'snowflakeUserName_sit')
snowflakePassword_sit = credentialsconfig.get('snowflake-credentials', 'snowflakePassword_sit')
snowflakeWarehouse_sit = credentialsconfig.get('snowflake-credentials', 'snowflakeWarehouse_sit')
snowflakeRole_sit_sit = credentialsconfig.get('snowflake-credentials', 'snowflakeRole_sit')
snowflakeDBName_sit = credentialsconfig.get('snowflake-credentials', 'snowflakeDatabase_sit')
snowflakeSchema_sit='DLAKESIT'

snowflakeUserName_prd = credentialsconfig.get('snowflake-credentials', 'snowflakeUserName_prd')
snowflakePassword_prd = credentialsconfig.get('snowflake-credentials', 'snowflakePassword_prd')
snowflakeWarehouse_prd = credentialsconfig.get('snowflake-credentials', 'snowflakeWarehouse_prd')
snowflakeRole_prd_prd = credentialsconfig.get('snowflake-credentials', 'snowflakeRole_prd')
snowflakeDBName_prd = credentialsconfig.get('snowflake-credentials', 'snowflakeDatabase_prd')
snowflakeSchema_prd='HCHB'



    
def snowFlakeConnection():
    try:
        conn = sf_c.connect(
            user=snowflakeUserName_prd,
            password=snowflakePassword_prd,
            account=snowflakeAccount,
            warehouse=snowflakeWarehouse_prd,
            database=snowflakeDBName_prd,
            schema=snowflakeSchema_prd,
 #           client_session_keep_alive=True
        )
        print("connected to SNOWFLAKE Database.")
        return conn
    except Exception as e:
        print("Error connecting to SNOWFLAKE Database. " + str(e))
        

def creds(DB_NAME):
    if (DB_NAME == 'DLAKESIT' or DB_NAME =='NEXUSSIT'):
        a= [snowflakeUserName_sit,snowflakePassword_sit,snowflakeAccount,snowflakeWarehouse_sit,snowflakeDBName_sit,snowflakeSchema_sit]
    if (DB_NAME == 'DLAKE' or DB_NAME =='NEXUS'):
        a =  [snowflakeUserName_prd,snowflakePassword_prd,snowflakeAccount,snowflakeWarehouse_prd,snowflakeDBName_prd,snowflakeSchema_prd]
    if(DB_NAME == 'HCHB' or DB_NAME =='HCHB_DAS' or DB_NAME == 'VNSNY_BI'):
        a =  [sqlserverHostName_HCHB,sqlserverDbName_HCHB,sqlserverUserName_HCHB,sqlserverPassword_HCHB]
        
    return a

def db_name(DB_NAME):
    sf_db_list = ['DLAKE','NEXUS','DLAKESIT','NEXUSSIT']
    sql_db_list = ['HCHB','D365','HCHB_DAS', 'VNSNY_BI']
    if DB_NAME in sf_db_list:
        return  'SNOWFLAKE'
    elif DB_NAME in sql_db_list:
        return 'SQL SERVER'
    
def load_to_ec2(TABLE_NAME,FULL_TABLE_NAME):
    sf_conn = snowFlakeConnection()
    sf_cur = sf_conn.cursor()
    sf_cur.execute("copy into 's3://vnsny-das-staging-prod/SRCFILES/HCDM_SF_COPY/"+TABLE_NAME+"/"+TABLE_NAME+"' from (select * from "+FULL_TABLE_NAME+") file_format = ( field_delimiter = ',' DATE_FORMAT = 'MM/DD/YYYY' TIMESTAMP_FORMAT = 'MM/DD/YYYY' COMPRESSION = NONE empty_field_as_null = false  NULL_IF = ('') type ='CSV') max_file_size=1000000000 HEADER = TRUE storage_integration = S3_INT_VNSNYPRD")
    sf_conn.commit()

    
    print(FULL_TABLE_NAME+ " loaded to s3 STAGING PROD")

    #aws s3 cp s3://vnsny-das-staging-prod/SRCFILES/HCDM_SF_COPY/SF_TO_CSV_TEST/ s3://vnsny-das-hchb-prd/test1/ --recursive
    
    os.system("aws s3 cp s3://vnsny-das-staging-prod/SRCFILES/HCDM_SF_COPY/"+TABLE_NAME+"/" +" s3://vnsny-das-hchb-prd/test1/"+TABLE_NAME+"/" +" --recursive")
    print(FULL_TABLE_NAME+ " loaded to vnsny-das-hchb-prd")
    
def read_csv(csv_file):
    with open(csv_file ,encoding="utf-8", newline="") as in_file:
        reader = csv.reader(in_file, delimiter=",")
        print(reader)
        
        next(reader)  # Header row

        for row in reader:
            yield row


def execute_query(q, conn_params):
    try:
        connection = pyodbc.connect(**conn_params)
        cursor = connection.cursor()
        cursor.execute(q)
        connection.commit()
        connection.close()
    
    except pyodbc.Error as err:
        logging.warn(err)    
            

    
def multi_row_insert(batch, target_table_name, conn_params):
    row_expressions = []

    for _ in range(batch.qsize()):
        row_data = tuple(batch.get())
        row_expressions.append(row_data)
        
    

    table = Table(target_table_name)
    insert_into = Query.into(table).insert(*row_expressions)
    
    final_query = query_generated.replace('"', '')
    
    #query_generated = str(insert_into)

    execute_query(final_query, conn_params)
    

def process_row(row, batch, target_table_name, conn_params):
    batch.put(row)

    if batch.full():
        multi_row_insert(batch, target_table_name, conn_params)

    return batch 



    
    
    
def load_csv(csv_file, target_table_name, conn_params):

    batch = queue.Queue(MULTI_ROW_INSERT_LIMIT)

    with futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        todo = []

        for row in read_csv(csv_file):
            future = executor.submit(
                process_row, row, batch, target_table_name, conn_params
            )
            todo.append(future)

        for future in futures.as_completed(todo):
            result = future.result()

    # Handle left overs
    if not result.empty():
        multi_row_insert(result,target_table_name , conn_params)
        
    


if __name__ == "__main__":
    
    sf_conn = snowFlakeConnection()
    df_QE = pd.read_sql_query("select * from NEXUS.JMAN.TABLE_REFRESH_WHITE_LIST", sf_conn)
    for i, row in enumerate(df_QE.itertuples()):
        if(str(row.TABLE_REFRESH_WHITE_LIST_ID)== TABLE_REFRESH_WHITE_LIST_ID):
            SOURCE_DB_NAME = db_name(row.SOURCE_DB)
            TARGET_DB_NAME = db_name(row.TARGET_DB)
            FULL_NAME = row.SOURCE_DB +'.'+row.SOURCE_SCHEMA+'.'+row.SOURCE_TABLE_NAME
            
            
            s3_resource = boto3.resource('s3')

            bucketname_source = 'vnsny-das-hchb-prd'
            file_to_search = row.SOURCE_TABLE_NAME+'*'
            s3_path_source = "s3://vnsny-das-hchb-prd/HCDM_FILE_COPY/"+row.SOURCE_TABLE_NAME+"/"
            
            print("THIS is the destination path: "+ s3_path_source)

            #s3_path_destination = 'OUTGOING/EXTRACTS/CAREPORT/'

            list_files = []
            
            for object in s3_resource.Bucket(bucketname_source).objects.filter(Prefix="HCDM_FILE_COPY/"+row.SOURCE_TABLE_NAME+"/"):
                object_full_path = object.key
                object_split = object_full_path.split('/')
                list_files.append(object_split[-1])
            
            print(list_files)   
            for list2 in list_files:
                    print(list2)
                
                

            filtered = fnmatch.filter(list_files, file_to_search)

            
            #print('Files LOADED')
            #path = "/AppPrd/CEDL/etl/SrcFiles/hcdm_to_csv/"+row.SOURCE_TABLE_NAME+"/"
            #dir_list = os.listdir(path)
            
            conn_params = {
                "server": sqlserverHostName_HCHB,
                "database": row.TARGET_DB,
                "user": sqlserverUserName_HCHB,
                #"tds_version": "7.4",
                "password": sqlserverPassword_HCHB,
                "port": 1433,
                "driver": "ODBC Driver 17 for SQL Server",
                }
            
            #truncating table
            
            if(row.LOAD_TYPE =='FULL_REFRESH'):
                truncate = 'TRUNCATE TABLE '+row.TARGET_DB +'.'+row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME
                execute_query(truncate,conn_params)
            
                print('Target table truncated')
                
            num = 0
            for file in filtered:
                final_query = "BULK INSERT "+row.TARGET_DB +'.'+row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME+" FROM 'R:/DAS_SQL_SERVER_MIGRATION/HCHB_S3_FILES/"+row.SOURCE_TABLE_NAME+"/"+file+"' WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '0x0a', FIRSTROW = 2)"
                
                    
                #final_query = "BULK INSERT "+row.TARGET_DB +'.'+row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME+" FROM 'F:\DAS_SQL_SERVER_MIGRATION\HCHB_S3_FILES\" ++file+"' WITH (FORMAT = 'csv', ROWTERMINATOR = '0x0a')"
                

                
                print(final_query)
            
                execute_query(final_query,conn_params)
                num = num+1

                print("FILE "+str(num)+" has been loaded" )
            
            print("DATA HAS BEEN LOADED")
    
            os.system("aws s3 rm s3://vnsny-das-staging-prod/SRCFILES/HCDM_SF_COPY/"+row.SOURCE_TABLE_NAME+ " --recursive")
            print("Files have been deleted from vnsny-das-staging-prod")


            os.system('aws s3 rm s3://vnsny-das-hchb-prd/HCDM_FILE_COPY/'+row.SOURCE_TABLE_NAME+ '/ --recursive --exclude ""')
            print("Files have been deleted from vnsny-das-hchb-prd")

            os.system("aws s3 rm s3://vnsny-das-hchb-prd/FILE_SENSOR/completed_"+row.SOURCE_TABLE_NAME+ ".txt")

            print("Files have been deleted from File sensor")




                
            
            
            
            
                
                
                
                
                
#                 cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+creds(row.TARGET_DB)[0]+';DATABASE='+creds(row.TARGET_DB)[1]+';UID='+creds(row.TARGET_DB)[2]+';PWD='+ creds(row.TARGET_DB)[3]) 
#                 print('connection established')
#                 cursor = cnxn.cursor()
#                 cursor.execute('TRUNCATE TABLE '+row.TARGET_DB +'.'+row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME)
#                 cnxn.commit()
            
#             print(row.TARGET_DB +'.'+row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME + ' has been truncated')


                
            
#             csv_path = "/AppPrd/CEDL/etl/SrcFiles/hcdm_to_csv/"+row.SOURCE_TABLE_NAME+"/"+ dir_list[0]
            
#             table_w_schema = row.TARGET_DB +'.'+ row.TARGET_SCHEMA+'.'+row.TARGET_TABLE_NAME
#             load_csv(csv_path, table_w_schema, conn_params)
            
#             print("data load complete")
            
# print("--- %s seconds ---" % (time.time() - start_time))


            
            
