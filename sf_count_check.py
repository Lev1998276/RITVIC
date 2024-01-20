import csv
import pandas as pd
import numpy as np
from pandas import DataFrame
import sys
import snowflake.connector as sf_c
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
import os
import configparser
import pyodbc
import re
import datetime as dt
import logging
import sqlalchemy
import pyodbc
from pypika import Column, Query, Table
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


TABLE_NAME = sys.argv[1]
TABLE_NAME2 = sys.argv[2]

#TABLE_NAME = 'NEXUS.DW_OWNER.DOH_RX_DTL' #CHANGE TABLE NAME 1 HERE 
#TABLE_NAME2 = 'DLAKE.CHOICE.FCT_CLAIM_MI'  #CHANGE TABLE NAME 2 HERE 



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

#MULTI DATABASE CONNECTION
def creds(DB_NAME):
    if (DB_NAME == 'DLAKESIT' or DB_NAME =='NEXUSSIT'):
        a= [snowflakeUserName_sit,snowflakePassword_sit,snowflakeAccount,snowflakeWarehouse_sit,snowflakeDBName_sit,snowflakeSchema_sit]
    if (DB_NAME == 'DLAKE' or DB_NAME =='NEXUS'):
        a =  [snowflakeUserName_prd,snowflakePassword_prd,snowflakeAccount,snowflakeWarehouse_prd,snowflakeDBName_prd,snowflakeSchema_prd]
    if(DB_NAME == 'HCHB' or DB_NAME =='HCHB_DAS' or DB_NAME == 'VNSNY_BI'):
        a =  [sqlserverHostName_HCHB,sqlserverDbName_HCHB,sqlserverUserName_HCHB,sqlserverPassword_HCHB]
        

    
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

sf_conn = snowFlakeConnection()
       
if __name__ == "__main__":

    query = "select count(*) from " + TABLE_NAME
    sf_cur = sf_conn.cursor()
    sf_cur.execute(query)
    
    # Fetch the first row (if any)
    sf_count = sf_cur.fetchone()[0]
    print("SNOWFLAKE 1 COUNT IS : "+ str(sf_count))
    
    query = "select count(*) from " + TABLE_NAME2
    sf_cur2 = sf_conn.cursor()
    sf_cur2.execute(query)
    
    # Fetch the first row (if any)
    sql_count= sf_cur2.fetchone()[0]
    print("SNOWFLAKE 2 COUNT IS : "+ str(sql_count))
    
    
    if(sql_count != sf_count):
        
        
    
        now = dt.datetime.now()
        ts_body =  now.strftime("%Y-%b-%d %H:%M:%S")
        ts_sub =  now.strftime('%a %b %d %H:%M:%S EST %Y')
        
        email_body = "DATA ON " +str(ts_body)+ " DOES NOT MATCH BETWEEN TABLE 1 AND TABLE 2"
        sql_text = "TABLE 2 COUNT IS : "+ str(sql_count) 
        sf_text = "TABLE 1 COUNT IS: "+ str(sf_count)
        
    
        HTML_TEXT =  '<html><body><p><strong><span style="font-size: 14px;">' + email_body + '</span></strong></p><p><strong><span style="font-size: 12px;">'+sf_text+'</span></strong></p><p><strong><span style="font-size: 12px;">'+sql_text+'</span></strong></p></body></html>'
        s = smtplib.SMTP('smtp.vnsny.org',587)
        #s.starttls()
        #s.login("AKIAUZ3DBCKETJAVNMVW", "BPHzO08cLGabNNUZSxe2zAelh5ADXGp6JIxDrlxDzsgI")
        #html = html.format(table=tabulate(data, headers=col_list, tablefmt="html"))
    
        fromEmail = 'IT_DAS_CORE@vnsny.org'
        #to_email= ['RitvicKashyap.Sanagavarapu@vnsny.org','Ajinkya.Mohapatra@vnsny.org','Harihara.Ganeshan@vnsny.org','Karthikeyan.Rajendiran@vnsny.org']
        to_email= ['ritvickashyap.sanagavarapu@vnshealth.org','fnu.sanya@vnshealth.org']
        #to_email= ['RitvicKashyap.Sanagavarapu@vnsny.org']
    
        msg = MIMEMultipart('alternative')
    
        msg['From'] = 'it-das-core@vnsny.org'
        msg['Subject'] = "HCDM SF AND SQL COUNT MISMATCH TEST EMAIL"+ ts_sub
        msg['To'] =','.join(to_email)
    
    
        part1 = MIMEText(HTML_TEXT,'html')
        msg.attach(part1)
        print('sending email')
        #print(mail_list[key])
        s.sendmail(fromEmail,to_email,msg.as_string())
        print('email sent')
        s.close()    
        sf_cur.close()
        sf_cur2.close()
        #cursor.close()
        sf_conn.close()
        #cnxn.close()
        
        exit(1)
    else:
        print('PROCESS COMPLETED SUCCESFULLY')
    



            
                
            

