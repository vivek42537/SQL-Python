import pandas as pd
from pandas import DataFrame
from pandas import ExcelWriter
from openpyxl import load_workbook
import mysql.connector
import csv
import logging
import numpy as np

import yaml

with open('test3.yaml', 'r') as yam:
    doc = yaml.safe_load(yam)

hostInfo = doc["DatabaseInfo"]["host"]
userInfo = doc["DatabaseInfo"]["user"]
passwdInfo = doc["DatabaseInfo"]["passwd"]
databaseInfo = doc["DatabaseInfo"]["database"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s\n:%(message)s')
fileHandler = logging.FileHandler('two.log')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

fileName = input("File name: ")
df = pd.read_excel (fileName)
df = df.where((pd.notnull(df)), None)
df['CREATED'] = df['CREATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['UPDATED'] = df['UPDATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['RESOLVED_AT'] = df['RESOLVED_AT'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['CREATED_DATE'] = df['CREATED_DATE'].dt.strftime('%Y-%m-%d')
df['START_MAINTENANCE'] = df['START_MAINTENANCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['END_MAINTENANCE'] = df['END_MAINTENANCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['REASSIGNMENT_COUNT'] = df['REASSIGNMENT_COUNT'].apply(str)


logger.info(df)

df.columns = df.columns.str.strip()
logger.debug(df.columns)

mydb = mysql.connector.connect(
    host = hostInfo,
    user = userInfo,
    passwd = passwdInfo,
    database = databaseInfo
)
# print (mydb)
mycursor = mydb.cursor()


x = doc
def POD (row):
    for key,value in doc["POD"].items():
        if row['ASSIGNMENT_GROUP'] == key :
            return value

def AlertCat (df):
    defaultValue = None
    mask = np.column_stack([df.NUMBER.str.contains('INC', na=False, case=False) for col in df])
    df.loc[mask.any(axis=1), 'ALERT_CAT'] = defaultValue

    for key,value in doc["ALERTcat"].items():
        print (key)
        if key == 'KEEP ALIVE' or key == 'NEND' or key == '0.00,NA' :
            print ('1')
            print (key)
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=True) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])

        if ((key == 'storag') & (np.column_stack([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df])) & (np.column_stack([df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df]))).any():
            print ('2')
            print (key)
            cond = np.logical_and([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df],[df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Backup Alert')

        if ((key == 'good' or key == 'metric' or key == 'monitor') & (np.column_stack([df.SUMMARY.str.contains('database', na=False, case=False) for col in df]))).any():
            print ('3')
            print (key)
            cond = np.logical_and([df.SUMMARY.str.contains('database', na=False, case=False) for col in df],[df.SUMMARY.str.contains('metric|good|monitor', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Sitescope Database')

        if ((key == 'r2c') & (np.column_stack([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df]))).any():
            print ('4')
            print (key)
            cond = np.logical_and([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df],[df.CONFIGURATION_ITEM.str.contains('r2c', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('R2C SRM')
        
        if (np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df])).any() :
            print ('5')
            print (key)
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df]) 
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])
        # if ((np.column_stack([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df])) & (np.column_stack([df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df]))).any():
        #     print('2')
        #     print(key)
        #     cond = np.logical_and([df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df],[df.SUMMARY.str.contains('backup', na=False, case=False) for col in df])
        #     mask = np.column_stack(cond)
        #     df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Backup Alert')
    
  

operation = ""
while (operation != 'done'):
    operation = input("Press 1, 2, 3, or 4 for to Create table in SQL, Insert data into SQL, ")

    if (operation == '1'): #CREATE TABLE
        mycursor.execute('CREATE TABLE RawPeople (NUMBER nvarchar(50), SUMMARY LONGTEXT, CONFIGURATION_ITEM LONGTEXT, CREATED nvarchar(50), COMPANY LONGTEXT, ASSIGNMENT_GROUP nvarchar(50), REASSIGNMENT_COUNT nvarchar(50), PRIORITY nvarchar(50), status nvarchar(50), STATE nvarchar(50), ITASK nvarchar(50), ACTIVE nvarchar(50), ACTIVITY_DUE nvarchar(50), DUE_DATE nvarchar(50), ADDITIONAL_ASSIGNEE_LIST LONGTEXT, APPROVAL nvarchar(50), TICKET_DURATION nvarchar(50), APPROVAL_HISTORY nvarchar(50), APPROVAL_SET nvarchar(50), ASSIGNED_TO nvarchar(50), BUSINESS_DURATION nvarchar(50), CALLER nvarchar(50), UPDATED nvarchar(50), CATEGORY nvarchar(50), U_CATEGORY nvarchar(50), RESOLVE_TIME nvarchar(50), RESOLVED_BY nvarchar(50), RESOLVED_AT nvarchar(50), RESOLVED_BY2 nvarchar(50), CREATED_BY nvarchar(50), CREATED_DATE nvarchar(50), LOCATION nvarchar(50), HAS_ATTACHMENTS nvarchar(50), HIDE_FROM_LIST nvarchar(50), OPENED_BY nvarchar(50), RECORD_SOURCE nvarchar(50), ROOT_CAUSE_L1 nvarchar(50), ROOT_CAUSE_L2 nvarchar(50), ROOT_CAUSE_L3 nvarchar(50), PARENT nvarchar(50), PARENT_INCIDENT nvarchar(50), PARENT_INCIDENT3 nvarchar(50), CHANGE_REQUEST nvarchar(50), CREATE_MAINTENANCE_WINDOW nvarchar(50), MX_WINDOW_STATUS nvarchar(50), START_MAINTENANCE nvarchar(50), END_MAINTENANCE nvarchar(50))')
        mydb.commit()
    
    elif (operation == '2'): #INSERT DATA INTO SQL
        for row in df.itertuples():
            mycursor.execute("INSERT INTO RawPeople (NUMBER, SUMMARY, CONFIGURATION_ITEM, CREATED, COMPANY, ASSIGNMENT_GROUP, REASSIGNMENT_COUNT, PRIORITY, status, STATE, ITASK, ACTIVE, ACTIVITY_DUE, DUE_DATE, ADDITIONAL_ASSIGNEE_LIST, APPROVAL, TICKET_DURATION, APPROVAL_HISTORY, APPROVAL_SET, ASSIGNED_TO, BUSINESS_DURATION, CALLER, UPDATED, CATEGORY, U_CATEGORY, RESOLVE_TIME, RESOLVED_BY, RESOLVED_AT, RESOLVED_BY2, CREATED_BY, CREATED_DATE, LOCATION, HAS_ATTACHMENTS, HIDE_FROM_LIST, OPENED_BY, RECORD_SOURCE, ROOT_CAUSE_L1, ROOT_CAUSE_L2, ROOT_CAUSE_L3, PARENT, PARENT_INCIDENT, PARENT_INCIDENT3, CHANGE_REQUEST, CREATE_MAINTENANCE_WINDOW, MX_WINDOW_STATUS, START_MAINTENANCE, END_MAINTENANCE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (row.NUMBER, 
            row.SUMMARY, 
            row.CONFIGURATION_ITEM,
            row.CREATED, 
            row.COMPANY, 
            row.ASSIGNMENT_GROUP, 
            row.REASSIGNMENT_COUNT, 
            row.PRIORITY, 
            row.status, 
            row.STATE, 
            row.ITASK, 
            row.ACTIVE, 
            row.ACTIVITY_DUE, 
            row.DUE_DATE, 
            row.ADDITIONAL_ASSIGNEE_LIST,
            row.APPROVAL, 
            row.TICKET_DURATION, 
            row.APPROVAL_HISTORY, 
            row.APPROVAL_SET, 
            row.ASSIGNED_TO, 
            row.BUSINESS_DURATION, 
            row.CALLER,
            row.UPDATED, 
            row.CATEGORY,
            row.U_CATEGORY, 
            row.RESOLVE_TIME, 
            row.RESOLVED_BY, 
            row.RESOLVED_AT, 
            row.RESOLVED_BY2, 
            row.CREATED_BY, 
            row.CREATED_DATE, 
            row.LOCATION, 
            row.HAS_ATTACHMENTS, 
            row.HIDE_FROM_LIST, 
            row.OPENED_BY, 
            row.RECORD_SOURCE,
            row.ROOT_CAUSE_L1, 
            row.ROOT_CAUSE_L2, 
            row.ROOT_CAUSE_L3, 
            row.PARENT, 
            row.PARENT_INCIDENT, 
            row.PARENT_INCIDENT3,
            row.CHANGE_REQUEST, 
            row.CREATE_MAINTENANCE_WINDOW, 
            row.MX_WINDOW_STATUS, 
            row.START_MAINTENANCE,
            row.END_MAINTENANCE))
        mydb.commit()
    
    elif (operation == '3'): #TAKE SQL DATA AND PUT INTO PANDAS
        mycursor.execute("SELECT * FROM RawPeople")
        myresult = mycursor.fetchall()
        df = DataFrame(myresult, columns=['NUMBER', 'SUMMARY', 'CONFIGURATION_ITEM', 'CREATED', 'COMPANY', 'ASSIGNMENT_GROUP', 'REASSIGNMENT_COUNT', 'PRIORITY', 'status', 'STATE', 'ITASK', 'ACTIVE', 'ACTIVITY_DUE', 'DUE_DATE', 'ADDITIONAL_ASSIGNEE_LIST', 'APPROVAL', 'TICKET_DURATION', 'APPROVAL_HISTORY', 'APPROVAL_SET', 'ASSIGNED_TO', 'BUSINESS_DURATION', 'CALLER', 'UPDATED', 'CATEGORY', 'U_CATEGORY', 'RESOLVE_TIME', 'RESOLVED_BY', 'RESOLVED_AT', 'RESOLVED_BY2', 'CREATED_BY', 'CREATED_DATE', 'LOCATION', 'HAS_ATTACHMENTS', 'HIDE_FROM_LIST', 'OPENED_BY', 'RECORD_SOURCE', 'ROOT_CAUSE_L1', 'ROOT_CAUSE_L2', 'ROOT_CAUSE_L3', 'PARENT', 'PARENT_INCIDENT', 'PARENT_INCIDENT3', 'CHANGE_REQUEST', 'CREATE_MAINTENANCE_WINDOW', 'MX_WINDOW_STATUS', 'START_MAINTENANCE', 'END_MAINTENANCE'])
        logger.info('This is the data from SQL')
        logger.info(df)
    
    elif (operation == '4'): #MAP POD
        df['POD'] = df.apply (lambda row: POD(row), axis=1)
        logger.info('Now with POD column')
        logger.info(df)
        #print (df)

    elif (operation == '5'): #MAP ALERT CATEGORY
        # df['ALERT_CATEGORY'] = df.apply (lambda row: AlertCat(row), axis=1)
        AlertCat (df)
        logger.info('Now with ALERT_CATEGORY column')
        logger.info(df)
        print (df)
    
    elif (operation == '6'): #EXPORT TO EXCEL SHEET
        fileName = input("File name: ")
        sheet = input("Sheet name: ")
        book = load_workbook(fileName)
        
        writer = pd.ExcelWriter(fileName, engine='openpyxl')
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        
        df.to_excel(writer, sheet_name= sheet, index=False)
        writer.save()   

    
