import pandas as pd
from pandas import DataFrame
from pandas import ExcelWriter
from openpyxl import load_workbook
import mysql.connector
import csv
import logging
import numpy as np

import yaml
import os

path = os.getcwd()

print(path)

with open('trigger.yaml', 'r') as yam:
    doc = yaml.safe_load(yam)

hostInfo = doc["DatabaseInfo"]["host"]
userInfo = doc["DatabaseInfo"]["user"]
passwdInfo = doc["DatabaseInfo"]["passwd"]
databaseInfo = doc["DatabaseInfo"]["database"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s\n:%(message)s')
fileHandler = logging.FileHandler('dataframe.log')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# fileName = input("File name: ")
# df = pd.read_excel (fileName)
# df = df.where((pd.notnull(df)), None)
# df['CREATED'] = df['CREATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
# df['UPDATED'] = df['UPDATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
# df['RESOLVED_AT'] = df['RESOLVED_AT'].dt.strftime('%Y-%m-%d %H:%M:%S')
# df['CREATED_DATE'] = df['CREATED_DATE'].dt.strftime('%Y-%m-%d')
# df['START_MAINTENANCE'] = df['START_MAINTENANCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
# df['END_MAINTENANCE'] = df['END_MAINTENANCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
# df['REASSIGNMENT_COUNT'] = df['REASSIGNMENT_COUNT'].apply(str)


# logger.info(df)

# df.columns = df.columns.str.strip()
# logger.debug(df.columns)

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

        if key == 'KEEP ALIVE' or key == 'NEND' or key == '0.00,NA' :
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=True) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])

        if ((key == 'storag') & (np.column_stack([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df])) & (np.column_stack([df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df]))).any():
            cond = np.logical_and([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df],[df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Backup Alert')

        if ((key == 'good' or key == 'metric' or key == 'monitor') & (np.column_stack([df.SUMMARY.str.contains('database', na=False, case=False) for col in df]))).any():
            cond = np.logical_and([df.SUMMARY.str.contains('database', na=False, case=False) for col in df],[df.SUMMARY.str.contains('metric|good|monitor', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Sitescope Database')

        if ((key == 'r2c') & (np.column_stack([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df]))).any():
            cond = np.logical_and([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df],[df.CONFIGURATION_ITEM.str.contains('r2c', na=False, case=False) for col in df])
            mask = np.column_stack(cond)
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('R2C SRM')
        
        if (np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df])).any() :
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df]) 
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])
    
  


#TAKE SQL DATA AND PUT INTO PANDAS
mycursor.execute("SELECT * FROM RawPeople")
myresult = mycursor.fetchall()
df = DataFrame(myresult, columns=['NUMBER', 'SUMMARY', 'CONFIGURATION_ITEM', 'CREATED', 'COMPANY', 'ASSIGNMENT_GROUP', 'REASSIGNMENT_COUNT', 'PRIORITY', 'status', 'STATE', 'ITASK', 'ACTIVE', 'ACTIVITY_DUE', 'DUE_DATE', 'ADDITIONAL_ASSIGNEE_LIST', 'APPROVAL', 'TICKET_DURATION', 'APPROVAL_HISTORY', 'APPROVAL_SET', 'ASSIGNED_TO', 'BUSINESS_DURATION', 'CALLER', 'UPDATED', 'CATEGORY', 'U_CATEGORY', 'RESOLVE_TIME', 'RESOLVED_BY', 'RESOLVED_AT', 'RESOLVED_BY2', 'CREATED_BY', 'CREATED_DATE', 'LOCATION', 'HAS_ATTACHMENTS', 'HIDE_FROM_LIST', 'OPENED_BY', 'RECORD_SOURCE', 'ROOT_CAUSE_L1', 'ROOT_CAUSE_L2', 'ROOT_CAUSE_L3', 'PARENT', 'PARENT_INCIDENT', 'PARENT_INCIDENT3', 'CHANGE_REQUEST', 'CREATE_MAINTENANCE_WINDOW', 'MX_WINDOW_STATUS', 'START_MAINTENANCE', 'END_MAINTENANCE'])
logger.info('This is the data from SQL')
logger.info(df)
    
#MAP POD
df['POD'] = df.apply (lambda row: POD(row), axis=1)
logger.info('Now with POD column')
logger.info(df)
    

#MAP ALERT CATEGORY
AlertCat (df)
logger.info('Now with ALERT_CATEGORY column')
logger.info(df)
    
#EXPORT TO EXCEL SHEET
# fileName = input("File name to export to: ")
# sheet = input("Sheet name: ")
fileName = doc["File"]["filename"]
sheet = doc["File"]["sheetname"]
book = load_workbook(fileName)
        
writer = pd.ExcelWriter(fileName, engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        
df.to_excel(writer, sheet_name= sheet, index=False)
writer.save()   

    
