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

# import sqlalchemy
# from sqlalchemy import create_engine
# engine = create_engine('mysql+pymysql://root:password@localhost:3306/test2')
#fileName = input("File Name: ")
#df = pd.read_excel (r'Book3.xlsx')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s\n:%(message)s')
fileHandler = logging.FileHandler('two.log')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# logging.basicConfig(filename = 'two.log', level = logging.DEBUG, format = '%(levelname)s:%(name)s\n:%(message)s')



fileName = input("File name: ")
df = pd.read_excel (fileName)
df = df.where((pd.notnull(df)), None)
df['CREATED'] = df['CREATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['UPDATED'] = df['UPDATED'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['RESOLVED_AT'] = df['RESOLVED_AT'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['REASSIGNMENT_COUNT'] = df['REASSIGNMENT_COUNT'].apply(str)

# df['Resolve_time'] = df['Resolve_time'].apply(str)
#print (df.dtypes)
# df['Book3'] = pd.to_datetime(df['Created'],unit='ms')
# df.to_excel('new_Book3.xlsx', index=False)

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
    for key,value in doc["ALERTcat"].items():
        if key == 'KEEP ALIVE' or 'NEND' :
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=True) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = value[1]

        if ((key == 'backup') & (np.column_stack([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df])) & (np.column_stack([df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df]))).any():
            mask = np.column_stack([df.SUMMARY.str.contains('backup', na=False, case=False) for col in df] and [df.U_CATEGORY.str.contains('storage', na=False, case=False) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Backup Alert')

        if ((key == 'good' or key == 'metric' or key == 'monitor') & (np.column_stack([df.SUMMARY.str.contains('database', na=False, case=False) for col in df]))).any():
            mask = np.column_stack([df.SUMMARY.str.contains('database', na=False, case=False) for col in df] and [df.SUMMARY.str.contains('metric|good|monitor', na=False, case=False) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Sitescope Database')
        
        if ((key == 'r2c') & (np.column_stack([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df]))).any():
            mask = np.column_stack([df.CONFIGURATION_ITEM.str.contains('srm', na=False, case=False) for col in df] and [df.CONFIGURATION_ITEM.str.contains('r2c', na=False, case=False) for col in df])
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('R2C SRM')
        
        if (np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df])).any():
            mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df]) 
            df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])
    
    df.fillna('Other', inplace=True)

# def AlertCat (df):
#     for key,value in doc["ALERTcat"].items():
#         if key == 'KEEP ALIVE' or 'NEND' :
#             mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=True) for col in df])
#             df.loc[mask.any(axis=1), 'ALERT_CAT'] = value[1]

#         mask = np.column_stack([df[value[0]].str.contains(key, na=False, case=False) for col in df]) 
#         df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value[1])

#     df.fillna('Other', inplace=True)
        


       # df.loc[mask.any(axis=1), 'ALERT_CAT'] = value[1]
    
    # for key,value in doc["Summary"].items():
    #     mask = np.column_stack([df.SUMMARY.str.contains(key, na=False, case=False) for col in df]) 
    #     if ((value == 'Sitescope Others') & (np.column_stack([df.SUMMARY.str.contains('database', na=False, case=False) for col in df]))).any():
    #         df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna('Sitescope Database')
    #     else:
    #         df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value)
    #     #df.loc[mask.any(axis=1), 'ALERT_CAT'] = value
    #     # df[df['ALERT_CAT'].isnull()].loc[mask.any(axis=1), 'ALERT_CAT'] = value
    
    # for key,value in doc["Category_u_category"].items():
    #     mask = np.column_stack([df.U_CATEGORY.str.contains(key, na=False, case=False) for col in df]) 
    #     df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value)
    
    # for key,value in doc["Assignment_group"].items():
    #     mask = np.column_stack([df.ASSIGNMENT_GROUP.str.contains(key, na=False, case=False) for col in df]) 
    #     df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value)
    
    # for key,value in doc["Company"].items():
    #     mask = np.column_stack([df.COMPANY.str.contains(key, na=False, case=False) for col in df]) 
    #     df.loc[mask.any(axis=1), 'ALERT_CAT'] = df.loc[mask.any(axis=1), 'ALERT_CAT'].fillna(value)
    
    #df.fillna('Other', inplace=True)
    

# AlertCat (df)
# print (df)
# def AlertCat (row):
    
#     for key,value in doc["Configuration Item"].items():
#         if (row['Configuration_item'] is not None) and (key.lower() in row['Configuration_item'].lower()) :
#             return value
#         else:
    
#             for key,value in doc["Summary"].items():
#                 if (row['Summary'] is not None) and (key.lower() in row['Summary'].lower()) :
#                     if (value.lower() == 'sitescope alert') and ('database' in row['Summary'].lower()) :
#                         return 'Sitescope Database'
#                     else:
#                         return value
                    
#                 else:
    
#                     for key,value in doc["Category_u_category"].items():
#                         if (row['Category_u_category'] is not None) and (key.lower() in row['Category_u_category'].lower()) :
#                             if ('backup' in row['Summary'].lower()) :
#                                 return 'Backup Alert' 
#                             if ('backup' not in row['Summary'].lower()) :
#                                 return value
#                         else:

#                             for key,value in doc["Assignment_group"].items():
#                                 if (row['Assignment_group'] is not None) and (key.lower() in row['Assignment_group'].lower()) :
#                                     return value
#                                 else:

#                                     for key,value in doc["Company"].items():
#                                         if (row['Company'] is not None) and (key.lower() in row['Company'].lower()) :
#                                             return value
                                                 

#                                         else:
#                                             for key,value in doc["Num"].items():
#                                                 if (row['Number'] is not None) and (key.lower() in row['Number'].lower()) :
#                                                     return value



operation = ""
while (operation != 'done'):
    operation = input("Press 1, 2, 3, or 4 for to Create table in SQL, Insert data into SQL, ")

    if (operation == '1'): #CREATE TABLE
        mycursor.execute('CREATE TABLE RawPeople (Number nvarchar(50), Summary LONGTEXT, `Configuration_item` LONGTEXT, Created nvarchar(50), Company LONGTEXT, `Assignment_group` nvarchar(50), `Reassignment_count` nvarchar(50), Priority nvarchar(50), Status nvarchar(50), State nvarchar(50), `Assigned_to` nvarchar(50), Caller nvarchar(50), Updated nvarchar(50), Category nvarchar(50), `Category_u_category` nvarchar(50), `Resolve_time` nvarchar(50), `Resolved_By` nvarchar(50), `Resolved_at` nvarchar(50), `Resolved_by2` nvarchar(50), `Created_by` nvarchar(50), `Created_date` nvarchar(50), Location nvarchar(50))')
        mydb.commit()
    
    elif (operation == '2'): #INSERT DATA INTO SQL
        for row in df.itertuples():
            mycursor.execute("INSERT INTO RawPeople (Number, Summary, Configuration_item, Created, Company, Assignment_group, Reassignment_count, Priority, Status, State, Assigned_to, Caller, Updated, Category, Category_u_category, Resolve_time, Resolved_By, Resolved_at, Resolved_by2, Created_by, Created_date, Location) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (row.Number, 
            row.Summary, 
            row.Configuration_item,
            row.Created, 
            row.Company, 
            row.Assignment_group, 
            row.Reassignment_count, 
            row.Priority, 
            row.Status, 
            row.State, 
            row.Assigned_to, 
            row.Caller, 
            row.Updated, 
            row.Category, 
            row.Category_u_category,
            row.Resolve_time, 
            row.Resolved_By, 
            row.Resolved_at, 
            row.Resolved_by2, 
            row.Created_by, 
            row.Created_date, 
            row.Location))
        mydb.commit()
    
    elif (operation == '3'): #TAKE SQL DATA AND PUT INTO PANDAS
        mycursor.execute("SELECT * FROM RawPeople")
        myresult = mycursor.fetchall()
        df = DataFrame(myresult, columns=['Number', 'Summary', 'Configuration_item', 'Created', 'Company', 'Assignment_group', 'Reassignment_count', 'Priority', 'Status', 'State', 'Assigned_to', 'Caller', 'Updated', 'Category', 'Category_u_category', 'Resolve_time', 'Resolved_By', 'Resolved_at', 'Resolved_by2', 'Created_by', 'Created_date', 'Location'])
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

    
