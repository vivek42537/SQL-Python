import pandas as pd
from pandas import ExcelWriter
from openpyxl import load_workbook
import mysql.connector
import csv
# import sqlalchemy
# from sqlalchemy import create_engine
# engine = create_engine('mysql+pymysql://root:password@localhost:3306/test2')

df = pd.read_excel (r'Book3.xlsx')
df = df.where((pd.notnull(df)), None)
df['Created'] = df['Created'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['Updated'] = df['Updated'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['Resolved_at'] = df['Resolved_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['Reassignment_count'] = df['Reassignment_count'].apply(str)
# df['Resolve_time'] = df['Resolve_time'].apply(str)
print (df.dtypes)
# df['Book3'] = pd.to_datetime(df['Created'],unit='ms')
# df.to_excel('new_Book3.xlsx', index=False)
#print (df)

df.columns = df.columns.str.strip()
print (df.columns)

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "test2"
)
# print (mydb)
mycursor = mydb.cursor()
# mycursor.execute('CREATE TABLE people14 (Number nvarchar(50), Summary LONGTEXT, `Configuration_item` nvarchar(50), Created nvarchar(50), Company nvarchar(50), `Assignment_group` nvarchar(50), `Reassignment_count` nvarchar(50), Priority nvarchar(50), Status nvarchar(50), State nvarchar(50), `Assigned_to` nvarchar(50), Caller nvarchar(50), Updated nvarchar(50), Category nvarchar(50), `Category_u_category` nvarchar(50), `Resolve_time` nvarchar(50), `Resolved_By` nvarchar(50), `Resolved_at` nvarchar(50), `Resolved_by2` nvarchar(50), `Created_by` nvarchar(50), `Created_date` nvarchar(50), Location nvarchar(50))')
# #df.to_sql('rawData', mydb, if_exists='replace', index = False)
# mydb.commit()

# df.to_sql('people13', con=engine)
for row in df.itertuples():
    mycursor.execute("INSERT INTO people14 (Number, Summary, Configuration_item, Created, Company, Assignment_group, Reassignment_count, Priority, Status, State, Assigned_to, Caller, Updated, Category, Category_u_category, Resolve_time, Resolved_By, Resolved_at, Resolved_by2, Created_by, Created_date, Location) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
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
#df.to_sql('people13', con=engine)
mydb.commit()

# query = "INSERT INTO dataRaw (Number, Summary, Configuration item(cmdb_ci), Created, Company, Assignment group, Reassignment count, Priority, Status, State, Assigned to, Caller, Updated, Category, Category(u_category), Resolve time, Resolved By, Resolved at, Resolved by2, Created by, Created date, Location) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# for r in range(1, sheet.nrows):
#     Number = sheet.cell(r,0).value 
#     Summary = sheet.cell(r,1).value 
#     'Configuration item(cmdb_ci)'  = sheet.cell(r,2).value 
#     Created = sheet.cell(r,3).value 
#     Company = sheet.cell(r,4).value 
#     'Assignment group' = sheet.cell(r,5).value 
#     'Reassignment count' = sheet.cell(r,6).value 
#     Priority = sheet.cell(r,7).value 
#     Status = sheet.cell(r,8).value 
#     State = sheet.cell(r,9).value 
#     'Assigned to' = sheet.cell(r,10).value 
#     Caller = sheet.cell(r,11).value 
#     Updated = sheet.cell(r,12).value 
#     Category = sheet.cell(r,13).value 
#     Category(u_category) = sheet.cell(r,14).value 
#     'Resolve time' = sheet.cell(r,15).value 
#     'Resolved By' = sheet.cell(r,16).value 
#     'Resolved at' = sheet.cell(r,17).value 
#     'Resolved by2' = sheet.cell(r,18).value 
#     'Created by' = sheet.cell(r,19).value 
#     'Created date' = sheet.cell(r,20).value 
#     Location = sheet.cell(r,21).value 

#     values = (Number, Summary, Configuration item(cmdb_ci), Created, Company, Assignment group, Reassignment count, Priority, Status, State, Assigned to, Caller, Updated, Category, Category(u_category), Resolve time, Resolved By, Resolved at, Resolved by2, Created by, Created date, Location)
#     mycursor.execute(query,values)
#     mydb.commit()