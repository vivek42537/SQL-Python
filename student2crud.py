import pandas as pd
from pandas import ExcelWriter
from pandas import DataFrame
from openpyxl import load_workbook
import mysql.connector
import csv
import time
import datetime
import logging 

logging.basicConfig(filename = 'one.log', level = logging.DEBUG, format = '%(levelname)s:%(name)s:%(message)s')


ts = time.time()
timestamp = (datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),)
df = pd.read_csv (r'students.csv')

logging.info(df)
# df = df.rename(columns={'Name ': 'Name'})
# df = df.rename(columns={'Country ': 'Country'})
# df = df.rename(columns={'Age ': 'Age'})
df.columns = df.columns.str.strip()
logging.info(df.columns)


mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "test2"
)
userName = input("user: ")
# mydb = mysql.connector.connect(
#     host = input("host: "),
#     #user = input("user: ")
#     user = userName,
#     passwd = input("password: "),
#     database = input("database: ")
# )
# print(mydb)

mycursor = mydb.cursor()


#mycursor.execute('CREATE TABLE people_info (Name nvarchar(50), Country nvarchar(50), Age int)')

# for row in df.itertuples():
#     mycursor.execute("INSERT INTO people_info (Name, Country, Age) VALUES (%s,%s,%s)",
#         (row.Name, 
#         row.Country,
#         row.Age))

#mycursor.execute("ALTER TABLE people_info ADD COLUMN User nvarchar(50)")

# print (df)
mycursor.execute("SELECT * FROM people_info")
myresult = mycursor.fetchall()
#myresult2 = mycursor.keys()
df = DataFrame(myresult, columns=['Name','Country','Age','Created','User'])
#df.columns = myresult2
#print (df)

def coolGuy (row):
    if row['Name'] == 'Bill' or ('Viv' in row['Name'] and 'China' in row['Country']):
        return 'Best'
    elif row['Name'] == 'Maria' :
        return 'Loser'
        

#print(df.apply (lambda row: coolGuy(row), axis=1))
df['Cool?'] = df.apply (lambda row: coolGuy(row), axis=1)
print(df)

mydb.commit()
operation = ""
while (operation != 'done'):
    operation = input("Press 1, 2, 3, or 4 for corresponding CRUD operation, OR press 5 to export to excel file: ")

    if (operation == '1'): #CREATE -insert row
        nam = input("Name: ")
        place = input("Country: ")
        num = input("Age: ")
        #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
        values = [(nam, place, num)]
        ts = time.time()
        timestamp = (datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),)
        mycursor.execute("INSERT INTO people_info (Name, Country, Age, Created, User) VALUES(%s,%s,%s,%s,%s)",(nam, place, int(num),timestamp[0],userName))
        mydb.commit()

    elif (operation == '2'): #READ -fetch data
        nam = input("Name: ")
        sql = "SELECT * FROM people_info WHERE Name = '{0}'"
        query = sql.format(str(nam))
        mycursor.execute(query)
        myresult = mycursor.fetchall() #gets specific data you want can use 'fetchone' if you want just a single entry
        print("sugoi")
        for row in myresult:
            print(row)

    elif (operation == '3'): #UPDATE - replace data
        nam = input("Name: ")
        age = input("Age: ")
        #place = input("Country:")
        ts = time.time()
        timestamp = (datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),)
        mycursor.execute("UPDATE people_info SET Age = %s WHERE Name = %s",(age, nam))
        mycursor.execute("UPDATE people_info SET Created = %s WHERE Name = %s",(timestamp[0], nam))
        mycursor.execute("UPDATE people_info SET User = %s WHERE Name = %s",(userName, nam))
        mydb.commit()

    elif (operation == '4'): #DELETE - delete data
        nam = input("Name: ")
        sql = "DELETE FROM people_info WHERE Name = '{0}'"
        query = sql.format(str(nam))
        mycursor.execute(query)
        mydb.commit()

    elif (operation == '5'): #EXPORT to excel file
        fileName = input("File name: ")
        sheet = input("Sheet name: ")
        book = load_workbook(fileName)
        writer = pd.ExcelWriter(fileName, engine='openpyxl')
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        pd.read_sql('SELECT * FROM people_info',mydb).to_excel(writer, sheet_name = sheet)
        writer.save()