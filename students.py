import pandas as pd
from pandas import ExcelWriter
from openpyxl import load_workbook
import mysql.connector
import csv
import time
import datetime

ts = time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')



df = pd.read_csv (r'students.csv')
print (df)
# df = df.rename(columns={'Name ': 'Name'})
# df = df.rename(columns={'Country ': 'Country'})
# df = df.rename(columns={'Age ': 'Age'})
df.columns = df.columns.str.strip()
print (df.columns)


mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "testdb"
)

print(mydb)

mycursor = mydb.cursor()
#mycursor.execute("ALTER TABLE people ADD id MEDIUMINT primary key NOT NULL AUTO_INCREMENT")
# mycursor.execute("ALTER TABLE people ADD Created DATETIME2(3) CONSTRAINT DF_YourTable_Created DEFAULT (SYSDATETIME())")
# mycursor.execute("""INSERT into people (test_date) VALUES ('%s')""",(timestamp))
# print('TIME:', timestamp)
   
#pd.read_sql('SELECT * FROM people',mydb).to_excel('foo.xlsx')
#mycursor.execute('CREATE TABLE people (Name nvarchar(50), Country nvarchar(50), Age int)')

# for row in df.itertuples():
#     mycursor.execute("INSERT INTO people (Name, Country, Age) VALUES (%s,%s,%s)",
#         (row.Name, 
#         row.Country,
#         row.Age))


# mydb.commit()
# mycursor.execute('CREATE TABLE people2 (Name nvarchar(50), Country nvarchar(50), Age int, updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(), created_at TIMESTAMP NOT NULL DEFAULT NOW())')

# for row in df.itertuples():
#     mycursor.execute("INSERT INTO people2 (Name, Country, Age) VALUES (%s,%s,%s)",
#         (row.Name, 
#         row.Country,
#         row.Age))

#mycursor.execute("SELECT * FROM people WHERE Name = 'Aria'")
#mycursor.execute("ALTER TABLE people DROP test_date")
book = load_workbook('foo.xlsx')
writer = pd.ExcelWriter('foo.xlsx', engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

pd.read_sql('SELECT * FROM people',mydb).to_excel(writer, sheet_name = 'RawData')
writer.save()
mydb.commit()
operation = ""
while(operation != 'done'):
    operation = input("Press 1, 2, 3, or 4 for correspongind CRUD operation, OR press 5 to export to excel file: ")
    if (operation == '1'): #CREATE -insert row
        nam = input("Name: ")
        place = input("Country: ")
        num = input("Age: ")
        #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
        values = [(nam, place, num)]
        mycursor.execute("INSERT INTO people (Name, Country, Age) VALUES(%s,%s,%s)",(nam, place, int(num)))
        mydb.commit()

    elif (operation == '2'): #READ -fetch data
        nam = input("Name: ")
        sql = "SELECT * FROM people WHERE Name = '{0}'"
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
        mycursor.execute("UPDATE people SET Age = %s WHERE Name = %s",(age, nam))
        mydb.commit()

    elif (operation == '4'): #DELETE - delete data
        nam = input("Name: ")
        sql = "DELETE FROM people WHERE Name = '{0}'"
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
        pd.read_sql('SELECT * FROM people',mydb).to_excel(writer, sheet_name = sheet)
        writer.save()

# sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
# values = ("zeebo", "poland", 98)
# mycursor.execute(sqlFormula, values)
# mydb.commit

