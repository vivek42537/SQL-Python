import pandas as pd
from pandas import ExcelWriter
from openpyxl import load_workbook
import mysql.connector
import csv

df = pd.read_csv (r'students.csv')
print (df)
# df = df.rename(columns={'Name ': 'Name'})
# df = df.rename(columns={'Country ': 'Country'})
# df = df.rename(columns={'Age ': 'Age'})
df.columns = df.columns.str.strip()
print (df.columns)


# mydb = mysql.connector.connect(
#     host = "localhost",
#     user = "root",
#     passwd = "password",
#     database = "test2"
# )

mydb = mysql.connector.connect(
    host = input("host: "),
    user = input("user: "),
    passwd = input("password: "),
    database = input("database: ")
)
print(mydb)

mycursor = mydb.cursor()


#mycursor.execute('CREATE TABLE people_info (Name nvarchar(50), Country nvarchar(50), Age int)')

# for row in df.itertuples():
#     mycursor.execute("INSERT INTO people_info (Name, Country, Age) VALUES (%s,%s,%s)",
#         (row.Name, 
#         row.Country,
#         row.Age))


# mydb.commit()
operation = ""
while (operation != 'done'):
    operation = input("Press 1, 2, 3, or 4 for corresponding CRUD operation, OR press 5 to export to excel file: ")

    if (operation == '1'): #CREATE -insert row
        nam = input("Name: ")
        place = input("Country: ")
        num = input("Age: ")
        #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
        values = [(nam, place, num)]
        mycursor.execute("INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)",(nam, place, num))
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
        mycursor.execute("UPDATE people_info SET Age = %s WHERE Name = %s",(age, nam))
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