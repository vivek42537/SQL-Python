import pandas as pd
import mysql.connector
import csv

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


#mycursor.execute('CREATE TABLE people (Name nvarchar(50), Country nvarchar(50), Age int)')

# for row in df.itertuples():
#     mycursor.execute("INSERT INTO people (Name, Country, Age) VALUES (%s,%s,%s)",
#         (row.Name, 
#         row.Country,
#         row.Age))


#mydb.commit()


operation = input("Press 1, 2, 3, or 4 for correspongind CRUD operation: ")
if (operation == '1'): #CREATE -insert row
    nam = input("Name: ")
    place = input("Country: ")
    num = input("Age: ")
    #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
    values = [(nam, place, num)]
    mycursor.execute("INSERT INTO people (Name, Country, Age) VALUES(%s,%s,%s)",(nam, place, num))
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

# sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
# values = ("zeebo", "poland", 98)
# mycursor.execute(sqlFormula, values)
# mydb.commit