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
    passwd = "Bdyhbyp0@s",
    database = "testdb"
)

print(mydb)

mycursor = mydb.cursor()


#mycursor.execute('CREATE TABLE people_info (Name nvarchar(50), Country nvarchar(50), Age int)')

for row in df.itertuples():
    mycursor.execute("INSERT INTO people_info (Name, Country, Age) VALUES (%s,%s,%s)",
        (row.Name, 
        row.Country,
        row.Age))


mydb.commit()
