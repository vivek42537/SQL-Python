import pandas as pd
import mysql.connector

df = pd.read_excel (r'cell phone data.xlsx')
print (df)


mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "Bdyhbyp0@s",
    database = "testdb"
)

print(mydb)

mycursor = mydb.cursor()