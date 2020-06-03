import mysql.connector
from datetime import datetime
import yaml

with open('test1.yaml', 'r') as yam:
    doc = yaml.safe_load(yam)

hostInfo = doc["DatabaseInfo"]["host"]
userInfo = doc["DatabaseInfo"]["user"]
passwdInfo = doc["DatabaseInfo"]["passwd"]
databaseInfo = doc["DatabaseInfo"]["database"]

#print(doc["Numbers"])
Numbers = doc["Numbers"]
#print(Numbers[0])
# if Numbers[0] + Numbers[1] + Numbers[2] == 9 :
#     print('BOOYAH')
# for nums in doc["Numbers"] :
#     print (nums[1])
# x= 0
# while Numbers[x] != 1 :
#     x = x + 1
#     print(x)
# for value in doc["Numbers"].values() :
#     print (value)

for key,value in doc["Numbers"].items():
    if key == 'Goal':
         print(value)

mydb = mysql.connector.connect(
    host = hostInfo,
    user = userInfo,
    passwd = passwdInfo,
    database = databaseInfo
)
# mydb = mysql.connector.connect(
#     host = "localhost",
#     user = "root",
#     passwd = "password",
#     database = "testdb"
# )
print(mydb)

mycursor = mydb.cursor()
#mycursor.execute("CREATE TABLE students (name VARCHAR(255), age INTEGER(10))")

# mycursor.execute("SHOW TABLES")

# for tb in mycursor:
#     print(tb)

# sqlFormula = "INSERT INTO students (name, age) VALUES(%s, %s)"
# students = [("Vivek" , 20), ("jacob" , 33), ("sam" , 90), ("bob" , 29)]


# #mycursor.execute(sqlFormula, student1)
# mycursor.executemany(sqlFormula, students)

# mydb.commit() #this command makes it show in database
nam = input("Name: ")
mycursor.execute("SELECT * FROM students WHERE name = '%s'",(nam))
myresult = mycursor.fetchall() #gets specific data you want can use 'fetchone' if you want just a single entry

# myresult = mycursor.fetchone()
# for row in myresult:
#     print(row)

now = datetime.now()
id = 1
formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
# Assuming you have a cursor named cursor you want to execute this query on:
#mycursor.execute('insert into students(id, date_created) values(%s, %s)', (id, formatted_date))


#sql = "SELECT * FROM students WHERE name LIKE '%a%'"

#sql = "UPDATE students SET age = 13 WHERE name = 'Jacob'"

# mycursor.execute("SELECT * FROM students LIMIT 3 OFFSET 2") 



# sql = "SELECT * FROM students ORDER BY name DESC"
# mycursor.execute(sql)
# myresult = mycursor.fetchall()

# for result in myresult:
#         print(result)
# sqlFormula = "INSERT INTO students (Name, Age) VALUES(%s,%s)"
# values = ("pedro", 76)
# mycursor.execute(sqlFormula, values)
# mydb.commit()

sql = "DELETE FROM students WHERE name = 'Charles'"
mycursor.execute(sql)
mydb.commit()

# nam = input("Name: ")
# num = input("Age: ")
#     #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
# values = [(nam, num)]
# mycursor.execute("INSERT INTO students (Name, Age) VALUES(%s,%s)",(nam, num))
# mydb.commit()
