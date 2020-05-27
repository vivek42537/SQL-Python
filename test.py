import mysql.connector
from datetime import datetime

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "testdb"
)
print('helllooooo')
print(mydb)

mycursor = mydb.cursor()
#mycursor.execute("CREATE TABLE students (name VARCHAR(255), age INTEGER(10))")

# mycursor.execute("SHOW TABLES")

# for tb in mycursor:
#     print(tb)

sqlFormula = "INSERT INTO students (name, age) VALUES(%s, %s)"
students = [("Vivek" , 20), ("jacob" , 33), ("sam" , 90), ("bob" , 29)]


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
mycursor.execute("ALTER TABLE student ADD time MEDIUMINT primary key NOT NULL AUTO_INCREMENT")
mydb.commit

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

# sql = "DELETE FROM students WHERE name = 'zeebo'"
# mycursor.execute(sql)
# mydb.commit()

# nam = input("Name: ")
# num = input("Age: ")
#     #sqlFormula = "INSERT INTO people_info (Name, Country, Age) VALUES(%s,%s,%s)"
# values = [(nam, num)]
# mycursor.execute("INSERT INTO students (Name, Age) VALUES(%s,%s)",(nam, num))
# mydb.commit()
