import mysql.connector

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "Bdyhbyp0@s",
    database = "testdb"
)

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

# mycursor.execute("SELECT age FROM students")
# #myresult = mycursor.fetchall() #gets specific data you want can use 'fetchone' if you want just a single entry

# myresult = mycursor.fetchone()
# for row in myresult:
#     print(row)

#sql = "SELECT * FROM students WHERE name LIKE '%a%'"

#sql = "UPDATE students SET age = 13 WHERE name = 'Jacob'"

# mycursor.execute("SELECT * FROM students LIMIT 3 OFFSET 2") 



# sql = "SELECT * FROM students ORDER BY name DESC"
# mycursor.execute(sql)
# myresult = mycursor.fetchall()

# for result in myresult:
#         print(result)


sql = "DELETE FROM students WHERE name = 'Vivek'"
mycursor.execute(sql)
mydb.commit()