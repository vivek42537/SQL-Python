import mysql.connector
from datetime import datetime
import yaml
import ruamel.yaml
from ruamel.yaml import YAML
from ruamel.yaml.constructor import SafeConstructor

with open('test1.yaml', 'r') as yam:
    doc = yaml.safe_load(yam)

# def construct_yaml_map(self, node):
#     # test if there are duplicate node keys
#     keys = set()
#     for key_node, value_node in node.value:
#         key = self.construct_object(key_node, deep=True)
#         if key in keys:
#             break
#         keys.add(key)
#     else:
#         data = {}  # type: Dict[Any, Any]
#         yield data
#         value = self.construct_mapping(node)
#         data.update(value)
#         return
#     data = []
#     yield data
#     for key_node, value_node in node.value:
#         key = self.construct_object(key_node, deep=True)
#         val = self.construct_object(value_node, deep=True)
#         data.append((key, val))

# SafeConstructor.add_constructor(u'tag:yaml.org,2002:map', construct_yaml_map)
# yaml = YAML(typ='safe')
# with open('test1.yaml', 'r') as yam:
#     doc = yaml.load(yam)
#data = yaml.load(yam)
 # {'foo': 1, 'bar': 2}

# with open('test1.yaml', 'r') as f:
#     data = f.read()
#     data = data.replace('\n\n', '\n---\n')

#     for doc in yaml.load_all(data):
#         print(doc)

# print(doc)

hostInfo = doc["DatabaseInfo"]["host"]
userInfo = doc["DatabaseInfo"]["user"]
passwdInfo = doc["DatabaseInfo"]["passwd"]
databaseInfo = doc["DatabaseInfo"]["database"]

#print(doc["Numbers"])
x = doc
def testy(y):
    for key,value in doc["DatabaseInfo"].items():
        if key == 'gicc' :
            return value
    
        else:
            for key,value in doc["Numbers"].items():
                if key == 'three' :
                    return value[0]
        #for key,value in doc["Numbers"].items():
            #if key == 'three' :
                #print ('POOP')
# x = 5           
# if x == 5 :
#     print (x)
# elif x == 5 :
#     print ('BADDY')
# for key,value in doc["Numbers"].items():
#     if key.lower() == 'goal':
#         print(value)
print (testy(x))

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
#nam = input("Name: ")
#mycursor.execute("SELECT * FROM students WHERE name = '%s'",(nam))
#myresult = mycursor.fetchall() #gets specific data you want can use 'fetchone' if you want just a single entry

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
