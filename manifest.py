import cx_Oracle
db = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = db.cursor()

file = open('C:/Users/AYator/Desktop/Trial.txt.txt','r')
file_content = file.read()

file.close()

query = """INSERT INTO ETL.MANIFEST(NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5,LEVEL6)
 values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
values = [line.split() for line in file_content]
cursor.execute(query,values)

db.commit()
cursor.close()
db.close()