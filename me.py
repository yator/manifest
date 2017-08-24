# import mysql module
import cx_Oracle

# import regular expression module
import re

# set file name & location (note we need to create a temporary file because
# the original one is messed up)

file = open("C:\Users\AYator\Desktop\Trial.txt.txt", 'r')
file_content = open('C:\Users\AYator\Desktop\Trial.txt.txt', 'w')

# initialize & establish connection
db = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = db.cursor()

# prepare your ready file

for line in file:
    # substitute useless information this also creates some formatting for the
    # actuall loading into mysql
    line = re.sub('NAME|',  line)
    line = re.sub('TRIAL|', line)
    line = re.sub('LEVEL0|', line)
    line = re.sub('GENDER|', line)
    line = re.sub('LEVEL1|',  line)
    line = re.sub('LEVEL2|',  line)
    line = re.sub('LEVEL3', line)
    line = re.sub('LEVEL4|',  line)
    line = re.sub('LEVEL5|',  line)
    line = re.sub('LEVEL6|',  line)
    file_content.write(line)

# load your ready file into db

# close file
file.close()

# create a query
query =  """INSERT INTO ETL.MANIFEST(NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5,LEVEL6)
 values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
# run it
cursor.execute(query)
# commit just in case
db.commit()
cursor.close()
db.close()