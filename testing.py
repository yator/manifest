
import cx_Oracle
import os
import string

# Open database connection
db = cx_Oracle.connect ('etl', 'etl', '10.2.2.50:1521/dwhdev')

cursor=db.cursor()

#Query under testing
query = """LOAD DATA LOCAL INFILE 'C:\Users\AYator\Desktop\Trial.txt.txt' \
      INTO TABLE ETL.MANIFEST\
      FIELDS TERMINATED BY ',' \
      OPTIONALLY ENCLOSED BY '"'  \
      LINES TERMINATED BY '\r\n' \
      IGNORE 1 LINES;;"""
cursor.execute(query)
 # Commit your changes in the database
db.commit()
   # Rollback in case there is any error
db.rollback()
cursor.close()
# disconnect from server
db.close()