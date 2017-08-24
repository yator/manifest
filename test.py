import cx_Oracle

conn = cx_Oracle.connect(
    user="scott",
    password="tiger",
    dsn=cx_Oracle.makedsn(
        "192.168.1.185", 1521, sid="xe",
    )
)

cursor = conn.cursor()
cursor.execute("select 1 from dual")

# run alter system kill session '<sid>, <session#>' here'
raw_input("stop the database, or kill the session, etc, press enter")

c2 = conn.cursor()

try:
    c2.execute("select 2 from data")
except cx_Oracle.DatabaseError as err:
    # cx_Oracle.DatabaseError: ORA-00028: your session has been killed
    print err

    # our session has been killed.  We have a dead socket in our application,
    # we need to clean it up unconditionally.

    # fails
    conn.close()