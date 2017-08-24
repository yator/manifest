import glob
import cx_Oracle

dbcon = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = dbcon.cursor()

# file = open('C:/Users/AYator/Desktop/Trial.txt.txt','r')
rows = []
_mtype = 'D:/Trial.txt.txt'
for filename in glob.glob(_mtype):
    fh = open(filename, 'r')

    for lines in fh:
        _mline = lines
        # print(_mline)
        _mstring = _mline
        [_mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1] = ['', '', '', '', '', '', '', '',
                                                                                       '']
        # print( _mstring)
        if _mstring[3:4] == '.':
            # print( _mstring)
            _mitem = _mstring[0:3]
            _mname = _mstring[5:27]
            _mpaxtype = _mstring[28:29]
            _mfrom = _mstring[29:33]
            _mto = _mstring[33:37]
            _mrbd = _mstring[38:39]
            _mclass = _mstring[40:41]
            _mseat = _mstring[43:46]
            _mfield1 = _mstring[48:53]

            _mrow = [_mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1]
            rows.append(_mrow)
    fh.close
    # cursor.prepare('INSERT INTO ETL.MANIFEST((NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5,LEVEL6) values(:1,:2,:3,:4,:5,:6,:7,:8,:9)')
    cursor.prepare(
        'insert into ETL.MANIFEST(NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5) values(:1,:2,:3,:4,:5,:6,:7,:8,:9)')
    cursor.executemany(None, rows)
    dbcon.commit()
    print("Done Processing File: " + filename)
    dbcon.close()