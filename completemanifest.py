import xlrd
import cx_Oracle
import datetime as dt
from os.path import join, dirname, abspath
import glob
import cx_Oracle
import time

dbcon = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = dbcon.cursor()

# file = open('C:/Users/AYator/Desktop/Trial.txt.txt','r')
rows = []
_mdate = (time.strftime('%d-%b-%Y'))
print(_mdate)
_mtype = 'C:/Users/AYator/Desktop/Trial.txt.txt'
for filename in glob.glob(_mtype):
    fh = open(filename, 'r')
    _mfilename = filename
    [_mflight, _mfltdat, _mfightfrom, _mbispax, _meconpax, _mdeptime, _mboardtime] = ['', '', '', '', '', '', '']
    [_mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1, _mfield2, _mticketnumber] = ['', '', '',
                                                                                                             '', '', '',
                                                                                                             '', '', '',
                                                                                                             '', '']
    for lines in fh:
        _mline = lines
        # print(_mline)
        _mstring = _mline

        if _mstring[0:4] == 'LIST':
            _mbispax = _mstring[47:50].strip()
            _meconpax = _mstring[51:55].strip()
        if _mstring[0:2] == 'KQ':
            _mflight = _mstring[0:9].strip()
            _mfltdat = _mstring[9:16].strip()
            _mfightfrom = _mstring[16:19].strip()
            _mdeptime = _mstring[23:28].strip()
            _mboardtime = _mstring[46:51].strip()

        # print( _mstring)
        if _mstring[3:4] == '.':
            # print( _mstring)
            _mitem = _mstring[0:3].strip()
            _mname = _mstring[5:27].strip()
            _mpaxtype = _mstring[27:28].strip()
            _mfrom = _mstring[29:33].strip()
            _mto = _mstring[33:37].strip()
            _mrbd = _mstring[38:39].strip()
            _mclass = _mstring[40:41].strip()
            _mseat = _mstring[42:46].strip()
            _mfield1 = _mstring[47:53].strip()

        if _mstring[0:47] == '                                               ':
            if _mitem != '' and _mstring[48:49] == '/':
                _mfield2 = _mstring[47:64].strip()
            if _mitem != '' and _mstring[48:49] != '/':
                _mticketnumber = _mstring[47:60].strip()
                #                 print(_mticketnumber)
                _mrow = [_mflight, _mfltdat, _mfightfrom, _mbispax, _meconpax, _mdeptime, _mboardtime, _mitem, _mname,
                         _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1, _mfield2, _mticketnumber,
                         _mfilename, _mdate]
                rows.append(_mrow)
                # [_mitem,_mname,_mpaxtype,_mfrom,_mto,_mrbd,_mclass,_mseat,_mfield1,_mfield2,_mticketnumber]=['','','','','','','','','','','']

    fh.close
    # cursor.prepare('INSERT INTO ETL.MANIFEST((NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5,LEVEL6) values(:1,:2,:3,:4,:5,:6,:7,:8,:9)')
    cursor.prepare(
        'insert into ETL.MANIFEST(FLIGHTNUMBER,FLIGHTDATE,FIGHTFROM,BISPAX,ECONPAX,  deptime,  boardtime,ITEM,PAXNAME,PAXTYPE,FRM,TOO,RBD,CLASSTRAVEL,SEAT,FIELD1,FIELD2,TICKETNUMBER,filename,procdate) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)')
    cursor.executemany(None, rows)
    dbcon.commit()
    print("Done Processing File: " + filename)
    cursor.close()
    dbcon.close()