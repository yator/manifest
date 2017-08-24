import xlrd
import cx_Oracle
import datetime as dt
from os.path import join, dirname, abspath
import glob
import cx_Oracle

dbcon = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = dbcon.cursor()

# file = open('C:/Users/AYator/Desktop/Trial.txt.txt','r')
rows = []
_mtype ='C:/Users/AYator/Desktop/Trial.txt.txt'
for filename in glob.glob(_mtype):
    fh = open(filename, 'r')
    _mfilename = filename
    [_mflight, _mfltdat, _mfightfrom, _mbispax, _meconpax, _mdeptime, _mboardtime] = ['', '', '', '', '', '', '']
    for lines in fh:
        _mline = lines
        # print(_mline)
        _mstring = _mline
        [_mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1, _mfield2, _mticketnumber] = ['', '',
                                                                                                                 '', '',
                                                                                                                 '', '',
                                                                                                                 '', '',
                                                                                                                 '', '',
                                                                                                                 '']
        if _mstring[0:4] == 'LIST':
            _mbispax = _mstring[48:50].strip()
            _meconpax = _mstring[52:55].strip()
        if _mstring[0:2] == 'KQ':
            _mflight = _mstring[0:9].strip()
            _mfltdat = _mstring[10:16].strip()
            _mfightfrom = _mstring[17:19].strip()
            _mdeptime = _mstring[24:27].strip()
            _mboardtime = _mstring[48:51].strip()
        if _mstring[49:50] == '/':
            _mfield2 = _mstring[48:64].strip()
        if _mstring[48:50] == '706':
            _mticketnumber = _mstring[48:60].strip()
            print(_mstring)

        # print( _mstring)
        if _mstring[3:4] == '.':
            # print( _mstring)
            _mitem = _mstring[0:3].strip()
            _mname = _mstring[5:27].strip()
            _mpaxtype = _mstring[28:29].strip()
            _mfrom = _mstring[29:33].strip()
            _mto = _mstring[33:37].strip()
            _mrbd = _mstring[38:39].strip()
            _mclass = _mstring[40:41].strip()
            _mseat = _mstring[43:46].strip()
            _mfield1 = _mstring[48:53].strip()

            _mrow = [_mflight, _mfltdat, _mfightfrom, _mbispax, _meconpax, _mdeptime, _mboardtime, _mitem, _mname,
                     _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1, _mfield2, _mticketnumber, _mfilename]
            rows.append(_mrow)
    fh.close
    # cursor.prepare('INSERT INTO ETL.MANIFEST((NAME,TRIAL,LEVEL0,GENDER,LEVEL1,LEVEL2,LEVEL3,LEVEL4,LEVEL5,LEVEL6) values(:1,:2,:3,:4,:5,:6,:7,:8,:9)')
    cursor.prepare(
        'insert into ETL.MANIFEST_TEST(FLIGHTNUMBER,FLIGHTDATE,FIGHTFROM,BISPAX,ECONPAX,  deptime,  boardtime,ITEM,PAXNAME,PAXTYPE,FRM,TOO,RBD,CLASSTRAVEL,SEAT,FIELD1,FIELD2,TICKETNUMBER,filename) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)')