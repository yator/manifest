import glob
import cx_Oracle
import time
import os.path
import datetime

# dbcon = cx_Oracle.connect('flightops', 'fl1ght0ps', '10.2.2.50:1521/dwhdev')
dbcon = cx_Oracle.connect('etl', 'etl', '10.2.2.35:1521/warehouse')
cursor = dbcon.cursor()
now = datetime.datetime.now()
now -= datetime.timedelta(days=1)
_mdate2 = (now.strftime("%d %b %y"))
_mdate = (time.strftime('%d %b %Y'))
# print (_mdate2)
# print(_mdate)
#_mtype = 'D:/My_Data/Credit_Recon/*KQ*.txt'
_mtype = '/u02/businessobjects/dataservices/getemails/rev.doc@kenya-airways.com_data/*.txt'
# _mtype = '/u02/businessobjects/dataservices/getemails/rev.doc@kenya-airways.com_data/*'+_mdate2+'*.txt'
_mcount = 1
for filename in glob.glob(_mtype):
    fh = open(filename, 'r')
    _mfilename = filename

    _mpos1 = _mfilename.index('(')
    _mpos2 = _mfilename.index(')')
    _mfilename2 = _mfilename[_mpos1 + 1:_mpos2].strip()
    _mdate3 = _mfilename2[0:2] + "-" + _mfilename2[3:6] + "-" + _mfilename2[9:11]
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(_mfilename)
    _mdate2 = time.ctime(mtime)[4:7] + '-' + time.ctime(mtime)[8:10]

    # print(_mcount)
    [_mflight, _mfltdat, _mfightfrom, _mbispax, _meconpax, _mdeptime, _mboardtime] = ['', '', '', '', '', '', '']
    [_mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mfield1, _mfield2, _mticketnumber, _mtransit] = [
        '', '', '', '', '', '', '', '', '', '', '', '']
    rows = []
    for lines in fh:
        _mline = lines
        # print(_mline)
        _mstring = _mline
        _mtransit = 'N'
        if _mstring[0:4] == 'LIST':
            mtotalpax = 0
            if ' J' in _mstring and ' M' in _mstring:
                _mpos = _mstring.index(' J')
                _mtotalstring1 = _mstring[_mpos:].strip()
                # print(_mtotalstring1)
                _mbispax = _mtotalstring1[_mtotalstring1.index('J') + 1:_mtotalstring1.index(' M')].strip()
                _meconpax = _mtotalstring1[_mtotalstring1.index(' M') + 2:_mtotalstring1.index(' TOTAL')].strip()
            # print(_mbispax)
            else:
                print("Error in File: " + _mfilename)
            if 'TOTAL' in _mstring:
                _mtotalpax = int(_mtotalstring1[_mtotalstring1.index(' TOTAL') + 6:].strip())
            else:
                print("Error in File: " + _mfilename)


                # print(_mtotalpax)
        if _mstring[0:2] == 'KQ':
            _mflight = _mstring[0:9].strip()
            _mfltdat = _mstring[9:14].strip()
            _mfltdat1 = _mfltdat[0:2] + '-' + _mfltdat[2:5] + '-' + time.ctime(mtime)[8:10]
            # print(_mfltdat1)
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
            _mboarded = _mstring[47:53].strip()

        if _mstring[0:47] == '                                               ':
            if _mitem != '' and _mstring[48:49] == '/':
                _mfield2 = _mstring[47:64].strip()
            if _mitem != '' and _mstring[48:49] != '/':
                _mticketnumber = _mstring[47:60].strip()
                if _mstring[0:50] == '                                               TRT':
                    _mtransit = 'Y'
                    # print(_mtransit)
                    #   _mtransit = 'Y'
                    # print(_mflight,_mfltdat,_mfightfrom,_mbispax,_meconpax,_mdeptime,_mboardtime,_mitem,_mname,_mpaxtype,_mfrom,_mto,_mrbd,_mclass,_mseat,_mfield1,_mfield2,_mticketnumber,_mfilename,_mdate)
                #                 print(_mticketnumber)
                if _mstring[0:50] != '                                               TRT':
                    _mrow = [_mflight, _mdate3, _mfightfrom, _mbispax, _meconpax, _mtotalpax, _mdeptime, _mboardtime,
                             _mitem, _mname, _mpaxtype, _mfrom, _mto, _mrbd, _mclass, _mseat, _mboarded, _mfield2,
                             _mticketnumber, _mtransit, _mfilename, _mdate]
                    rows.append(_mrow)
    fh.close
   # print("Done Processing File: " + filename)
    if _mtotalpax > 0 and _mtransit != 'Y':
        cursor.prepare(
            'insert into dwh_foundation.T_CM_MANIFEST(FLIGHTNUMBER,FLIGHTDATE,FIGHTFROM,BISPAX,ECONPAX,TOTALPAX,  deptime,  boardtime,ITEM,PAXNAME,PAXTYPE,FRM,TOO,RBD,CLASSTRAVEL,SEAT,BOARDED,FIELD2,TICKETNUMBER,TRANSIT,filename,procdate) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)')
        cursor.executemany(None, rows)
        dbcon.commit()
        _mcount = _mcount + 1
cursor = dbcon.cursor()
dbcon.close()