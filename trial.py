import xlrd
import cx_Oracle

list = xlrd.open_workbook("C:\Users\AYator\Desktop\kq.xlsx.xlsx")
worksheet = list.sheet_by_index(0)

database = cx_Oracle.connect('etl', 'etl', '10.2.2.50:1521/dwhdev')
cursor = database.cursor()
for r in range(1, worksheet.nrows):
    CHAIN_ID = worksheet.cell(r, 0).value
    SUBMISSION_MID = worksheet.cell(r, 1).value
    DBA = worksheet.cell(r, 2).value
    OUTLET = worksheet.cell(r, 3).value
    SUBMISSION_DATE = worksheet.cell(r, 4).value
    TERMINAL_ID = worksheet.cell(r, 5).value
    BATCH_NBR = worksheet.cell(r, 6).value
    ITEM_NBR = worksheet.cell(r, 7).value
    CARD_NUMBER = worksheet.cell(r, 8).value
    CARD_TYPE = worksheet.cell(r, 9).value
    AUTH_CODE = worksheet.cell(r, 10).value
    AUTH_SRC_CODE = worksheet.cell(r, 11).value
    POS_ENTRY_MODE = worksheet.cell(r, 12).value
    TRANSACTION_TYPE = worksheet.cell(r, 13).value
    DCC_ELIGIBLE = worksheet.cell(r, 14).value
    DCC_IND = worksheet.cell(r, 15).value
    CARDHOLDER_AMT = worksheet.cell(r, 16).value
    CARD_CURR = worksheet.cell(r, 17).value
    TRANSACTION_AMT = worksheet.cell(r, 18).value
    TRANS_CURR = worksheet.cell(r, 19).value
    CASHBACK_AMOUNT = worksheet.cell(r, 20).value
    CSH_BCK_CURR = worksheet.cell(r, 21).value
    TRANSACTION_DATE = worksheet.cell(r, 22).value
    TRANSACTION_TIME = worksheet.cell(r, 23).value
    RRN = worksheet.cell(r, 24).value
    TRANS_REF_TEXT = worksheet.cell(r, 25).value
    VOID_INDICATOR = worksheet.cell(r, 26).value
    CUSTOM_DATA = worksheet.cell(r, 27).value
    BATCH_CONTROL_NBR = worksheet.cell(r, 28).value
    CONVERSION_RATE = worksheet.cell(r, 29).value
    PAPER_ELECTRONIC = worksheet.cell(r, 30).value
    WALLET_TYPE = worksheet.cell(r, 31).value
    WALLET_DATA = worksheet.cell(r, 32).value

    query = """insert into CREDIT_CARD_TRANS_LIST (CHAIN_ID,SUBMISSION_MID,DBA,OUTLET,SUBMISSION_DATE,TERMINAL_ID,BATCH_NBR,ITEM_NBR,CARD_NUMBER,CARD_TYPE,
       AUTH_CODE,AUTH_SRC_CODE,POS_ENTRY_MODE,TRANSACTION_TYPE,DCC_ELIGIBLE,DCC_IND,CARDHOLDER_AMT,CARD_CURR,
        TRANSACTION_AMT,TRANS_CURR,CASHBACK_AMOUNT,CSH_BCK_CURR,TRANSACTION_DATE,TRANSACTION_TIME,RRN,
        TRANS_REF_TEXT,VOID_INDICATOR,CUSTOM_DATA,BATCH_CONTROL_NBR,CONVERSION_RATE, PAPER_ELECTRONIC,WALLET_TYPE, WALLET_DATA)
         values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"%s","%s","%s",
                  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )""" % (
        CHAIN_ID, SUBMISSION_MID, DBA, OUTLET, SUBMISSION_DATE, TERMINAL_ID, BATCH_NBR, ITEM_NBR, CARD_NUMBER,
        CARD_TYPE,
        AUTH_CODE, AUTH_SRC_CODE, POS_ENTRY_MODE, TRANSACTION_TYPE, DCC_ELIGIBLE, DCC_IND, CARDHOLDER_AMT, CARD_CURR,
        TRANSACTION_AMT, TRANS_CURR, CASHBACK_AMOUNT, CSH_BCK_CURR, TRANSACTION_DATE, TRANSACTION_TIME, RRN,
        TRANS_REF_TEXT, VOID_INDICATOR, CUSTOM_DATA, BATCH_CONTROL_NBR, CONVERSION_RATE, PAPER_ELECTRONIC, WALLET_TYPE,
        WALLET_DATA)
    # execute query
    cursor.execute(query)

    # Commit the transaction
    database.commit()
    # Close the cursor
    cursor.close()
    # Close the database connection
    database.close()
print("All done !")
columns = str(worksheet.ncols)
rows = str(worksheet.nrows)
print ("i just import " + columns + " columns and " + rows + " rows ")

