import pymysql
import csv
import traceback
import re

#add behaviour info(iteration 3)
#...

def connectDatabase():
    """Create database connection"""
    global db
    db = pymysql.connect(host='localhost', user='root', password='',
                         db='banking', cursorclass=pymysql.cursors.DictCursor,charset='utf8mb4')


def getDisponentBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     WHERE type="DISPONENT"'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s
                         ORDER BY date DESC
                         LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                #if(len(balance)>0):
                    #balances[result["client_id"]]=balance[0]['balance']
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET most_recent_balance=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['balance'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()

def getDisponentMaxBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="DISPONENT" AND max_balance is null'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s ORDER BY balance DESC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET max_balance=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['balance'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()

def getDisponentMinBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="DISPONENT" AND min_balance is null'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s ORDER BY balance ASC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET min_balance=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['balance'],result["client_id"]])
                    db.commit()
                    cursor.close()
        print("DONE COLLECTING MIN BALANCE")
    except Exception:
        traceback.print_exc()

def getDisponentInfo():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id,decade,max_balance,min_balance,most_recent_balance FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="DISPONENT"'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            with db.cursor() as cursor:
                sql= '''update account SET disponent_decade=%s,disponent_max_balance=%s,disponent_recent_balance=%s,
                        disponent_min_balance=%s WHERE id=%s'''
                cursor.execute(sql,[result["decade"],result["max_balance"],result["most_recent_balance"],result["min_balance"],result["account_id"]])
                db.commit()
                cursor.close()
                print("UPDATED ACCOUNT "+str(result["account_id"]))
    except Exception:
        traceback.print_exc()

def getDisponentInfoExtra():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id,max_withdrawal,max_credit FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="DISPONENT"'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            with db.cursor() as cursor:
                sql= '''update account SET disponent_max_withdrawal=%s,disponent_max_credit=%s WHERE id=%s'''
                cursor.execute(sql,[result["max_withdrawal"],result["max_credit"],result["account_id"]])
                db.commit()
                cursor.close()
                print("UPDATED ACCOUNT "+str(result["account_id"]))
    except Exception:
        traceback.print_exc()

def main():
    connectDatabase()
    #getDisponentBalances()
    #getDisponentMaxBalances()
    #getDisponentMinBalances()
    #getDisponentInfo()
    getDisponentInfoExtra()

if __name__ == '__main__':
    main()  