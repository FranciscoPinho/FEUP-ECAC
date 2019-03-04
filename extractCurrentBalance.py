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

def getClientBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     WHERE type="OWNER"'''
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

def getClientMaxBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="OWNER" AND max_balance is null'''
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

def getClientMinBalances():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     WHERE type="OWNER" AND min_balance is null'''
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
    except Exception:
        traceback.print_exc()

def main():
    connectDatabase()
    #getClientBalances()
    #getClientMaxBalances()
    getClientMinBalances()

if __name__ == '__main__':
    main()  