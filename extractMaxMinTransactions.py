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



def getMaxWithdrawal():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     '''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s and type like "withdrawal%%" ORDER BY amount DESC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET max_withdrawal=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['amount'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()

def getMaxCredit():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                  '''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s and type like "credit%%" ORDER BY amount DESC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET max_credit=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['amount'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()


def getMinWithdrawal():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                     '''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s and type like "withdrawal%%" ORDER BY amount ASC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET min_withdrawal=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['amount'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()

def getMinCredit():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT account_id,client_id FROM account
                     LEFT JOIN disposition
                     ON account.id=disposition.account_id
                     LEFT JOIN client
                     ON disposition.client_id=client.id
                  '''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            balance=[]
            with db.cursor() as cursor:
                sql = '''select * from trans_union WHERE account_id=%s and type like "credit%%" ORDER BY amount ASC LIMIT 1'''
                cursor.execute(sql,[result["account_id"]])
                balance = cursor.fetchall()
                cursor.close() 
            if(len(balance)>0):
                with db.cursor() as cursor:
                    sql= '''update client SET min_credit=%s WHERE id=%s'''
                    cursor.execute(sql,[balance[0]['amount'],result["client_id"]])
                    db.commit()
                    cursor.close()
    except Exception:
        traceback.print_exc()

def main():
    connectDatabase()
    #getMaxCredit()
    #getMaxWithdrawal()
    getMinCredit()
    getMinWithdrawal()

if __name__ == '__main__':
    main()  