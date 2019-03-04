import pymysql
import csv
import traceback
import re

#start with clients(iteration 1)
#add district info related to clients(iteration 2)
#add behaviour info(iteration 3)
#...

def connectDatabase():
    """Create database connection"""
    global db
    db = pymysql.connect(host='localhost', user='root', password='',
                         db='banking', cursorclass=pymysql.cursors.DictCursor,charset='utf8mb4')


def convertCSVToSQL():
    addDistricts()
    addClients()
    addAccounts()
    addDisposition()
    addCardTestTrain()
    addLoanTestTrain()
    addTransTestTrain()

def addDistricts():
    try:
        with open('district.csv','r') as csv_districts:
            districts = csv.reader(csv_districts,delimiter=";")
            headers=next(districts)
            print(headers)
            for row in districts:
                with db.cursor() as cursor:
                    # Create a new record
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                    sql = 'INSERT INTO district VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()

def addClients():
    try:
        with open('client.csv','r') as csv_cli:
            clients = csv.reader(csv_cli,delimiter=";")
            headers=next(clients)
            print(headers)
            for row in clients:
                with db.cursor() as cursor:
                    # Create a new record
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                    sql = 'INSERT INTO client VALUES (%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()

def addAccounts():
    try:
        with open('account.csv','r') as csv_accounts:
            accs = csv.reader(csv_accounts,delimiter=";")
            headers=next(accs)
            print(headers)
            for row in accs:
                with db.cursor() as cursor:
                    # Create a new record
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                            continue
                        if(idx==3):
                            dateChunks=re.findall('..?',str(attribute))
                            year = ''
                            year='19'+dateChunks[0] if int(dateChunks[0])>20 else '20'+dateChunks[0]
                            month=dateChunks[1]
                            day=dateChunks[2]
                            date = year+'-'+month+'-'+day
                            row[idx]=date
                    sql = 'INSERT INTO account VALUES (%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        traceback.print_exc()

def addDisposition():
    try:
        with open('disp.csv','r') as csv_disp:
            dispositions = csv.reader(csv_disp,delimiter=";")
            headers=next(dispositions)
            print(headers)
            for row in dispositions:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                    sql = 'INSERT INTO disposition VALUES (%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()

def addCardTestTrain():
    try:
        with open('card_test.csv','r') as csv_cardtest:
            cardtests = csv.reader(csv_cardtest,delimiter=";")
            headers=next(cardtests)
            print(headers)
            for row in cardtests:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                    sql = 'INSERT INTO card_test VALUES (%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
        with open('card_train.csv','r') as csv_cardtrains:
            cardtrains = csv.reader(csv_cardtrains,delimiter=";")
            headers=next(cardtrains)
            for row in cardtrains:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                    sql = 'INSERT INTO card_train VALUES (%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()

def addLoanTestTrain():
    try:
        with open('loan_test.csv','r') as csv_loantest:
            loantests = csv.reader(csv_loantest,delimiter=";")
            headers=next(loantests)
            print(headers)
            for row in loantests:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?" or attribute==''):
                            row[idx]=None
                            continue
                        if(idx==2):
                            dateChunks=re.findall('..?',str(attribute))
                            year = ''
                            year='19'+dateChunks[0] if int(dateChunks[0])>20 else '20'+dateChunks[0]
                            month=dateChunks[1]
                            day=dateChunks[2]
                            date = year+'-'+month+'-'+day
                            row[idx]=date
                    sql = 'INSERT INTO loan_test VALUES (%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
        with open('loan_train.csv','r') as csv_loantrains:
            loantrains = csv.reader(csv_loantrains,delimiter=";")
            headers=next(loantrains)
            print(headers)
            for row in loantrains:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?"):
                            row[idx]=None
                            continue
                        if(idx==2):
                            dateChunks=re.findall('..?',str(attribute))
                            year = ''
                            year='19'+dateChunks[0] if int(dateChunks[0])>20 else '20'+dateChunks[0]
                            month=dateChunks[1]
                            day=dateChunks[2]
                            date = year+'-'+month+'-'+day
                            row[idx]=date
                    sql = 'INSERT INTO loan_train VALUES (%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()

def addTransTestTrain():
    try:
        with open('trans_test.csv','r') as csv_transtest:
            transtests = csv.reader(csv_transtest,delimiter=";")
            headers=next(transtests)
            print(headers)
            for row in transtests:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?" or attribute==''):
                            row[idx]=None
                            continue
                        if(idx==2):
                            dateChunks=re.findall('..?',str(attribute))
                            year = ''
                            year='19'+dateChunks[0] if int(dateChunks[0])>20 else '20'+dateChunks[0]
                            month=dateChunks[1]
                            day=dateChunks[2]
                            date = year+'-'+month+'-'+day
                            row[idx]=date
                    sql = 'INSERT INTO trans_test VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
        with open('trans_train.csv','r') as csv_transtrains:
            transtrains = csv.reader(csv_transtrains,delimiter=";")
            headers=next(transtrains)
            print(headers)
            for row in transtrains:
                with db.cursor() as cursor:
                    for idx,attribute in enumerate(row):
                        if(attribute=="?" or attribute==''):
                            row[idx]=None
                            continue
                        if(idx==2):
                            dateChunks=re.findall('..?',str(attribute))
                            year = ''
                            year='19'+dateChunks[0] if int(dateChunks[0])>20 else '20'+dateChunks[0]
                            month=dateChunks[1]
                            day=dateChunks[2]
                            date = year+'-'+month+'-'+day
                            row[idx]=date
                    sql = 'INSERT INTO trans_train VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql, row)
                    db.commit()
    except Exception:
        print(row)
        traceback.print_exc()


def resetDatabase():
    with db.cursor() as cursor:
        sql = 'DELETE FROM district'
        cursor.execute(sql)
        sql = 'DELETE FROM client'
        cursor.execute(sql)
        sql = 'DELETE FROM account'
        cursor.execute(sql)
        sql = 'DELETE FROM disposition'
        cursor.execute(sql)
        sql='DELETE FROM card_test'
        cursor.execute(sql)
        sql='DELETE FROM card_train'
        cursor.execute(sql)
        sql='DELETE FROM loan_test'
        cursor.execute(sql)
        sql='DELETE FROM loan_train'
        cursor.execute(sql)
        sql='DELETE FROM trans_test'
        cursor.execute(sql)
        sql='DELETE FROM trans_train'
        cursor.execute(sql)
        db.commit()
    
def main():
    connectDatabase()
    #resetDatabase()
    convertCSVToSQL()

if __name__ == '__main__':
    main()  