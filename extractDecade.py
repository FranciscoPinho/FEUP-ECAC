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

def getClientDecade():
    try:
        results=[]
        with db.cursor() as cursor:
            sql = '''SELECT id,birth_number FROM client'''
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
        for result in results:
            decade=str(result["birth_number"])[0]+"0"
            with db.cursor() as cursor:
                sql= '''update client SET decade=%s WHERE id=%s'''
                cursor.execute(sql,[decade,result["id"]])
                db.commit()
                cursor.close()
    except Exception:
        traceback.print_exc()
def main():
    connectDatabase()
    getClientDecade()

if __name__ == '__main__':
    main()  