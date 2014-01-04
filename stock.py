'''
Created on Dec 11, 2013

@author: zhk
'''
import MySQLdb

DB_HOST = '128.31.7.128'
DB_PORT = 3306
DB_NAME = 'stock'
DB_USER = 'zhk'
DB_PASS = 'G0373485x'

class Stock(object):
    def __init__(self, ticker):
        pass
    
    
conn = MySQLdb.connect(host = DB_HOST, user = DB_USER, passwd = DB_PASS, db = DB_NAME, port = 3306)
query = '''SELECT * FROM stock s WHERE ticker = '{}' '''
query = query.format('MMM')

        