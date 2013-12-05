'''
Created on Oct 3, 2013

@author: zhk
'''
# nasdaq ftp://ftp.nasdaqtrader.com/
# yahoo finance http://biz.yahoo.com/p/s_conameu.html
# bo http://www.bseindia.com/markets/equity/EQReports/MarketWatch.aspx?expandable=2
# ns http://www.nseindia.com/products/content/equities/equities/homepage_eq.htm
# sz http://www.szse.cn/main/en/marketdata/stockinformation/

import urllib2
from time import gmtime, strftime, sleep
import MySQLdb

log_directory = 'log/'
base_url = 'http://chartapi.finance.yahoo.com/instrument/1.0/{ticker}/chartdata;type=quote;range=1d/csv'
conn = MySQLdb.connect(host = '128.31.7.93', user = 'zhk', passwd = 'g0373485x', db = 'stock', port = 3306)
table = 'stock'
default_interval = 3

def load_tickers(file_name, prefix = '', suffix = ''):
    f = open(file_name)
    tickers = [prefix + x.strip() for x in f.readlines()]
    return tickers

def grab_ticker(ticker):
    print ticker
    try:
        response = urllib2.urlopen(base_url.format(ticker = ticker))
        data = response.readlines()
        f = open(log_directory + ticker + '_' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '.csv', 'w')
        f.writelines(data)
        f.close()
        rows = []
        for line in data[15:]:
            rows = rows + [ticker] + line.strip().split(',')
        if len(rows) == 0:
            with open("failed", "a") as myfile:
                myfile.write(ticker + '\t' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '\n')
        else:
            query = '''INSERT IGNORE INTO {} (ticker, timestamp, close, high, low, open, volume) VALUES ''' + ','.join(['''('{}', {}, {}, {}, {}, {}, {}) '''] * (len(rows) / 7))
            query = query.format(table, *rows)
            conn.cursor().execute(query)
    except Exception, e:
        print ticker, e
        return False
    return True

def grab_tickers(ticker):
    interval = 0
    for ticker in tickers:
        while not grab_ticker(ticker):
            interval = interval * 3
            print 'waiting for', interval, 'seconds'
            sleep(interval)
        interval = default_interval
        sleep(default_interval)

if __name__ == '__main__':
    tickers = load_tickers('s&p')
    grab_tickers(tickers)