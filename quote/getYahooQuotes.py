"""
get-yahoo-quotes.py:  Script to download Yahoo historical quotes using the new cookie authenticated site.
 Usage: get-yahoo-quotes SYMBOL
 History
 06-03-2017 : Created script
"""

__author__ = "Brad Luicas"
__copyright__ = "Copyright 2017, Brad Lucas"
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Brad Lucas"
__email__ = "brad@beaconhill.com"
__status__ = "Production"


import re
import sys
import time
import datetime
import requests
import pandas as pd


def split_crumb_store(v):
    return v.split(':')[2].strip('"')


def find_crumb_store(lines):
    # Looking for
    # ,"CrumbStore":{"crumb":"9q.A4D1c.b9
    for l in lines:
        if re.findall(r'CrumbStore', l):
            return l
    print("Did not find CrumbStore")


def get_cookie_value(r):
    return {'B': r.cookies['B']}


def get_page_data(symbol):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    url = "https://finance.yahoo.com/quote/%s/?p=%s" % (symbol, symbol)
    print("quote URL:", url)
    r = requests.get(url, headers=headers)
    cookie = get_cookie_value(r)
    # lines = r.text.encode('utf-8').strip().replace('}', '\n')
    lines = r.content.strip().decode("utf-8").replace('}', '\n')
    return cookie, lines.split('\n')


def get_cookie_crumb(symbol):
    cookie, lines = get_page_data(symbol)
    crumb = split_crumb_store(find_crumb_store(lines))
    # Note: possible \u002F value
    # ,"CrumbStore":{"crumb":"FWP\u002F5EFll3U"
    # FWP\u002F5EFll3U
    # crumb2 = crumb.decode('unicode-escape')
    return cookie, crumb


def get_data(symbol, start_date, end_date, cookie, crumb):
    url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=1d&events=history&crumb=%s" % (symbol, start_date, end_date, crumb)
    print(url)
    response = requests.get(url, cookies=cookie)
    data = []
    for block in response.iter_lines():
        data.append(block.decode("utf-8").split(","))
    if (len(data) > 1):
        try:
            df = pd.DataFrame(data[1:], columns=data[0])
            df['Symbol'] = symbol
            return df
        except Exception as e:
            print(pd)


def get_now_epoch():
    # @see https://www.linuxquestions.org/questions/programming-9/python-datetime-to-epoch-4175520007/#post5244109
    # return int(time.mktime(datetime.datetime.now().timetuple()))
    return int(time.time())

def getTimeNumber(date):
    return int((date - datetime.datetime(1970,1,1)).total_seconds())

def parseDate(dateStr, hour):
    date = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
    return date.replace(hour=hour)

def getQuotesFromYahooInternal(symbol, start, end):
    start_date = 0
    end_date = get_now_epoch()
    if start is not None:
        start_date = getTimeNumber(parseDate(start, 7))
    if end is not None:
        end_date = getTimeNumber(parseDate(end, 17))
    cookie, crumb = get_cookie_crumb(symbol)
    print('symbol', symbol)
    print('crumb', crumb)
    df = get_data(symbol, start_date, end_date, cookie, crumb)
    df.set_index(['Date'], inplace=True)
    return df

# symbol: ATI
# start: 2017-06-12
# end: 2017-06-12
def getQuotesFromYahoo(symbol, start, end):
    retryCount = 0
    while retryCount < 5 :
        try:
            return getQuotesFromYahooInternal(symbol, start, end)
        except Exception as e:
            print(str(e))
            retryCount += 1
        
if __name__ == '__main__':
    # If we have at least one parameter go ahead and loop overa all the parameters assuming they are symbols
    if len(sys.argv) == 1:
        print("\nUsage: get-yahoo-quotes.py SYMBOL\n\n")
    else:
        for i in range(1, len(sys.argv)):
            symbol = sys.argv[i]
            print("--------------------------------------------------")
            print("Downloading %s to %s.csv" % (symbol, symbol))
            getQuotesFromYahoo(symbol)
            print("--------------------------------------------------")