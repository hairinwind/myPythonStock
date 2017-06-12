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
    url = "https://finance.yahoo.com/quote/%s/?p=%s" % (symbol, symbol)
    r = requests.get(url)
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
    response = requests.get(url, cookies=cookie)
    data = []
    for block in response.iter_lines():
        data.append(block.decode("utf-8").split(","))
    if (len(data) > 1):
        try:
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            print(pd)


def get_now_epoch():
    # @see https://www.linuxquestions.org/questions/programming-9/python-datetime-to-epoch-4175520007/#post5244109
    return int(time.mktime(datetime.datetime.now().timetuple()))


def getQuotesFromYahoo(symbol):
    start_date = 0
    end_date = get_now_epoch()
    cookie, crumb = get_cookie_crumb(symbol)
    print('crumb', crumb)
    df = get_data(symbol, start_date, end_date, cookie, crumb)
    df.set_index(['Date'])
    return df
        
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