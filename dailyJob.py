import multiprocessing
import functools
import datetime
import time
from stockMongo import getAllSymbols
from stockQuote import fetchAndStoreQuotes

if __name__ == '__main__':      
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is started...")
      
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    
    fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    
    symbols = getAllSymbols()
    
    multiprocessing.freeze_support()
    with multiprocessing.Pool(1) as p: # multiprocessing.cpu_count() -1
        p.map(fetchAndStore, symbols)
        
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is done...")