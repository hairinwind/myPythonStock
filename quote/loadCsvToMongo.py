from base.stockMongo import insertQuotes
from base import fileUtil
from quote.stockQuote import toDict 
import pandas as pd
import os
import shutil
import multiprocessing

def getQuotesCsvFileFullPath(csvFile):
    return fileUtil.QUOTES_DIR + csvFile

def moveQuotesCsvFile(file, targetDir):
    shutil.move(getQuotesCsvFileFullPath(file), targetDir)
    
def isDataFrameValid(df): 
    columns = list(df)
    columns.append(df.index.name)
    mFields = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Symbol']
    return all(w in columns for w in mFields)

def loadCsv(csvFile):
    try: 
        quotes = pd.read_csv(getQuotesCsvFileFullPath(csvFile), dtype={'Symbol': 'str'})
        if isDataFrameValid(quotes):
            print(quotes['Symbol'][0])
            quotes.reset_index(inplace=True)
            quotes.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            quotesJson = toDict(quotes)
            insertQuotes(quotesJson)
            print(len(quotes), ' quotes were inserted...')
            moveQuotesCsvFile(csvFile, fileUtil.QUOTES_SUCCESS_DIR)
        else:
            moveQuotesCsvFile(csvFile, fileUtil.QUOTES_ERROR_DIR)
    except:
        moveQuotesCsvFile(csvFile, fileUtil.QUOTES_ERROR_DIR)
        
def isFile(csvFile):
    return os.path.isfile(getQuotesCsvFileFullPath(csvFile))   

def loadAllQuoteFiles():
    csvFiles = list(filter(isFile, os.listdir(fileUtil.QUOTES_DIR))) 
    # csvFiles = ['INF_2017-06-21_2017-06-21.csv']
    with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
        p.map(loadCsv, csvFiles)

if __name__ == '__main__':
    multiprocessing.freeze_support()        
    loadAllQuoteFiles()
