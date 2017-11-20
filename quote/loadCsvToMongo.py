import multiprocessing
import numpy as np
import os
import shutil

from base import fileUtil
from base.parallel import runToAllDone
from base.stockMongo import insertQuotes
import pandas as pd
from quote.stockQuote import toDict 


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
            quotes = quotes.replace('null', np.nan)
            quotes = quotes.dropna(how='any')
            quotesJson = toDict(quotes)
            insertQuotes(quotesJson)
            print(len(quotes), ' quotes were inserted...')
            fileUtil.archiveQuoteFileToZip(fileUtil.QUOTES_SUCCESS_DIR, getQuotesCsvFileFullPath(csvFile))
        else:
            moveQuotesCsvFile(csvFile, fileUtil.QUOTES_ERROR_DIR)
    except:
        moveQuotesCsvFile(csvFile, fileUtil.QUOTES_ERROR_DIR)
        
def isFile(csvFile):
    return os.path.isfile(getQuotesCsvFileFullPath(csvFile))   

def loadAllQuoteFiles():
    csvFiles = list(filter(isFile, os.listdir(fileUtil.QUOTES_DIR))) 
    # csvFiles = ['INF_2017-06-21_2017-06-21.csv']
    # with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
    runToAllDone(loadCsv, [(csvFile,) for csvFile in csvFiles])
    
    
def loadSymbolFromCsv(symbolCsv):
    return pd.read_csv(symbolCsv, dtype={'Symbol': 'str', 'Name':'str'})
    

if __name__ == '__main__':
    multiprocessing.freeze_support()        
    loadAllQuoteFiles()
