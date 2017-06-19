from base.stockMongo import insertQuotes
from quote.stockQuote import convertToJson 
import pandas as pd
import os
import shutil
import multiprocessing

def moveFile(file, targetDir):
    shutil.move("d:/quotes/{}".format(file), targetDir)
    
def isDataFrameValid(df): 
    columns = list(df)
    columns.append(df.index.name)
    mFields = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Symbol']
    return all(w in columns for w in mFields)

def loadCsv(csvFile):
    try: 
        quotes = pd.DataFrame.from_csv("d:/quotes/{}".format(csvFile))
        if isDataFrameValid(quotes):
            print(quotes['Symbol'][0])
            quotesJson = convertToJson(quotes)
            insertQuotes(quotesJson)
            print(len(quotes), ' quotes were inserted...')
            moveFile(csvFile, "d:/quotes/success/")
        else:
            moveFile(csvFile, "d:/quotes/error/")
    except:
        moveFile(csvFile, "d:/quotes/error/")
        
def isFile(csvFile):
    return os.path.isfile("d:/quotes/{}".format(csvFile))        

if __name__ == '__main__':
    csvFiles = list(filter(isFile, os.listdir("d:/quotes")))    
    multiprocessing.freeze_support()        
    with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
        p.map(loadCsv, csvFiles)
