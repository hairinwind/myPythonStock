import csv
import pandas as pd
from base import stockMongo
from quote import stockQuote
from quote import loadCsvToMongo
from machinelearning import machineLearning
from machinelearning import machineLearningRunner

def readCsv(csvfile):
    with open(csvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    return rows

def addNewSymbol(symbol): 
    print(symbol)
    existSymbol = stockMongo.findSymbol(symbol['Symbol'])
    if existSymbol is None:
        stockMongo.insertSymbols([symbol])
    
    # collect all history data
    stockQuote.fetchAndStoreQuotes(symbol)
    loadCsvToMongo.loadAllQuoteFiles();
    quotes = stockMongo.findQuotesBySymbolPeriod(symbol['Symbol'],'1900-01-01', '2100-01-01')
    stockQuote.getAndSaveNextTxDayData(pd.DataFrame(list(quotes)))
    
    # machine learning, create pickle, save accurancy
    for ml in machineLearning.machineLearningModes : 
        machineLearningRunner.learn(ml, None, symbols=[symbol['Symbol']])


def loadSymbolFromCsv(csvFile): 
    symbols = loadCsvToMongo.loadSymbolFromCsv(csvFile)
    symbols.reset_index(inplace=True)
    
    if (len(symbols) > 0):
        records = symbols.reset_index()
        records.drop(['level_0', 'index'], axis=1, inplace=True, errors='ignore')
        for symbol in records.to_dict(orient='records'):
            symbol['isETF']=True 
            addNewSymbol(symbol)
            

if __name__ == '__main__':    
#     rows = readCsv('./data/companylist.csv') 
#     rows.extend(readCsv('./data/companylist1.csv'))
#     rows.extend(readCsv('./data/companylist2.csv'))
#     
#     for row in rows:
#         del row['']
#         print(row)
#     
#     insertSymbols(rows)
#     
#     print(len(rows), 'symbols were inserted...')

# add new symbol
#     symbol = {"Symbol":"ANDV", "Name":"Andeavor", "sp500":True}
#     symbol = {"Symbol":"BHGE", "Name":"Baker Hughes, a GE company", "sp500":True}
#     symbol = {"Symbol":"DWDP", "Name":"DowDuPont Inc.", "sp500":True}
#     symbol = {"Symbol":"TPR", "Name":"Tapestry, Inc.", "sp500":True}
#     addNewSymbol(symbol);
    
    loadSymbolFromCsv("../data/ETF_101.csv")
