import pymongo

client = pymongo.MongoClient()
stockDb = client.stock

def getAllSymbols():
    return stockDb.symbol.find()

def insertQuotes(quotes): 
    stockDb.quote.insert_many(quotes)

def insertSymbols(symbols):
    stockDb.symbol.insert_many(symbols)
    
def updateSymbolQuoteSource(symbol, quoteSource):
    symbol = findSymbol(symbol)
    if (symbol is not None):
        symbol['quoteSource'] = quoteSource
        print(symbol)
        stockDb.symbol.save(symbol)
        
def findSymbol(symbol):
    return stockDb.symbol.find_one({"Symbol": symbol})

def findAllQuotesBySymbol(symbol):
    return stockDb.quote.find({"Symbol":symbol}, {"_id":0, "Date":1, "Open":1, "High":1, "Low":1,"Close":1, "Adj Close":1, "Volume":1})

def findLatestQuotes(symbol, number):
    return stockDb.quote.find({"Symbol":symbol}
                              , {"_id":0, "Date":1, "Open":1, "High":1, "Low":1,"Close":1, "Adj Close":1, "Volume":1}).sort("Date",-1).limit(number)
                              
def findQuotesAfterDate(symbol, lastLearningDate):
    return stockDb.quote.find({"Symbol":symbol, "Date":{"$gt":lastLearningDate}})     

def saveLearnAccuracy(symbol, mode, confidence, trainNumber):             
    return stockDb.learnAccuracy.insert({"Symbol":symbol, "Mode":mode, "Confidence":confidence, "trainNumber":trainNumber})  