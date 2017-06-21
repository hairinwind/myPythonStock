import pymongo

client = pymongo.MongoClient()
stockDb = client.stock

# columns = {"_id":1, "Date":1, "Open":1, "High":1, "Low":1, "Close":1, "Adj Close":1, "Volume":1}

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

def updateQuoteNextClose(docId, nextClose, nextClosePercentage):
    stockDb.quote.update_one({'_id':docId}, {"$set": {"nextClose":nextClose,"nextClosePercentage":nextClosePercentage}}, upsert=False)
        
def findSymbol(symbol):
    return stockDb.symbol.find_one({"Symbol": symbol})

def findAllQuotesBySymbol(symbol):
    return stockDb.quote.find({"Symbol":symbol, "Close":{"$ne" : "null"}})

def findLatestQuotes(symbol, number):
    return stockDb.quote.find({"Symbol":symbol}).sort("Date",-1).limit(number)
                              
def findQuotesAfterDate(symbol, lastLearningDate):
    return stockDb.quote.find({"Symbol":symbol, "Date":{"$gt":lastLearningDate}})    

def findLatestTwoDaysQuote(symbol): 
    return  stockDb.quote.find({"Symbol":symbol}).sort("Date",-1).limit(2)

def saveLearnAccuracy(symbol, mode, confidence, trainNumber):             
    return stockDb.learnAccuracy.insert({"Symbol":symbol, "Mode":mode, "Confidence":confidence, "trainNumber":trainNumber})  

def savePrediction(predict):
    return stockDb.prediction.insert(predict)