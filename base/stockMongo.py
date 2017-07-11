import pymongo
from bson.objectid import ObjectId

SYMBOL_INACTIVE = "inactive"

client = pymongo.MongoClient()
stockDb = client.stock

# columns = {"_id":1, "Date":1, "Open":1, "High":1, "Low":1, "Close":1, "Adj Close":1, "Volume":1}

def getAllActiveSymbols():
    return stockDb.symbol.find({ "Status": { "$ne": SYMBOL_INACTIVE } })

def insertQuotes(quotes): 
    stockDb.quote.insert_many(quotes)

def insertSymbols(symbols):
    stockDb.symbol.insert_many(symbols)
    
def updateSymbolStatus(symbol, status):
    symbol = findSymbol(symbol)
    if (symbol is not None):
        symbol['Status'] = status
        stockDb.symbol.save(symbol)
    
def updateSymbolQuoteSource(symbol, quoteSource):
    symbol = findSymbol(symbol)
    if (symbol is not None):
        symbol['quoteSource'] = quoteSource
        stockDb.symbol.save(symbol)

def updateQuoteNextClose(docId, nextClose, nextClosePercentage):
    stockDb.quote.update_one({'_id':docId}, {"$set": {"nextClose":nextClose,"nextClosePercentage":nextClosePercentage}}, upsert=False)
        
def findSymbol(symbol):
    return stockDb.symbol.find_one({"Symbol": symbol})

def findAllQuotesBySymbol(symbol):
    return stockDb.quote.find({"Symbol":symbol, "Close":{"$ne" : "null"}})

def findLatestQuotes(symbol, number):
    return stockDb.quote.find({"Symbol":symbol}).sort("Date",-1).limit(number)
                              
def findQuotesAfterDate(symbol, date):
    return stockDb.quote.find({"Symbol":symbol, "Date":{"$gt":date}})  

def findQuotesBySymbolDate(symbol, date):
    return stockDb.quote.find({"Symbol":symbol, "Date":date})  

def findLatestTwoDaysQuote(symbol): 
    return  stockDb.quote.find({"Symbol":symbol}).sort("Date",-1).limit(2)

def findPredictionByDate(mode, date, prediction):
    return stockDb.prediction.find({"Date":date, "Prediction": prediction, "Mode":mode})

def findLearnAccuracy(mode):
    return stockDb.learnAccuracy.find({"Mode":mode}) 

def findLatestPrediction(symbol, mode):
    return stockDb.prediction.find({"Symbol":symbol, "Mode":mode}).sort("Date", -1).limit(1)
    
def findLatestPredictionDate(mode):
    result = stockDb.prediction.find({"Mode":mode}).sort("Date", -1).limit(1)
    return list(result)[0]['Date']

def findDuplicatedQuotes():
    pipeline = [
                    {"$match": {"$and":[{"Date":{"$gte":"2017-03-08"}},{"Date":{"$lte":"2017-03-08"}}]} },
                    {"$group" : {"_id" : {"Symbol" : "$Symbol","Date" : "$Date"}, "count": {"$sum" : 1} } },
                    {"$match" : {"_id" : { "$ne" : "null" } , "count" : {"$gt": 1} }}
                ]
    return stockDb.quote.aggregate(pipeline=pipeline)

def saveLearnAccuracy(symbol, mode, confidence, trainNumber):             
    return stockDb.learnAccuracy.insert({"Symbol":symbol, "Mode":mode, "Confidence":confidence, "trainNumber":trainNumber})  

def savePrediction(predict):
    return stockDb.prediction.insert(predict)

def updatePredictionIsCorrect(symbol, date, result):
    prediction = stockDb.prediction.find_one({'Symbol':symbol, "Date":date})
    if prediction is None:
        print("No prediction for {} on date {}".format(symbol, date))
        return
    isCorrect = prediction['prediction'] == result
    return stockDb.prediction.update_one({'Symbol':symbol, "Date":date}, {"$set": {"isCorrect":isCorrect}}, upsert=False)

def deleteById(_id):
    return stockDb.quote.delete_one({'_id': ObjectId(_id)})