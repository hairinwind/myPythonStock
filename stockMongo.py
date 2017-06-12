import pymongo

client = pymongo.MongoClient()
# stockDb = client.stock
stockDb = client.bulk_example

def getAllSymbols():
    return stockDb.symbol.find()

def insertQuotes(quotes): 
    stockDb.quote.insert_many(quotes)

def insertSymbols(symbols):
    stockDb.symbol.insert_many(symbols)
    
def updateSymbolQuoteSource(symbol, quoteSource):
    symbol = findSymbol(symbol)
    if (symbol != None):
        symbol['quoteSource'] = quoteSource
        print(symbol)
        stockDb.symbol.save(symbol)
        
def findSymbol(symbol):
    return stockDb.symbol.find_one({"Symbol": symbol})