from base.stockMongo import updateSymbolAddNewProperty
from base.stockMongo import findSymbol 

def loadsp500FromFile():
    with open('../data/s&p500.txt') as f:
        lines = f.read().splitlines()
        for symbol in lines: 
#             updateSymbolAddNewProperty(symbol, 'sp500', True)
            symbolObj = findSymbol(symbol)
            if (symbolObj is None):
                print(symbol)


if __name__ == '__main__':
    loadsp500FromFile()