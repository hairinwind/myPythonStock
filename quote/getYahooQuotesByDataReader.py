from pandas_datareader import data as dreader

def getQuotesFromYahoo(symbol, start, end):
    symbols = [symbol]
    pnls = {i:dreader.DataReader(i, 'yahoo', start, end) for i in symbols}
#     print(pnls)
    return pnls.get(symbol)


if __name__ == '__main__':
    print(getQuotesFromYahoo('AMZN', '2017-11-20', '2017-11-20'))
