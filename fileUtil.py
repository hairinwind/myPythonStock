import os

QUOTES_DIR = "d:/quotes/"
QUOTES_ERROR_DIR = QUOTES_DIR + "error/"
QUOTES_SUCCESS_DIR = QUOTES_DIR + "success/"

# parse file name ATI_2017-06-12_2017-06-12.csv, returns ATI 2017-06-12 2017-06-12
def parse(csvFile):
    return csvFile.split('.')[0].split('_')
    
def saveQuotesToCsv(symbol, quotes, start, end):
    if (quotes is not None):
        quotes.to_csv(QUOTES_DIR+'{}_{}_{}.csv'.format(symbol, start, end))
    else:
        print(symbol, 'quotes is None')

def removeErrorCsv(csvFile):
    os.remove(QUOTES_ERROR_DIR+csvFile)