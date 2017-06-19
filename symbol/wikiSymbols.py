# to those quotes provided by quandl WIKI, update the quoteSource to be 'quandl/WIKI'

import csv
from base.stockMongo import updateSymbolQuoteSource

with open('data/WIKI-datasets-codes.csv') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        updateSymbolQuoteSource(row[0].split('/')[1], 'quandl/WIKI') 
