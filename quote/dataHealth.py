from base import stockMongo
import pandas as pd
import numpy as np

# this file is to eliminate bad data

if __name__ == '__main__':
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    for item in df['_id'].values:
        symbol = item['Symbol']
        date = item['Date']
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        
        # if nextClose or nextClosePercentage is null, put in the values from previous
        # nextCloseNotNull_df = quotes_df.loc[lambda df: pd.notnull(df.nextClose),:]
        # nextCloseNull_df = quotes_df.loc[lambda df: pd.isnull(df.nextClose)]
        for i in range(1, len(quotes_df)):
            if pd.isnull(quotes_df.loc[i, 'nextClose']) : 
                quotes_df.loc[i, 'nextClose'] = quotes_df.loc[i-1, 'nextClose']
                quotes_df.loc[i, 'nextClosePercentage'] = quotes_df.loc[i-1, 'nextClosePercentage']
                stockMongo.updateQuoteNextClose(quotes_df.loc[i, '_id'], quotes_df.loc[i-1, 'nextClose'], quotes_df.loc[i-1, 'nextClosePercentage'])
        
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        null_df = quotes_df.loc[lambda df: pd.isnull(df.Open) | pd.isnull(df.Close) | pd.isnull(df.Volume) | df.Open == 0,:]
                
        for id in null_df['_id'].values:
            stockMongo.deleteById(id)
            
            
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    for item in df['_id'].values:
        symbol = item['Symbol']
        date = item['Date']
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        for i in range(0, len(quotes_df)-1) :
            stockMongo.deleteById(quotes_df.loc[i,'_id'])
    
    # both have full data, delete the first one, keep the last one
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    print(df)    