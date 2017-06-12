from stockMongo import insertQuotes
from stockQuote import convertToJson 
import pandas as pd
import os
import shutil

def moveFileToErrorFolder(file):
    shutil.move("quotes/{}".format(file), "quotes/errors/")

files = os.listdir("quotes")
for file in files:
    try: 
        quotes = pd.DataFrame.from_csv("quotes/{}".format(file))
        print(quotes['Symbol'][0])
        quotesJson = convertToJson(quotes)
        insertQuotes(quotesJson)
        print(len(quotes), ' quotes were inserted...')
    except Exception as e: 
        moveFileToErrorFolder(file)
        continue
