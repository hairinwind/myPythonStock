import csv
from stockMongo import insertSymbols

def readCsv(csvfile):
    with open(csvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    return rows
    
rows = readCsv('./data/companylist.csv') 
rows.extend(readCsv('./data/companylist1.csv'))
rows.extend(readCsv('./data/companylist2.csv'))

for row in rows:
    del row['']
    print(row)

insertSymbols(rows)

print(len(rows), 'symbols were inserted...')