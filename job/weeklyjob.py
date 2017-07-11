from machinelearning.machineLearningMode1 import initialMachineLearning
import base.stockMongo as stockMongo
import pandas as pd
import multiprocessing

def learn():
    symbols = pd.DataFrame(list(stockMongo.getAllActiveSymbols()))['Symbol'].values      
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(initialMachineLearning, symbols)

if __name__ == '__main__': 
    learn()