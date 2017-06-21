import pandas as pd

def weaveInNextTxDayData(df):
    if len(df) < 2:
        return df
    df = df.sort_values(['Date'])
    df['nextClose'] = df['Close'].shift(-1)
    df = df.apply(pd.to_numeric, errors='ignore')
    df['nextClosePercentage'] = (df['nextClose']-df['Close'])/df['Close']
    return df