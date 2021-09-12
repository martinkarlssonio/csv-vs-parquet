"""
Written by Martin Karlsson
www.martinkarlsson.io

How more efficient is it to work with Parquet files compared to CSV?.
"""

import pandas as pd
import time
from pandasql import sqldf

epics = [
    'CS.D.NZDUSD.MINI.IP',
    'CS.D.CHFJPY.MINI.IP',
    'CS.D.USDCAD.MINI.IP',
    'CS.D.EURUSD.MINI.IP',
    'CS.D.GBPUSD.MINI.IP',
    'CS.D.AUDUSD.MINI.IP'
]

if __name__ == '__main__':
  
    # Create dataframes from CSV files
    startCsv = int(time.time())
    for epic in epics:
        stockDataDfCsv = pd.read_csv('output/stockDict{}.csv'.format(epic))
        newDfCsv = sqldf("SELECT * FROM stockDataDfCsv WHERE volume > '10'")
        newDfCsv.dropna()
        newDfCsv.to_csv('output/stockDictNew{}.csv'.format(epic),index=False)
    endCsv = int(time.time())
    
    # Create dataframes from Parquest files
    startParquet = int(time.time())
    for epic in epics:
        stockDataDfParquet = pd.read_parquet('output/stockDict{}.parquet'.format(epic))
        newDfParquet = sqldf("SELECT * FROM stockDataDfParquet WHERE volume > '10'")
        newDfParquet.dropna()
        newDfParquet.to_parquet('output/stockDictNew{}.parquet'.format(epic),index=False)
    endParquet = int(time.time())
    
    # Calculate the statistics
    totalTimeCsv = endCsv - startCsv
    totalTimeParquet = endParquet - startParquet
    diffExecutionTime = int(((totalTimeCsv-totalTimeParquet)/totalTimeCsv)*100)

    print(startCsv,endCsv,startParquet,endParquet)
    print("Using parquet files was {} percent lower in execution time!".format(diffExecutionTime))

    """
    Output:
    Using parquet files was 50 percent lower in execution time!
    """