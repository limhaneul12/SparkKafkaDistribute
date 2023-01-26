import pandas as pd 
import os 
import sys
import pyarrow

path = (f"{os.getcwd()}/data/{i}" for i in os.listdir(f"{os.getcwd()}/data"))

def test():
    d = 0
    for p in path: 
        sd = os.listdir(p)
        sd.sort()
        for data in sd:
            l = f"{p}/{data}"
            f = pd.read_parquet(l)
            d += f.shape[0]
            print(l ,f.shape[0])
    print(f"총 데이터 개수 --> {d}")



location = "/Users/imhaneul/Documents/spark-kafka-distribute/sparkAnaliysis/data/2020/fhvhv_tripdata_2020-01.parquet"
data = pd.read_parquet(location)

print(data.columns)
