import pandas as pd 
import os 
import sys
import pyarrow

path = (f"{os.getcwd()}/data/{i}" for i in range(1, len(os.listdir(f"{os.getcwd()}/data"))))

d = 0
for p in path: 
    for data in os.listdir(p):
        l = f"{p}/{data}"
        f = pd.read_parquet(l)
        d += f.shape[0]
        print(l ,f.shape[0])
print(f"총 데이터 개수 --> {d}")

