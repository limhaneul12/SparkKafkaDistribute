import pandas as pd 
import os 
import sys
import pyarrow

path = (f"{os.getcwd()}/data/{i}" for i in os.listdir(f"{os.getcwd()}/data"))

d = 0
for p in path: 
    sd = os.listdir(p)
    sd.sort()
    for data in sd:
        l = f"{p}/{data}"
        f = pd.read_parquet(l)
        print(f)


