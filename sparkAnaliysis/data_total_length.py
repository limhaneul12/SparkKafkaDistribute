import os
import pandas as pd


data_directory = f"{os.getcwd()}/data"
filenames = sorted(
    [f"{data_directory}/{filename}" for filename in os.listdir(data_directory)]
)

data_total_length: int = sum(pd.read_parquet(data).__len__() for data in filenames)

print(f"총 데이터의 크기는 다음과 같습니다 --> {data_total_length}")
