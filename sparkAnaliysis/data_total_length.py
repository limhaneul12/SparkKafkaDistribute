import os
import pandas as pd


base_path: str = os.path.join(os.getcwd(), "data")
filepaths: list[str] = [
    os.path.join(subdir, filename)
    for subdir, _, filenames in os.walk(base_path)
    for filename in filenames
]

data_total_length: int = sum(pd.read_parquet(data).__len__() for data in filepaths)

print(f"총 데이터의 크기는 다음과 같습니다 --> {data_total_length}")
