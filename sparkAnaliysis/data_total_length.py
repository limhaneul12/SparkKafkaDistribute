import os
from pathlib import Path
import pandas as pd


def parquet_file_all() -> list[list[Path]]:
    def location(data: str) -> list[Path]:
        return sorted(Path(__file__).parent.joinpath(f"data/{data}").glob("*"))

    return [location(data) for data in os.listdir(f"{os.getcwd()}/data")]


# 데이터 크기를 계산할 변수를 초기화
total_data_size = 0

# parquet_file_all() 함수를 한 번 호출하여 결과를 저장
all_parquet_files = parquet_file_all()

# 모든 파일의 데이터 크기를 합산합니다.
for file_group in all_parquet_files:
    for file in file_group:
        total_data_size += pd.read_parquet(file).__len__()

# 결과를 출력합니다.
print(f"총 데이터의 크기는 다음과 같습니다 --> {total_data_size}")
