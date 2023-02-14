from pyspark.sql import SparkSession
import os 
from typing import Generator, List


MAX_MEMORY = "5g"
spark = SparkSession.builder.master("local")\
                    .appName("ml_taix")\
                    .config("spark.excutor.memory", MAX_MEMORY)\
                    .config("spark.driver.memory", MAX_MEMORY).getOrCreate()
                    
                    
trip_data = f"{os.getcwd()}/data/2020"
trip_directory = spark.read.parquet(f"file:///{trip_data}/*")
trip_directory.createOrReplaceTempView("trips")


# qs = """
# SELECT
#     pickup_datetime,
#     PULocationID as pickup_location_id,
#     DOLocationID as drop_location_id,
#     tips,
#     driver_pay,
#     trip_miles,
#     trip_time
# FROM 
#     trips
# """
# spark.sql(qs).describe().show()


# qs = """
# SELECT 
#     pickup, 
#     count(*) as time
# FROM
#     (SELECT 
#         split(pickup_datetime, " ")[0] as pickup
#     FROM 
#         trips    
#     )
# GROUP BY 
#     pickup
# ORDER BY 
#     pickup
# """
# time_data = spark.sql(qs).toPandas()

import seaborn as sns 
import matplotlib.pyplot as plt 

# fig, ax = plt.subplots(figsize=(100, 9))
# sns.barplot(x="pickup", y="time", data=time_data)
# plt.xticks(rotation=45)
# plt.title("NYC Texi 2020-01-01 ~ 2021-01-01")
# plt.show()


directory: Generator[str, None, None] = [f"{os.getcwd()}/data/{i}" for i in os.listdir(f"{os.getcwd()}/data")][2]
filename: List[str] = [f"{directory}/{data}" for data in os.listdir(directory)]
filename.sort()


qs = """
SELECT 
    pickup, 
    count(*) as time
FROM
    (SELECT 
        split(pickup_datetime, " ")[0] as pickup
    FROM 
        trips    
    )
GROUP BY 
    pickup
ORDER BY 
    pickup
"""


def year_read_data() -> List:
    return [spark.read.parquet(f"file:///{data}") for data in filename]


def year_data() -> List:
    t_pickup_dat = []
    for data in year_read_data():
        data.createOrReplaceTempView("trips")
        td = spark.sql(qs).toPandas()
        t_pickup_dat.append(td)
    return t_pickup_dat


def visualization_shape(x: str, y:str, data: List) -> None:
    n_rows = 3
    n_cols = 4
    
    fig, ax = plt.subplots(n_rows, n_cols, figsize=(30, 10))
    plt.subplots_adjust(wspace = 0.4, hspace = 0.4)
    fig.set_size_inches((80, 20))

    for i, axi in enumerate(ax.flat):
        sns.barplot(x=x, y=y, data=data[i], ax=axi)
        axi.set_title(f'taxi 2020 --> date {i+1}')
        axi.set_xticklabels(axi.get_xticklabels(), rotation=30)

    # 데이터 플롯 출력
    plt.show()



visualization_shape(x="pickup", y="time", data=year_data())

