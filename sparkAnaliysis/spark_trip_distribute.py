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
        td = [spark.sql(qs).toPandas()]
        t_pickup_dat.append(td)
    return t_pickup_dat


def visualization() -> None:
    fig, ax = plt.subplots(3, 4, figsize=(30, 10))
    plt.subplots_adjust(wspace = 0.5, hspace = 0.6)
    fig.set_size_inches((80, 20))

    for tf_p in (tf for data in year_data() for tf in data):
        for low in range(len(ax)):
                for row in (j for n in ax for j in range(len(n))):   
                    sns.barplot(x="pickup", y="time", data=tf_p, ax=ax[low][row])
                    ax[low][row].tick_params(rotation=45)
                    ax[low][row].set_title(f"taxi data")
    plt.show()



visualization()