from pyspark.sql import SparkSession, DataFrame
import seaborn as sns
import matplotlib.pyplot as plt
import os


MAX_MEMORY = "5g"
spark = (
    SparkSession.builder.master("local[*]")
    .appName("ml_taix")
    .config("spark.excutor.memory", MAX_MEMORY)
    .config("spark.driver.memory", MAX_MEMORY)
    .getOrCreate()
)


trip_data = f"{os.getcwd()}/data/2020"
trip_directory = spark.read.parquet(f"file:///{trip_data}/*")
trip_directory.createOrReplaceTempView("trips")


directory: str = [f"{os.getcwd()}/data/{i}" for i in os.listdir(f"{os.getcwd()}/data")][
    2
]
filename: list[str] = [f"{directory}/{data}" for data in os.listdir(directory)]
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


def year_read_data() -> list[DataFrame]:
    return [spark.read.parquet(f"file:///{data}") for data in filename]


def year_data() -> list:
    t_pickup_dat = []
    for data in year_read_data():
        data.createOrReplaceTempView("trips")
        td = spark.sql(qs).toPandas()
        t_pickup_dat.append(td)
    return t_pickup_dat


def visualization_shape(x: str, y: str, data: list) -> None:
    n_rows = 3
    n_cols = 4

    fig, ax = plt.subplots(n_rows, n_cols, figsize=(30, 10))
    plt.subplots_adjust(wspace=0.4, hspace=0.4)
    fig.set_size_inches((80, 20))

    for i, axi in enumerate(ax.flat):
        sns.barplot(x=x, y=y, data=data[i], ax=axi)
        axi.set_title(f"taxi 2020 --> date {i+1}")
        axi.set_xticklabels(axi.get_xticklabels(), rotation=30)

    # 데이터 플롯 출력
    plt.show()


visualization_shape(x="pickup", y="time", data=year_data())
