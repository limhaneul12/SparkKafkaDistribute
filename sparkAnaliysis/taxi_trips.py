from pathlib import Path

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DateType


MAX_MEMORY = "5g"
spark = (
    SparkSession.builder.master("local[*]")
    .appName("TripAnaliysis")
    .config("spark.excutor.memory", MAX_MEMORY)
    .config("spark.driver.memory", MAX_MEMORY)
    .getOrCreate()
)


def spark_paquet_read(year: int) -> list[DataFrame]:
    def parquet_file_all() -> list[str]:
        return sorted(
            list(Path(__file__).parent.joinpath(f"data/{str(year)}").glob("*"))
        )

    return [spark.read.parquet(f"file:///{data}") for data in parquet_file_all()]


def datetime_groupby(data: DataFrame, name: str, agg_name: str) -> DataFrame:
    return (
        data.select(F.split(col(name), " ")[0].name("pickup"))
        .groupBy("pickup")
        .agg(F.count("*").name(agg_name))
    )


def datetime_miles_average(data: DataFrame) -> DataFrame:
    return (
        data.select(
            F.split(col("pickup_datetime"), " ")[0].name("pickup"), col("trip_miles")
        )
        .groupBy("pickup")
        .agg(
            F.count("pickup").name("pickup_total"),
            F.avg("trip_miles").name("average_miles"),
        )
    )


def to_null_date(value: Column) -> Column:
    return F.when(value.isNull(), F.lit("2019-02-20")).otherwise(value.cast(DateType()))


def process_data(data: DataFrame) -> None:
    request_groupby = datetime_groupby(
        data, "request_datetime", "request_count"
    ).withColumn("pickup", to_null_date(col("pickup")))

    trip_groupby = datetime_groupby(data, "pickup_datetime", "trip_count")
    drop_groupby = datetime_groupby(data, "dropoff_datetime", "drop_count")
    average_mile = datetime_miles_average(data)

    rtd_join: DataFrame = (
        trip_groupby.join(request_groupby, on="pickup")
        .join(drop_groupby, on="pickup")
        .join(average_mile, on="pickup")
    ).orderBy("pickup")

    week_day_rtd_join = (
        rtd_join.select(
            F.date_format(col("pickup"), "EEEE").alias("week"),
            col("pickup"),
            col("trip_count"),
            col("request_count"),
            col("drop_count"),
            col("average_miles"),
        )
    ).toPandas()

    week_day_rtd_join.to_csv(f"prepro/prepro_.csv", index=False)
