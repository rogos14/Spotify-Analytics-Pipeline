from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    year,
    floor,
    count,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("Gold Decade Summary")
    .getOrCreate()
)

tracks_df = spark.read.parquet(
    "data/silver/tracks_clean.parquet"
)

tracks_gold_base = (
    tracks_df
    .withColumn(
        "release_year",
        year(col("release_date"))
    )
    .withColumn(
        "decade",
        floor(col("release_year") / 10) * 10
    )
    .withColumn(
        "duration_minutes",
        col("duration_ms") / 60000
    )
    .filter(
        col("release_year") >= 1920
    )
)

decade_summary = (
    tracks_gold_base
    .groupBy("decade")
    .agg(
        count("*").alias("total_tracks"),
        round(avg("popularity"), 2).alias("average_popularity")
    )
    .orderBy("decade")
)

#decade_summary.show()

decade_summary.write.mode("overwrite").parquet(
    "data/gold/decade_summary.parquet"
)

print("Gold decade summary successfully created.")
spark.stop()