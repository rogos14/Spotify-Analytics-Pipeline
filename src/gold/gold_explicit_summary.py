from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    year,
    floor,
    count,
    sum,
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

explicit_summary = (
    tracks_gold_base
    .groupBy("decade")
    .agg(
        count("*").alias("total_tracks"),
        sum("explicit").alias("total_explicit")
        )
    .orderBy("decade")
)

explicit_percentage = explicit_summary.withColumn( 
    "explicit_percentage", 
    round(
        (col("total_explicit")/col("total_tracks")) *100,
        2 
    )
)

# explicit_percentage.show(truncate=False)

explicit_percentage.write.mode("overwrite").parquet(
    "data/gold/explicit_summary.parquet"
)

print("Gold explicit summary successfully created.")

spark.stop