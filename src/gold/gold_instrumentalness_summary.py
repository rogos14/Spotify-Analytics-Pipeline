from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    year,
    floor,
    count,
    round,
    when,
    avg
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

instrumental_bucket = tracks_gold_base.withColumn(
    "instrumentalness_bucket",
    when(
        col("instrumentalness") > 0.8,
        "Instrumental")
    .when(
	(col("instrumentalness") > 0.6) & (col("instrumentalness") <= 0.8),
        "Mostly Instrum.")
    .when(
	(col("instrumentalness") > 0.4) & (col("instrumentalness") <= 0.6 ),
        "Balanced")
    .when(
	(col("instrumentalness") > 0.2) & (col("instrumentalness") <= 0.4),
        "Slightly Instrum.")
    .otherwise("Mostly Vocal"))

instrumental_summary = (
    instrumental_bucket
    .groupBy("decade",
             "instrumentalness_bucket")
    .agg(
        count("*").alias("total_tracks"),
        round(avg("popularity"), 2).alias("avg_popularity"),
        round(avg("energy"), 2).alias("avg_energy"),
        round(avg("valence"), 2).alias("avg_valence"),
        round(avg("instrumentalness"), 2).alias("avg_instrumentalness"),
        round(avg("danceability"), 2).alias("avg_danceability"),
        round(avg("duration_minutes"), 2).alias("avg_duration_minutes")
    )
)

instrumental_summary = (
    instrumental_summary
    .withColumn(
        "instrumental_order",
        when(col("instrumentalness_bucket") == "Instrumental", 5)
        .when(col("instrumentalness_bucket") == "Mostly Instrumental", 4)
        .when(col("instrumentalness_bucket") == "Balanced", 3)
        .when(col("instrumentalness_bucket") == "Slightly Instrumental", 2)
        .otherwise(1)
    )
    .orderBy(
        "decade",
        "instrumental_order")
    .drop("instrumental_order")
)

#instrumental_summary.show()

instrumental_summary.write.mode("overwrite").parquet(
    "data/gold/intrumental_summary.parquet"
)
print("Gold instrumentalness summary successfully created.")

spark.stop()