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

tracks_bucket = tracks_gold_base.withColumn(
    "popularity_bucket",
    when(
        col("popularity")>90,
        "Top Hits")
    .when(
	(col("popularity") > 80) & (col("popularity") <= 90),
        "Hits")
    .when(
	(col("popularity") > 60) & (col("popularity") <=80 ),
        "Very High")
    .when(
	(col("popularity") > 40) & (col("popularity") <= 60),
        "High")
    .when(
	(col("popularity") > 20) & (col("popularity") <= 40),
        "Medium")
    .otherwise("Low"))

popularity_summary = (
    tracks_bucket
    .groupBy("popularity_bucket")
    .agg(
        count("*").alias("total_tracks"),
        round(avg("danceability"), 2).alias("avg_danceability"),
        round(avg("energy"), 2).alias("avg_energy"),
        round(avg("tempo"), 2).alias("avg_tempo"),
        round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        round(avg("valence"), 2).alias("avg_valence"), 
        round(avg("instrumentalness"), 3).alias("avg_instrumentalness")
        )
)

popularity_summary = (
    popularity_summary
    .withColumn(
        "bucket_order",
        when(col("popularity_bucket") == "Top Hits", 1)
        .when(col("popularity_bucket") == "Hits", 2)
        .when(col("popularity_bucket") == "Very High", 3)
        .when(col("popularity_bucket") == "High", 4)
        .when(col("popularity_bucket") == "Medium", 5)
        .otherwise(6)
    )
    .orderBy("bucket_order")
    .drop("bucket_order")
)

# popularity_summary.show()

popularity_summary.write.mode("overwrite").parquet(
    "data/gold/popularity_summary.parquet"
)

print("Gold popularity summary successfully created.")

spark.stop()