from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    when, 
    concat, 
    lit, 
    length, 
    to_date,
    min,
    max,
)

#Creating spark session
spark = (
    SparkSession.builder
    .appName("Spotify Silver Tracks")
    .getOrCreate()
)

#Read raw data
tracks_df = spark.read.csv(
    "data/raw/tracks.csv",
    header = True,
    inferSchema = True
)

#Remove corrupted rows
tracks_valid = tracks_df.filter(
    col("release_date").rlike(
        r"^\d{4}(-\d{2})?(-\d{2})?$"
    )
)

#Standarizing date formats
tracks_silver = tracks_valid.withColumn(
    "release_date_clean",
    when(
        length(col("release_date"))==4,
        concat(col("release_date"), lit("-01-01"))
    )
    .when(
        length(col("release_date"))==7,
        concat(col("release_date"), lit("-01"))
    )
    .otherwise(col("release_date"))
)

#Converting to date type
tracks_silver = tracks_silver.drop(
    "release_date_clean"
)

tracks_clean = (
    tracks_silver
    .withColumn("popularity", col("popularity").cast("int"))
    .withColumn("duration_ms", col("duration_ms").cast("int"))
    .withColumn("danceability", col("danceability").cast("double"))
    .withColumn("energy", col("energy").cast("double"))
    .withColumn("key", col("key").cast("int"))
    .withColumn("loudness", col("loudness").cast("double"))
    .withColumn("mode", col("mode").cast("int"))
    .withColumn("speechiness", col("speechiness").cast("double"))
    .withColumn("acousticness", col("acousticness").cast("double"))
    .withColumn("instrumentalness", col("instrumentalness").cast("double"))
    .withColumn("liveness", col("liveness").cast("double"))
    .withColumn("valence", col("valence").cast("double"))
    .withColumn("tempo", col("tempo").cast("double"))
    .withColumn("time_signature", col("time_signature").cast("int"))
)

#Validating data types
tracks_clean.printSchema()

print(f"Rows count: {tracks_clean.count()}")

tracks_clean.select(
    min("release_date"),
    max("release_date")
).show()

tracks_clean.select("release_date").show(10, truncate=False)

tracks_clean.write.mode("overwrite").parquet(
    "data/silver/tracks_clean.parquet"
)

print("Tracks silver layer saved successfully")
spark.stop()
