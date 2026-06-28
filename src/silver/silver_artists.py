from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Creating spark session
spark = (
    SparkSession.builder
    .appName("Artists Validation")
    .getOrCreate()
)

#Read raw artists file
artists_df = spark.read.csv(
    "data/raw/artists.csv",
    header=True,
    inferSchema=True
)
print(f"Rows: {artists_df.count()}")

artists_valid = artists_df.filter(
    col("popularity").rlike("^[0-9]+$")
)

print(f"Raw number of rows: {artists_df.count()}")
print(f"Valid rows: {artists_valid.count()}")
print(f"Rows removed: {artists_df.count() - artists_valid.count()}")

artists_clean = (
    artists_valid
    .withColumn(
        "followers",
        col("popularity").cast("long")
    )
    .withColumn(
        "popularity",
        col("popularity").cast("int")
    )
)

artists_clean.printSchema()

artists_clean.write.mode("overwrite").parquet(
    "data/silver/artists_clean.parquet"
)

print("Artists silver layer saved successfully.")
spark.stop()
