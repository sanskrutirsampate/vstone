from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Source folder
SOURCE_PATH = "/Volumes/vstone/bronze/raw_volume/csv_incremental"

@dp.table(
    name="flight_csv_incremental_bronze",
    comment="Incremental Bronze table using Lakeflow Declarative Pipelines"
)
def flight_csv_incremental_bronze():

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(SOURCE_PATH)
    )

    return (
        df
        .withColumn("load_dt", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )