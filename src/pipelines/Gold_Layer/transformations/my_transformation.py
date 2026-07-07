from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# Read all Silver tables
# ============================================================

silver_main = spark.read.table("vstone.silver.flight_silver")

silver_incremental = spark.read.table(
    "vstone.silver.flight_csv_incremental_silver"
)

silver_json = spark.read.table(
    "vstone.silver.flight_json_silver"
)

silver_xml = spark.read.table(
    "vstone.silver.flight_xml_silver"
)

# ============================================================
# Union all Silver datasets
# ============================================================

silver_all = (
    silver_main
    .unionByName(silver_incremental, allowMissingColumns=True)
    .unionByName(silver_json, allowMissingColumns=True)
    .unionByName(silver_xml, allowMissingColumns=True)
)

# ============================================================
# Remove duplicate records
# ============================================================

silver_all = silver_all.dropDuplicates()

# ============================================================
# Add standard audit columns
# ============================================================

silver_all = (
    silver_all
    .withColumn(
        "gold_load_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "gold_source",
        F.lit("Lakeflow DLT")
    )
)





@dp.table(
    name="staging_flights",
    comment="Unified Silver dataset used for Gold layer"
)
def staging_flights():

    return (
        silver_all
        .filter(
            F.col("flightdate").isNotNull()
        )
    )



# ============================================================
# GOLD DIMENSION : AIRLINE
# ============================================================

@dp.table(
    name="dim_airline",
    comment="Gold Airline Dimension"
)
def dim_airline():

    df = spark.read.table("vstone.gold.staging_flights")

    airline_dim = (
        df
        .select("airline")
        .filter(F.col("airline").isNotNull())
        .dropDuplicates()
        .orderBy("airline")
    )

    # Generate surrogate key
    airline_dim = (
        airline_dim
        .withColumn(
            "airline_sk",
            F.row_number().over(
                Window.orderBy("airline")
            )
        )
    )

    # Audit columns
    airline_dim = (
        airline_dim
        .withColumn(
            "load_dt",
            F.current_timestamp()
        )
        .withColumn(
            "source",
            F.lit("Lakeflow DLT")
        )
    )

    # SCD Type 2 columns
    airline_dim = (
        airline_dim
        .withColumn(
            "__start_at",
            F.current_timestamp()
        )
        .withColumn(
            "__end_at",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "__is_current",
            F.lit(True)
        )
    )

    return airline_dim.select(
        "airline_sk",
        "airline",
        "load_dt",
        "source",
        "__start_at",
        "__end_at",
        "__is_current"
    )



@dp.table(
    name="dim_airport",
    comment="Airport Dimension - SCD2 Ready"
)
    
def dim_airport():

    df = spark.read.table("vstone.gold.staging_flights")
    origin = (
        df.select(
            F.col("origin").alias("airport_code"),
            F.col("origincityname").alias("city_name"),
            F.col("originstate").alias("state_code"),
            F.col("originstatename").alias("state_name"),
            "load_dt",
            "source"
        )
    )

    dest = (
        df.select(
            F.col("dest").alias("airport_code"),
            F.col("destcityname").alias("city_name"),
            F.col("deststate").alias("state_code"),
            F.col("deststatename").alias("state_name"),
            "load_dt",
            "source"
        )
    )

    airport_dim = (
        origin.unionByName(dest)
        .dropDuplicates(["airport_code"])
    )

    window = Window.orderBy("airport_code")

    airport_dim = (
        airport_dim
        .withColumn(
            "airport_sk",
            F.row_number().over(window)
        )
        .withColumn(
            "__start_at",
            F.current_timestamp()
        )
        .withColumn(
            "__end_at",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "__is_current",
            F.lit(True)
        )
    )

    return airport_dim.select(
        "airport_sk",
        "airport_code",
        "city_name",
        "state_code",
        "state_name",
        "load_dt",
        "source",
        "__start_at",
        "__end_at",
        "__is_current"
    )


# ============================================================
# GOLD FACT TABLE : FLIGHT DELAYS
# ============================================================

@dp.table(
    name="fact_flight_delays",
    comment="Gold Fact Table"
)
def fact_flight_delays():

    # Read DLT tables
    flights = dp.read("staging_flights")

    airline = (
        dp.read("dim_airline")
        .select(
            "airline",
            "airline_sk"
        )
    )

    airport = (
        dp.read("dim_airport")
        .select(
            "airport_code",
            "airport_sk"
        )
    )

    # ============================================================
    # Lookup tables
    # ============================================================

    origin_lookup = (
        airport.select(
            F.col("airport_code").alias("origin"),
            F.col("airport_sk").alias("origin_airport_sk")
        )
    )

    dest_lookup = (
        airport.select(
            F.col("airport_code").alias("dest"),
            F.col("airport_sk").alias("dest_airport_sk")
        )
    )

    # ============================================================
    # Join Dimensions
    # ============================================================

    fact = (
        flights
        .join(
            airline,
            on="airline",
            how="left"
        )
        .join(
            origin_lookup,
            on="origin",
            how="left"
        )
        .join(
            dest_lookup,
            on="dest",
            how="left"
        )
    )

    # ============================================================
    # Generate Surrogate Key
    # ============================================================

    window = Window.orderBy(
        "flightdate",
        "airline",
        "origin",
        "dest"
    )

    fact = (
        fact
        .withColumn(
            "flight_sk",
            F.row_number().over(window)
        )
        .withColumn(
            "flight_date_key",
            F.date_format(
                "flightdate",
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "load_dt",
            F.current_timestamp()
        )
        .withColumn(
            "source",
            F.lit("Lakeflow DLT")
        )
    )

    # ============================================================
    # Final Fact Table
    # ============================================================

    return fact.select(

        # Primary Key
        "flight_sk",

        # Foreign Keys
        "airline_sk",
        "origin_airport_sk",
        "dest_airport_sk",

        # Date
        "flight_date_key",
        "flightdate",

        # Measures
        "depdelayminutes",
        "arrdelayminutes",
        "airtime",
        "actualelapsedtime",
        "distance",

        # Flags
        "cancelled",
        "diverted",

        # Audit
        "load_dt",
        "source"
    )


# ============================================================
# AGGREGATE : AIRLINE DELAY SUMMARY
# ============================================================

@dp.table(
    name="airline_delay_summary",
    comment="Gold Aggregate - Airline Performance Summary"
)
def airline_delay_summary():

    fact = dp.read("fact_flight_delays")

    dim_airline = (
        dp.read("dim_airline")
        .select("airline_sk", "airline")
    )

    return (
        fact
        .join(dim_airline, "airline_sk", "left")
        .groupBy(
            "airline_sk",
            "airline"
        )
        .agg(
            F.count("*").alias("total_flights"),

            F.avg("depdelayminutes").alias("avg_departure_delay"),

            F.avg("arrdelayminutes").alias("avg_arrival_delay"),

            F.sum(
                F.when(F.col("cancelled"), 1).otherwise(0)
            ).alias("cancelled_flights"),

            F.avg("distance").alias("avg_distance")
        )
        .orderBy("airline")
    )


# ============================================================
# AGGREGATE : AIRPORT DELAY SUMMARY
# ============================================================

@dp.table(
    name="airport_delay_summary",
    comment="Gold Aggregate - Airport Performance Summary"
)
def airport_delay_summary():

    fact = dp.read("fact_flight_delays")

    dim_airport = (
        dp.read("dim_airport")
        .select("airport_sk", "airport_code", "city_name", "state_name")
    )

    return (
        fact
        .join(
            dim_airport,
            fact.origin_airport_sk == dim_airport.airport_sk,
            "left"
        )
        .groupBy(
            "airport_sk",
            "airport_code",
            "city_name",
            "state_name"
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.avg("depdelayminutes").alias("avg_departure_delay"),
            F.avg("arrdelayminutes").alias("avg_arrival_delay"),
            F.avg("distance").alias("avg_distance")
        )
        .orderBy("airport_code")
    )



# ============================================================
# AGGREGATE : MONTHLY DELAY SUMMARY
# ============================================================

@dp.table(
    name="monthly_delay_summary",
    comment="Gold Aggregate - Monthly Delay Summary"
)
def monthly_delay_summary():

    fact = dp.read("fact_flight_delays")

    return (
        fact
        .groupBy(
            F.year("flightdate").alias("year"),
            F.month("flightdate").alias("month")
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.avg("depdelayminutes").alias("avg_departure_delay"),
            F.avg("arrdelayminutes").alias("avg_arrival_delay"),
            F.sum(
                F.when(F.col("cancelled"),1).otherwise(0)
            ).alias("cancelled_flights")
        )
        .orderBy("year","month")
    )



# ============================================================
# AGGREGATE : ROUTE PERFORMANCE SUMMARY
# ============================================================

@dp.table(
    name="route_delay_summary",
    comment="Gold Aggregate - Route Performance"
)
def route_delay_summary():

    flights = dp.read("staging_flights")

    return (
        flights
        .groupBy(
            "origin",
            "dest"
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.avg("depdelayminutes").alias("avg_departure_delay"),
            F.avg("arrdelayminutes").alias("avg_arrival_delay"),
            F.avg("distance").alias("avg_distance")
        )
        .orderBy(
            "origin",
            "dest"
        )
    )


# ============================================================
# AGGREGATE : CANCELLATION SUMMARY
# ============================================================

@dp.table(
    name="cancellation_summary",
    comment="Gold Aggregate - Cancellation Summary"
)
def cancellation_summary():

    fact = dp.read("fact_flight_delays")

    dim_airline = (
        dp.read("dim_airline")
        .select("airline_sk","airline")
    )

    return (
        fact
        .join(
            dim_airline,
            "airline_sk",
            "left"
        )
        .groupBy(
            "airline_sk",
            "airline"
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.sum(
                F.when(F.col("cancelled"),1).otherwise(0)
            ).alias("cancelled_flights")
        )
        .withColumn(
            "cancellation_rate",
            F.round(
                F.col("cancelled_flights")*100/F.col("total_flights"),
                2
            )
        )
        .orderBy("airline")
    )