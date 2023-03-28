import datetime

from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

clean_airports_df = spark.read.parquet("../target/clean_airports")
clean_carriers_df = spark.read.parquet("../target/clean_carriers")
cleaned_flights_df = spark.read.parquet("../target/cleaned_flights")

clean_airports_df.printSchema()
clean_carriers_df.printSchema()
cleaned_flights_df.printSchema()

clean_airports_df.show(truncate=False)
clean_carriers_df.show(truncate=False)

aa = clean_carriers_df.filter(clean_carriers_df.CARRIER == 'AA')
aa.show(truncate=False)

flights_of_aa = (
    cleaned_flights_df
        .join(clean_carriers_df, on=cleaned_flights_df.unique_carrier == clean_carriers_df.CARRIER)
        
)
flights_of_aa.show(truncate=False)