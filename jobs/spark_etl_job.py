from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, to_timestamp

DAYS_DIFF = 723
BUCKET_NAME = 'flight-sparks-w3'
PROJECT_ID = 'static-gravity-312212'
DATASET_ID = 'staging'

spark_session = SparkSession.builder \
    .appName("JSONtoBigQuery") \
    .getOrCreate()

df = spark_session.read.format("json") \
    .load("gs://flight-sparks-w3/input/2019-*.json")

df.printSchema()
df.show(n=50)

df_add_days = df.withColumn("flight_date", date_add(df.flight_date, DAYS_DIFF))
df_add_days.show()
df_flights = df_add_days.select(
    "id",
    "airline_code",
    "airtime",
    "arrival_delay",
    "arrival_time",
    "departure_delay",
    "departure_time",
    "source_airport",
    "destination_airport",
    "distance",
    "flight_date",
    to_timestamp(
        "flight_date",
        "yyyy-MM-dd").alias("flight_at"),
    "flight_num")
df_flights.show()
df_flights.printSchema()

gdf_airline = df_flights.groupBy("flight_date", "airline_code") \
    .count() \
    .orderBy("flight_date", "airline_code")

gdf_airline.show()

gdf_travel = df_flights.groupBy("flight_date", "source_airport", "destination_airport") \
    .count() \
    .orderBy("flight_date")

gdf_travel.show()

# Load Spark Dataframe to BigQuery
df_flights.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .option('partitionField', 'flight_at') \
    .option('partitionType', 'DAY') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.flights")

gdf_airline.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.count_flight_airlines_each_day")

gdf_travel.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.count_flight_travels_each_day")

# Write Spark Dataframe to another format
df_flights.write.mode('overwrite').partitionBy("flight_date") \
    .format('parquet') \
    .save(f'gs://{BUCKET_NAME}/output/flights.parquet')

df_flights.write.mode('overwrite').format('csv') \
    .option('header', True) \
    .save(f'gs://{BUCKET_NAME}/output/flights.csv')

spark_session.stop()
