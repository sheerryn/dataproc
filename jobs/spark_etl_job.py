from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType, DateType, IntegerType
import time

DAYS_DIFF = 723
BUCKET_NAME = 'flight-sparks-w3'
PROJECT_ID = 'static-gravity-312212'
DATASET_ID = 'staging'

spark_session = SparkSession.builder \
    .appName("JSONtoBigQuery") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("airline_code", StringType()),
    StructField("flight_num", IntegerType()),
    StructField("source_airport", StringType()),
    StructField("destination_airport", StringType()),
    StructField("distance", LongType()),
    StructField("flight_date", DateType()),
    StructField("departure_time", LongType()),
    StructField("arrival_time", LongType()),
    StructField("departure_delay", LongType()),
    StructField("arrival_delay", LongType()),
    StructField("airtime", LongType()),
])

start = time.time()
df_without_schema = spark_session.read.format("json") \
    .load(f"gs://{BUCKET_NAME}/input/2021-*.json")
print(f"Read data execution time without specifying schema: {time.time() - start} seconds")
df_without_schema.printSchema()

start = time.time()
df_with_schema = spark_session.read.format("json") \
    .load(f"gs://{BUCKET_NAME}/input/2021-*.json", schema=schema)
print(f"Read data execution time with schema: {time.time() - start} seconds")
df_with_schema.printSchema()

df_add_days = df_with_schema.withColumn("flight_date", date_add(df_with_schema.flight_date, DAYS_DIFF))
df_add_days.show()

start = time.time()
gdf_airline = df_add_days.groupBy("flight_date", "airline_code") \
    .count() \
    .orderBy("flight_date", "airline_code")

gdf_airline.show()

gdf_travel = df_add_days.groupBy("flight_date", "source_airport", "destination_airport") \
    .count() \
    .orderBy("flight_date")

gdf_travel.show()

# Load Spark Dataframe to BigQuery
start = time.time()
df_add_days.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .option('partitionField', 'flight_date') \
    .option('partitionType', 'DAY') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.flights")
print(f"Execution time write flights data to BigQuery: {time.time() - start} seconds")

start = time.time()
gdf_airline.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.count_flight_airlines_each_day")
print(f"Execution time write count_flight_airlines_each_day data to BigQuery: {time.time() - start} seconds")

start = time.time()
gdf_travel.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save(f"{PROJECT_ID}:{DATASET_ID}.count_flight_travels_each_day")
print(f"Execution time write count_flight_travels_each_day data to BigQuery: {time.time() - start} seconds")

# Write Spark Dataframe to another format
start = time.time()
df_add_days.repartition(1).write.mode('overwrite') \
    .partitionBy("flight_date") \
    .format('parquet') \
    .save(f'gs://{BUCKET_NAME}/output/flights.parquet')
print(f"Execution time write partitioned parquet: {time.time() - start} seconds")

start = time.time()
df_add_days.repartition(1).write.mode('overwrite') \
    .partitionBy("flight_date") \
    .format('csv') \
    .save(f'gs://{BUCKET_NAME}/output/flights.csv')
print(f"Execution time write partitioned csv data to BigQuery: {time.time() - start} seconds")

start = time.time()
df_add_days.repartition(1).write.mode('overwrite') \
    .partitionBy("flight_date") \
    .format('json') \
    .save(f'gs://{BUCKET_NAME}/output/flights.json')
print(f"Execution time write partitioned json data to BigQuery: {time.time() - start} seconds")

spark_session.stop()