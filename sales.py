from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType, DateType, IntegerType
import time

DAYS_DIFF = 723
BUCKET_NAME = 'elevated-dynamo-bucket'
PROJECT_ID = 'elevated-dynamo-370709'
DATASET_ID = 'dataproc'

spark_session = SparkSession.builder \
    .appName("JSONtoBigQuery") \
    .getOrCreate()

schema = StructType([
    StructField("Invoice ID",StringType()),
    StructField("Branch",StringType()),
    StructField("City",StringType()),
    StructField("Customer type",StringType()),
    StructField("Gender",StringType()),
    StructField("Product line",StringType()),
    StructField("Unit price",IntegerType()),
    StructField("Quantity",IntegerType()),
    StructField("Tax 5%",IntegerType()),
    StructField("Total",IntegerType()),
    StructField("Date",DateType()),
    StructField("Time",StringType()),
    StructField("Payment",StringType()),
    StructField("cogs",IntegerType()),
    StructField("gross margin percentage",IntegerType()),
    StructField("gross income",IntegerType()),
    StructField("Rating",IntegerType()),
])


start = time.time()
df_with_schema = spark_session.read.format("csv") \
    .load(f"gs://elevated-dynamo-bucket/supermarket_sales - Sheet1.csv")
print(f"Read data execution time without specifying schema: {time.time() - start} seconds")

df_with_schema.write.format('bigquery') \
    .option('table', 'elevated-dynamo-370709.dataproc.sales' ) \
    .save()

spark_session.stop()