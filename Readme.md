## Batch Processing with Spark + Dataproc
This project is much utilizing Google Cloud Platform specifically:
1. Dataproc as the managed cluster where we can submit our PySpark code as a job to the cluster.
2. Google BigQuery as our Data Warehouse to store final data after transformed by PySpark
3. Google Cloud Storage to store the data source, our PySpark code and to store the output besides BigQuery