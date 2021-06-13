# Batch Processing with Spark + Dataproc
This project is much utilizing Google Cloud Platform specifically:
1. **Dataproc** as the managed cluster where we can submit our PySpark code as a job to the cluster.
2. **Google BigQuery** as our Data Warehouse to store final data after transformed by PySpark
3. **Google Cloud Storage** to store the data source, our PySpark code and to store the output besides BigQuery

## **Data Sources and Output Target**
This project is taking **JSON** as the data sources that you may see on `input/` folder. 

Those files will be read by Spark as Spark DataFrame and write the transformed data into:
1. CSV
2. Parquet
3. JSON
4. BigQuery Table

Which explained more in [Output & Explanation](#output-and-explanation) section

## **Setup**
First of all, you need to have `gcloud` command whether its on Local on you can use Cloud Shell instead.
1. Enable Dataproc API, you may see it how to enable the API on: https://cloud.google.com/dataproc/docs/quickstarts/quickstart-gcloud
2. If you decide to run whole process on local using `gcloud`. You may refer to https://cloud.google.com/sdk/docs/install for the installation guide.
   
If you've did two steps above, lets move to next part which is [Setup using Non Workflow Template](#using-non-workflow-template) vs [Using Workflow Template](#using-workflow-template).
### Using Non Workflow Template
Non workflow template means that we have to create the cluster first, then submit jobs to our cluster then delete the cluster by manually (but still can be done using `gcloud` command). The steps are:
1. Create a Dataproc cluster with:
   ```
   gcloud beta dataproc clusters create ${CLUSTER_NAME} \
      --region=${REGION} \
      --zone=${ZONE} \
      --single-node \
      --master-machine-type=n1-standard-2 \
      --bucket=${BUCKET_NAME} \
      --image-version=1.5-ubuntu18 \
      --optional-components=ANACONDA,JUPYTER \
      --enable-component-gateway \
      --metadata 'PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
   ```
   Note that you need to declare some variables that mentioned with `${}` first. And you can add or remove the arguments based on your needs which you can see more on: https://cloud.google.com/sdk/gcloud/reference/beta/container/clusters/create
2. Submit your job, in this case I submitted PySpark job
   ```
   gcloud beta dataproc jobs submit pyspark gs://${WORKFLOW_BUCKET_NAME}/jobs/spark_etl_job.py \
      --cluster=${CLUSTER_NAME} \
      --region=${REGION} \
      --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
   ```
   If you wish to submit other than PySpark, please refer here: https://cloud.google.com/sdk/gcloud/reference/beta/dataproc/jobs/submit
3. After the job submitted and finished running. You can delete the cluster to prevent getting charged. Although in real case at company we let the clusters always ON and live. However if you want to delete, you can do it by:
   ```
   gcloud beta dataproc clusters delete ${CLUSTER_NAME} \
      --region=${REGION}
   ```
### Using Workflow Template
1. Run this command to create workflow-template 
      ```
   gcloud beta dataproc workflow-templates create ${TEMPLATE} \
    --region=${REGION}
      ``` 
      For `TEMPLATE` variable you can set it to any string, and `REGION` you may see more on here: https://cloud.google.com/compute/docs/regions-zones

2. Set managed cluster on created workflow-template by run this command:
    ```
    gcloud beta dataproc workflow-templates set-managed-cluster ${TEMPLATE} \
      --region=${REGION} \
      --bucket=${WORKFLOW_BUCKET_NAME} \
      --zone=${ZONE} \
      --cluster-name="bash-wf-template-pyspark-cluster" \
      --single-node \
      --master-machine-type=n1-standard-2 \
      --image-version=1.5-ubuntu18
      ```
    Actually you can add more arguments or change the value of argument based on your needs. Refer more on: https://cloud.google.com/sdk/gcloud/reference/dataproc/workflow-templates/set-managed-cluster
3. Add job on workflow-template by running this command
   ```
   gcloud beta dataproc workflow-templates add-job pyspark gs://${WORKFLOW_BUCKET_NAME}/jobs/your_job_name.py \
      --step-id="bash-pyspark-wf-template-gcs-to-bq" \
      --workflow-template=${TEMPLATE} \
      --region=${REGION} \
      --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
   ```
   The job is not limited to `pyspark` only, you can also add Hadoop MapReduce job or others. More on https://cloud.google.com/sdk/gcloud/reference/dataproc/workflow-templates/add-job
4. After the job added, its time to run the workflow-template by instantiate it with:
   ```
   gcloud beta dataproc workflow-templates instantiate ${TEMPLATE} \
    --region=${REGION}
   ```
If you want to run that `gcloud` commands just using single command, you may see those scripts on `script.sh` that I've created on this Repo.

## **Output and Explanation**
### **Output**
Here is the output from running Spark Job on Dataproc cluster:
#### Google Cloud Storage Flatfile
1. CSV<br>
   Partitioned by **date** will create folders to group the data by date **automatically**
   ![csv-date-dir](images/GCS%20CSV%20Output%20directory%20[1].png)
   And if you click one of the folder, it will show you files that *partitioned* by spark. You can adjust it by using `repartition(n)` in your Spark code when writing data to file.
   ![csv-file-in-a-date-dir](images/GCS%20CSV%20Output%20directory%20[2].png)
   Note: This partition is called *hive partitioning*. Reference: https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs#supported_data_layouts
2. JSON<br>
   It shows same result like CSV output
   ![json-date-dir](images/GCS%20JSON%20Output%20directory%20[1].png)
   ![json-file-in-a-date-dir](images/GCS%20JSON%20Output%20directory%20[2].png)
3. Parquet<br>
   It also create the same directory structure like CSV and JSON. It just differents on the file content and parquet file will be compressed using snappy by default.
   ![parquet-date-dir](images/GCS%20Parquet%20Output%20directory%20[1].png)
   ![parquet-file-in-a-date-dir](images/GCS%20Parquet%20Output%20directory%20[2].png)
   As this screenshot shows that the file has `snappy` before the `.parquet` file type.
   ![snappy](images/Parquet%20Compressed%20by%20Snappy.png)

#### BigQuery
   1. Flights data
   <br>The BigQuery Table output shows as below, this table is partitioned by `flight_date` column
   ![bigquery-flights-output](images/Partitioned%20Flights%20BigQuery%20Table.png)
   2. Aggregated Travels Destination on Each Day
   This table contains information about how many flights from source airport - destination airport for each day.
   ![agg-travels-day](images/Aggregated%20Flights%20Travel%20BigQuery.png)
   3. Aggregated Airline Codes on Each Day
   This table contains information about how many specific airline code on flights for each day.
   ![agg-airline-day](images/Aggregated%20Flights%20Airlines%20BigQuery.png)

### **Explanation**
When write data from Spark Dataframe to those file types, I used `repartition(n)` to set how many partitioning files on each partitioned by specified by `partitionBy(column)`. The more number specified on `repartition()` the less size on each file will be.

In `flights` BigQuery table, it shows "This is a partitioned table" meaning that when you filter query by that partitioned column it will return the results faster with less cost because it won't scan all rows but only the some data based on filtered value that you specified on `WHERE` clause. In Spark you can set the options of what type and column on partition with:
```
df.write.format('bigquery')
    .option('partitionField', column_name) \
    .option('partitionType', 'DAY')
```