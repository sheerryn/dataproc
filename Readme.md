# Batch Processing with Spark + Dataproc
This project is much utilizing Google Cloud Platform specifically:
1. **Dataproc** as the managed cluster where we can submit our PySpark code as a job to the cluster.
2. **Google BigQuery** as our Data Warehouse to store final data after transformed by PySpark
3. **Google Cloud Storage** to store the data source, our PySpark code and to store the output besides BigQuery

## **Setup**
### Using Workflow Template
1. Install `gcloud` command if you insist to run bash command on local rather than on Cloud Shell. You may refer to https://cloud.google.com/sdk/docs/install for installation guide.
2. Run this command to create workflow-template 
      ```
   gcloud beta dataproc workflow-templates create ${TEMPLATE} \
    --region=${REGION}
      ``` 
      For `TEMPLATE` variable you can set it to any string, and `REGION` you may see more on here: https://cloud.google.com/compute/docs/regions-zones

3. Set managed cluster on created workflow-template by run this command:
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
4. Add job on workflow-template by running this command
   ```
   gcloud beta dataproc workflow-templates add-job pyspark gs://${WORKFLOW_BUCKET_NAME}/jobs/your_job_name.py \
      --step-id="bash-pyspark-wf-template-gcs-to-bq" \
      --workflow-template=${TEMPLATE} \
      --region=${REGION} \
      --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
   ```
   The job is not limited to `pyspark` only, you can also add Hadoop MapReduce job or others. More on https://cloud.google.com/sdk/gcloud/reference/dataproc/workflow-templates/add-job
5. After the job added, its time to run the workflow-template by instantiate it with:
   ```
   gcloud beta dataproc workflow-templates instantiate ${TEMPLATE} \
    --region=${REGION}
   ```
