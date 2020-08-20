# Dataproc_processing

This folder contains pySpark scripts that can be launched with a GCP Dataproc cluster.

For instance, `create_cpg_site_list.py` will output the list of CpG sites to use in order to create 
the training dataset 

## How to run

To run a file in this repo use the `gcloud dataproc jobs submit` from the GCP cli
([doc here](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/)).

For instance, to run `create_cpg_sites_list.py` use
```
gcloud dataproc jobs submit pyspark
   --cluster build-hackathon-nyc-cluster
   --region us-east1 create_cpg_sites_list.py
   --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar
```

It is possible to run those file locally provided you have the PySpark installed.