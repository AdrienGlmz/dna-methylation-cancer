import os
import logging
from datetime import date

from google.cloud import bigquery

from tfx.types.experimental.simple_artifacts import Dataset
from tfx.dsl.component.experimental.decorators import component
from tfx.dsl.component.experimental.annotations import OutputArtifact, Parameter


@component
def bigquery_query_component(project_id: Parameter[str],
                             query: Parameter[str],
                             table_name: Parameter[str],
                             transformed_data: OutputArtifact[Dataset]):
    client = bigquery.Client()
    today = str(date.today()).replace('-', '')

    dataset_name = f'{project_id}.dna_cancer_prediction'

    if client.get_dataset(dataset_name):
        dataset = bigquery.Dataset(dataset_name)
    else:
        dataset = bigquery.Dataset(dataset_name)
        client.create_dataset(dataset)

    # job_config = bigquery.QueryJobConfig()
    # job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
    # job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    # job_config.destination = table_name

    logging.info(f'Starting processing BQ query')

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete

    logging.info(f'Completed data preprocessing. Output in  {table_name}')

    # Write the location of the output table to metadata.
    transformed_data.set_string_custom_property('output_dataset', dataset_name)
    transformed_data.set_string_custom_property('output_table', table_name)
