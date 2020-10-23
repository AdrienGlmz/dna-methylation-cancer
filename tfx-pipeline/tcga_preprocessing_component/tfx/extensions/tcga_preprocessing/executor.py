"""TFX Custom TCGAPreprocessing executor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Any, Dict, Text, List

import apache_beam as beam
import pickle
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

from tfx import types
from tfx.components.base import base_executor
from tfx.utils import telemetry_utils
from tfx.components.example_gen import utils
from google.cloud import storage


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.Pipeline)
@beam.typehints.with_output_types(beam.typehints.Dict[Text, Any])
def _ReadFromBigQueryImpl(  # pylint: disable=invalid-name
        pipeline: beam.Pipeline,
        query: Text,
        use_bigquery_source: bool = False) -> beam.pvalue.PCollection:
    """Read from BigQuery.

  Args:
    pipeline: beam pipeline.
    query: a BigQuery sql string.
    use_bigquery_source: Whether to use BigQuerySource instead of experimental
      `ReadFromBigQuery` PTransform.

  Returns:
    PCollection of dict.
  """
    if use_bigquery_source:
        return (pipeline
                | 'ReadFromBigQuerySource' >> beam.io.Read(
                    beam.io.BigQuerySource(query=query, use_standard_sql=True)))

    return (pipeline
            | 'ReadFromBigQuery' >> ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                bigquery_job_labels=telemetry_utils.get_labels_dict()))


class Executor(base_executor.BaseExecutor):
    """TFX Custom TCGAPreprocessing executor."""

    def Do(
            self,
            input_dict: Dict[Text, List[types.Artifact]],
            output_dict: Dict[Text, List[types.Artifact]],
            exec_properties: Dict[Text, Any],
    ) -> None:
        """Executes:
          1. Read data from BigQuery
          2. Execute the beam.PTransform
          3. Output transformed table to BigQuery"""
        # Get execution parameters
        query = exec_properties['query']
        beam_transform_uri = exec_properties['input_base']
        output_schema = exec_properties['output_schema']
        table_name = exec_properties['table_name']
        use_bigquery_source = exec_properties['use_bigquery_source']

        # Pass table name in output artifact
        for output_examples_artifact in output_dict[utils.EXAMPLES_KEY]:
            output_examples_artifact.set_string_custom_property(
                'tcga_output_table_name',
                table_name)

        # Download beam.PTransform object from GCS
        tmp_download_file_name = 'custom_beam_pipeline_downloaded.pkl'
        bucket_name = beam_transform_uri.split('/')[2]
        blob_name = '/'.join(beam_transform_uri.split('/')[3:])

        download_blob(bucket_name, blob_name, tmp_download_file_name)
        with open(tmp_download_file_name, 'rb') as f:
            beam_transform = pickle.load(f)

        if beam_transform:
            # Execute pipeline
            with self._make_beam_pipeline() as pipeline:
                (pipeline
                 | 'QueryTable' >> _ReadFromBigQueryImpl(  # pylint: disable=no-value-for-parameter
                            query=query,
                            use_bigquery_source=use_bigquery_source)
                 | beam_transform
                 | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                            table_name,
                            schema=output_schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                 )
        else:
            with self._make_beam_pipeline() as pipeline:
                (pipeline
                 | 'QueryTable' >> _ReadFromBigQueryImpl(  # pylint: disable=no-value-for-parameter
                            query=query,
                            use_bigquery_source=use_bigquery_source)
                 | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                            table_name,
                            schema='SCHEMA_AUTODETECT',
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                 )