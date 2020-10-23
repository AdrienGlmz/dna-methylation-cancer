"""TFX Custom TCGAPreprocessing component definition."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Optional, Text, Any

from tfx import types
from tfx.components.example_gen import utils
from tfx.components.base import executor_spec

from tfx.extensions.tcga_preprocessing import executor
from tfx.components.example_gen import driver
from tfx.proto import example_gen_pb2

from tfx.types import standard_artifacts
from tfx.types.artifact import Property
from tfx.types.artifact import PropertyType
from tfx.types.component_spec import ChannelParameter
from tfx.types.component_spec import ExecutionParameter
from tfx.types.component_spec import ComponentSpec
from tfx.components.base import base_component
from tfx.types import channel_utils

import apache_beam as beam
import pickle
import google.auth
import google.cloud.storage as storage

# Span for an artifact.
SPAN_PROPERTY = Property(type=PropertyType.INT)
# Version for an artifact.
VERSION_PROPERTY = Property(type=PropertyType.INT)
# Version for an artifact.
SPLIT_NAMES_PROPERTY = Property(type=PropertyType.STRING)


class TCGAPreprocessingSpec(ComponentSpec):
    """TFX Custom TCGAPreprocessing component spec."""

    PARAMETERS = {
        'query':
            ExecutionParameter(type=Text),
        'output_schema':
            ExecutionParameter(type=Any),
        'table_name':
            ExecutionParameter(type=Text),
        'use_bigquery_source':
            ExecutionParameter(type=Any),

        # defaults args used by TFX
        'input_config':
            ExecutionParameter(type=example_gen_pb2.Input),
        'output_config':
            ExecutionParameter(type=example_gen_pb2.Output),
        'input_base':
            ExecutionParameter(type=(str, Text))
    }
    INPUTS = {}
    OUTPUTS = {
        'examples': ChannelParameter(type=standard_artifacts.Examples)
    }


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    credentials, project_id = google.auth.default()

    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def upload_beam_to_gcs(beam_pipeline, gcs_bucket):
    """Uploads a beam.PTransform object to a GCS bucket"""
    tmp_local_name = 'custom_beam_pipeline.pkl'
    with open(tmp_local_name, 'wb') as f:
        pickle.dump(beam_pipeline, f)
    tmp_destination_blob_name = 'beam_pipeline/custom_beam_pipeline.pkl'

    upload_blob(gcs_bucket, tmp_local_name, tmp_destination_blob_name)
    uri = f"gs://{gcs_bucket}/{tmp_destination_blob_name}"
    return uri


class TCGAPreprocessing(base_component.BaseComponent):
    """Custom TFX FreewheelPreprocessing component.

    The FrewheelPreprocessing component takes a BigQuery query, executes a beam.PTransform
    transform and write the outputs into a BigQuery table.
    """
    SPEC_CLASS = TCGAPreprocessingSpec
    EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(executor.Executor)
    DRIVER_CLASS = driver.Driver

    def __init__(self,
                 query: Optional[Text] = None,
                 beam_transform: beam.PTransform = None,
                 bucket_name: Optional[Text] = None,
                 output_schema: Optional[Text] = None,
                 table_name: Optional[Text] = None,
                 use_bigquery_source: Optional[Any] = False,
                 input_config: Optional[example_gen_pb2.Input] = None,
                 output_config: Optional[example_gen_pb2.Output] = None,
                 example_artifacts: Optional[types.Channel] = None,
                 instance_name: Optional[Text] = None):
        """Constructs a BigQueryExampleGen component.

        Args:
            query: BigQuery sql string, query result will be treated as a single
                split, can be overwritten by input_config.
                input_config: An example_gen_pb2.Input instance with Split.pattern as
                BigQuery sql string. If set, it overwrites the 'query' arg, and allows
                different queries per split. If any field is provided as a
                RuntimeParameter, input_config should be constructed as a dict with the
                same field names as Input proto message.
            beam_transform: beam.PTransform pipeline. Will be used to processed data ingested
                by the BigQuery query.
            bucket_name: string containing a GCS bucket name. Will be used as a temporary storage
                space to read query and pickle file.
            table_name: string containing the BigQuery output table name.
            use_bigquery_source: Whether to use BigQuerySource instead of experimental
                `ReadFromBigQuery` PTransform (required by the BigQueryExampleGen executor)
            input_config: An example_gen_pb2.Input instance with Split.pattern as
                BigQuery sql string. If set, it overwrites the 'query' arg, and allows
                different queries per split. If any field is provided as a
                RuntimeParameter, input_config should be constructed as a dict with the
                same field names as Input proto message.
            output_config: An example_gen_pb2.Output instance, providing output
                    configuration. If unset, default splits will be 'train' and 'eval' with
                    size 2:1. If any field is provided as a RuntimeParameter,
                    input_config should be constructed as a dict with the same field names
                    as Output proto message.
            example_artifacts: Optional channel of 'ExamplesPath' for output train and
                    eval examples.
            instance_name: Optional unique instance name. Necessary if multiple
                    BigQueryExampleGen components are declared in the same pipeline.

        Raises:
            RuntimeError: Only one of query and input_config should be set.
        """

        # Configure inputs and outputs
        input_config = input_config or utils.make_default_input_config()
        output_config = output_config or utils.make_default_output_config(
            input_config)

        if not example_artifacts:
            example_artifacts = channel_utils.as_channel(
                [standard_artifacts.Examples()])

        # Upload Beam Transform to a GCS Bucket
        beam_transform_uri = upload_beam_to_gcs(beam_transform, bucket_name)

        spec = TCGAPreprocessingSpec(
            # custom parameters
            query=query,
            output_schema=output_schema,
            table_name=table_name,
            use_bigquery_source=use_bigquery_source,
            # default parameters
            input_config=input_config,
            output_config=output_config,
            input_base=beam_transform_uri,
            # outputs
            examples=example_artifacts
        )
        super(TCGAPreprocessing, self).__init__(spec=spec, instance_name=instance_name)
