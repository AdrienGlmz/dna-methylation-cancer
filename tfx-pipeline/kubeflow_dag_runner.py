"""Define KubeflowDagRunner to run the pipeline using Kubeflow."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from absl import logging

from pipeline import configs
from pipeline import pipeline
from tfx.orchestration.kubeflow import kubeflow_dag_runner
from tfx.proto import trainer_pb2
from tfx.utils import telemetry_utils

# TFX pipeline produces many output files and metadata. All output data will be
# stored under this OUTPUT_DIR.
OUTPUT_DIR = os.path.join('gs://', configs.GCS_BUCKET_NAME)

# TFX produces two types of outputs, files and metadata.
# - Files will be created under PIPELINE_ROOT directory.
PIPELINE_ROOT = os.path.join(OUTPUT_DIR, 'tfx_pipeline_output',
                             configs.PIPELINE_NAME)

# The last component of the pipeline, "Pusher" will produce serving model under
# SERVING_MODEL_DIR.
SERVING_MODEL_DIR = os.path.join(PIPELINE_ROOT, 'serving_model')

# Specifies data file directory. DATA_PATH should be a directory containing CSV
# files for CsvExampleGen in this example. By default, data files are in the
# GCS path: `gs://{GCS_BUCKET_NAME}/tfx-template/data/`. Using a GCS path is
# recommended for KFP.
#
# One can optionally choose to use a data source located inside of the container
# built by the template, by specifying
# DATA_PATH = 'data'. Note that Dataflow does not support use container as a
# dependency currently, so this means CsvExampleGen cannot be used with Dataflow
# (step 8 in the template notebook).

DATA_PATH = 'gs://{}/tfx-template/data/'.format(configs.GCS_BUCKET_NAME)


def run():
    """Define a kubeflow pipeline."""

    # Metadata config. The defaults works work with the installation of
    # KF Pipelines using Kubeflow. If installing KF Pipelines using the
    # lightweight deployment option, you may need to override the defaults.
    # If you use Kubeflow, metadata will be written to MySQL database inside
    # Kubeflow cluster.
    metadata_config = kubeflow_dag_runner.get_default_kubeflow_metadata_config()

    # This pipeline automatically injects the Kubeflow TFX image if the
    # environment variable 'KUBEFLOW_TFX_IMAGE' is defined. Currently, the tfx
    # cli tool exports the environment variable to pass to the pipelines.
    # TODO(b/157598477) Find a better way to pass parameters from CLI handler to
    # pipeline DSL file, instead of using environment vars.
    # tfx_image = os.environ.get('KUBEFLOW_TFX_IMAGE', None)
    tfx_image = 'gcr.io/gcp-nyc/tfx-pipeline'

    runner_config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
        kubeflow_metadata_config=metadata_config, tfx_image=tfx_image)
    pod_labels = kubeflow_dag_runner.get_default_pod_labels()
    pod_labels.update({telemetry_utils.LABEL_KFP_SDK_ENV: 'tfx-template'})
    kubeflow_dag_runner.KubeflowDagRunner(
        config=runner_config, pod_labels_to_attach=pod_labels
    ).run(
        pipeline.create_pipeline(
            pipeline_name=configs.PIPELINE_NAME,
            pipeline_root=PIPELINE_ROOT,
            gcp_project=configs.GOOGLE_CLOUD_PROJECT,
            gcs_bucket=configs.GCS_BUCKET_NAME,
            tcga_betas_query=configs.TCGA_BETAS_QUERY,
            tcga_betas_output_schema=configs.TCGA_BETAS_OUTPUT_SCHEMA,
            tcga_betas_output_table_name=configs.TCGA_BETAS_OUTPUT_TABLE,
            cpg_sites_list_query=configs.CPG_SITES_LIST_QUERY,
            cpg_sites_list_output_schema=configs.CPG_SITES_OUTPUT_SCHEMA,
            cpg_sites_list_output_table_name=configs.CPG_SITES_OUTPUT_TABLE,
            pivot_query=configs.PIVOT_DATASET_QUERY,
            pivot_output_table=configs.PIVOT_OUTPUT_TABLE,
            final_dataset_query=configs.TRAIN_QUERY,
            preprocessing_fn=configs.PREPROCESSING_FN,
            run_fn=configs.RUN_FN,
            train_args=trainer_pb2.TrainArgs(num_steps=configs.TRAIN_NUM_STEPS),
            eval_args=trainer_pb2.EvalArgs(num_steps=configs.EVAL_NUM_STEPS),
            eval_accuracy_threshold=configs.EVAL_ACCURACY_THRESHOLD,
            serving_model_dir=SERVING_MODEL_DIR,
            beam_pipeline_args=configs.BIG_QUERY_WITH_DIRECT_RUNNER_BEAM_PIPELINE_ARGS,
            # TODO(step 8): (Optional) Uncomment below to use Dataflow.
            # beam_pipeline_args=configs.DATAFLOW_BEAM_PIPELINE_ARGS,
            # TODO(step 9): (Optional) Uncomment below to use Cloud AI Platform.
            # ai_platform_training_args=configs.GCP_AI_PLATFORM_TRAINING_ARGS,
            # TODO(step 9): (Optional) Uncomment below to use Cloud AI Platform.
            # ai_platform_serving_args=configs.GCP_AI_PLATFORM_SERVING_ARGS,
        ))


if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    run()
