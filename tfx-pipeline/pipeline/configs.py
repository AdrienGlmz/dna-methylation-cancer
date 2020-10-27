import os  # pylint: disable=unused-import

# Pipeline name will be used to identify this pipeline.
PIPELINE_NAME = 'dna-methylation-binary'

# GCP related configs.
GOOGLE_CLOUD_PROJECT = 'gcp-nyc'
GCS_BUCKET_NAME = GOOGLE_CLOUD_PROJECT + '-kubeflowpipelines-default'
GOOGLE_CLOUD_REGION = 'us-central1'

# TCGA Preprocessing config
TCGA_BETAS_OUTPUT_TABLE = 'dna_cancer_prediction.test_tcga_betas'

TCGA_BETAS_QUERY = """
WITH
  brca_betas AS (
  SELECT
    MIM.CpG_probe_id,
    dna.case_barcode,
    dna.aliquot_barcode,
    dna.probe_id,
    dna.beta_value,
    gc.Name
  FROM
    `isb-cgc.platform_reference.methylation_annotation` gc
  JOIN
    `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation` dna
  ON
    gc.Name = dna.probe_id
  JOIN
    `isb-cgc.platform_reference.GDC_hg38_methylation_annotation` MIM
  ON
    MIM.CpG_probe_id = gc.Name),

  participants_id AS (
  SELECT
    DISTINCT case_barcode
  FROM
    `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation`),

  participants_id_numbered AS (
  SELECT
    case_barcode,
    ROW_NUMBER() OVER (order by case_barcode) as row_number
  FROM
    participants_id)

SELECT
  A.case_barcode,
  A.beta_value,
  A.CpG_probe_id,
  A.aliquot_barcode,
  SUBSTR(A.aliquot_barcode, 14, 2) AS sample_id,
  IF
  (SUBSTR(A.aliquot_barcode, 14, 2) IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19'), "normal",
  IF
  (SUBSTR(A.aliquot_barcode, 14, 2) IN ('01', '02', '03', '04',  '05', '06', '07', '08', '09'), 'tumor', "control")) AS sample_status,
  B.row_number
FROM
  brca_betas A
LEFT JOIN
  participants_id_numbered B
ON A.case_barcode = B.case_barcode
LIMIT 100
"""


TCGA_BETAS_OUTPUT_SCHEMA = "case_barcode:STRING, beta_value:FLOAT, CpG_probe_id:STRING, aliquot_barcode:STRING, " \
                       "sample_id:STRING, sample_status:STRING, row_number:INTEGER"

# CPG Site list config
CPG_SITES_OUTPUT_TABLE = 'dna_cancer_prediction.test_cpg_site_list'

CPG_SITES_LIST_QUERY = """
WITH

cpg_count_table AS (
SELECT CpG_probe_id, count(beta_value) as cpg_count, (SELECT count(distinct aliquot_barcode) FROM `dna_cancer_prediction.test_tcga_betas`) as participant_count
FROM `dna_cancer_prediction.tcga_betas`
GROUP BY CpG_probe_id
),

filter_cpg AS (
SELECT CpG_probe_id, cpg_count, participant_count
FROM cpg_count_table A
WHERE (cpg_count / participant_count)  >= 0.9
),

cpg_filtered AS (
SELECT A.CpG_probe_id, B.beta_value, B.sample_status
FROM filter_cpg A
LEFT JOIN `dna_cancer_prediction.tcga_betas` B
ON A.CpG_probe_id = B.CpG_probe_id
),

cpg_avg_beta AS (
SELECT CpG_probe_id, avg(beta_value) as global_beta_avg
FROM cpg_filtered
GROUP BY CpG_probe_id
),

cpg_avg_beta_tumor AS (
SELECT CpG_probe_id, avg(beta_value) as tumor_beta_avg
FROM cpg_filtered
WHERE sample_status = 'tumor'
GROUP BY CpG_probe_id
),

cpg_avg_beta_normal AS (
SELECT CpG_probe_id, avg(beta_value) as normal_beta_avg
FROM cpg_filtered
WHERE sample_status = 'normal'
GROUP BY CpG_probe_id
),

cpg_averages AS (
SELECT A.CpG_probe_id, A.global_beta_avg, COALESCE(B.tumor_beta_avg, A.global_beta_avg) as tumor_beta_avg, COALESCE(C.normal_beta_avg, A.global_beta_avg) as normal_beta_avg
FROM cpg_avg_beta A
LEFT JOIN cpg_avg_beta_tumor B ON A.CpG_probe_id = B.CpG_probe_id
LEFT JOIN cpg_avg_beta_normal C on A.CpG_probe_id = C.CpG_probe_id
),

cpg_variances AS (
SELECT CpG_probe_id, POWER((tumor_beta_avg - global_beta_avg), 2) + POWER((normal_beta_avg - global_beta_avg), 2) as sum_squares_between_groups
FROM cpg_averages
ORDER BY sum_squares_between_groups desc
)

SELECT *
FROM cpg_variances
LIMIT 5000
"""

CPG_SITES_OUTPUT_SCHEMA = "CpG_probe_id:STRING, sum_squares_between_groups:FLOAT"

# PIVOT dataset config
PIVOT_OUTPUT_TABLE = 'gcp-nyc.dna_cancer_prediction.test_pivot_table'

PIVOT_DATASET_QUERY = """
CALL dna_cancer_prediction.pivot(
  'dna_cancer_prediction.test_tcga_betas' # source table
  , 'dna_cancer_prediction.test_pivot_table_temp' # destination table
  , ['aliquot_barcode'] # row_ids
  , 'CpG_probe_id' # pivot_col_name
  , 'beta_value' # pivot_col_value
  , 100 # max_columns
  , 'SUM' # aggregation
  , '' # optional_limit
);
"""

# BigQuery Example Gen Query
TRAIN_QUERY = """
SELECT A.*, B.sample_status
FROM dna_cancer_prediction.test_pivot_table_temp A
LEFT JOIN dna_cancer_prediction.test_tcga_betas B ON A.aliquot_barcode = B.aliquot_barcode
"""


PREPROCESSING_FN = 'models.preprocessing.preprocessing_fn'
# RUN_FN = 'models.keras.model.run_fn'
# NOTE: Uncomment below to use an estimator based model.
RUN_FN = 'models.estimator.model.run_fn'

TRAIN_NUM_STEPS = 1000
EVAL_NUM_STEPS = 150

# Change this value according to your use cases.
EVAL_ACCURACY_THRESHOLD = 0.6

# Beam args to use BigQueryExampleGen with Beam DirectRunner.
# TODO(step 7): (Optional) Uncomment here to provide GCP related configs for
#               BigQuery.
BIG_QUERY_WITH_DIRECT_RUNNER_BEAM_PIPELINE_ARGS = [
   '--project=' + GOOGLE_CLOUD_PROJECT,
   '--temp_location=' + os.path.join('gs://', GCS_BUCKET_NAME, 'tmp'),
   ]

# The rate at which to sample rows from the Chicago Taxi dataset using BigQuery.
# The full taxi dataset is > 120M record.  In the interest of resource
# savings and time, we've set the default for this example to be much smaller.
# Feel free to crank it up and process the full dataset!
_query_sample_rate = 0.0001  # Generate a 0.01% random sample.

# The query that extracts the examples from BigQuery.  The Chicago Taxi dataset
# used for this example is a public dataset available on Google AI Platform.
# https://console.cloud.google.com/marketplace/details/city-of-chicago-public-data/chicago-taxi-trips
# TODO(step 7): (Optional) Uncomment here to use BigQuery.
# BIG_QUERY_QUERY = """
#         SELECT
#           pickup_community_area,
#           fare,
#           EXTRACT(MONTH FROM trip_start_timestamp) AS trip_start_month,
#           EXTRACT(HOUR FROM trip_start_timestamp) AS trip_start_hour,
#           EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS trip_start_day,
#           UNIX_SECONDS(trip_start_timestamp) AS trip_start_timestamp,
#           pickup_latitude,
#           pickup_longitude,
#           dropoff_latitude,
#           dropoff_longitude,
#           trip_miles,
#           pickup_census_tract,
#           dropoff_census_tract,
#           payment_type,
#           company,
#           trip_seconds,
#           dropoff_community_area,
#           tips,
#           IF(tips > fare * 0.2, 1, 0) AS big_tipper
#         FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
#         WHERE (ABS(FARM_FINGERPRINT(unique_key)) / 0x7FFFFFFFFFFFFFFF)
#           < {query_sample_rate}""".format(
#    query_sample_rate=_query_sample_rate)

# Beam args to run data processing on DataflowRunner.
#
# TODO(b/151114974): Remove `disk_size_gb` flag after default is increased.
# TODO(b/151116587): Remove `shuffle_mode` flag after default is changed.
# TODO(b/156874687): Remove `machine_type` after IP addresses are no longer a
#                    scaling bottleneck.
# TODO(step 8): (Optional) Uncomment below to use Dataflow.
# DATAFLOW_BEAM_PIPELINE_ARGS = [
#    '--project=' + GOOGLE_CLOUD_PROJECT,
#    '--runner=DataflowRunner',
#    '--temp_location=' + os.path.join('gs://', GCS_BUCKET_NAME, 'tmp'),
#    '--region=' + GOOGLE_CLOUD_REGION,
#
#    # Temporary overrides of defaults.
#    '--disk_size_gb=50',
#    '--experiments=shuffle_mode=auto',
#    '--machine_type=n1-standard-8',
#    ]

# A dict which contains the training job parameters to be passed to Google
# Cloud AI Platform. For the full set of parameters supported by Google Cloud AI
# Platform, refer to
# https://cloud.google.com/ml-engine/reference/rest/v1/projects.jobs#Job
# TODO(step 9): (Optional) Uncomment below to use AI Platform training.
# GCP_AI_PLATFORM_TRAINING_ARGS = {
#     'project': GOOGLE_CLOUD_PROJECT,
#     'region': GOOGLE_CLOUD_REGION,
#     # Starting from TFX 0.14, training on AI Platform uses custom containers:
#     # https://cloud.google.com/ml-engine/docs/containers-overview
#     # You can specify a custom container here. If not specified, TFX will use
#     # a public container image matching the installed version of TFX.
#     # TODO(step 9): (Optional) Set your container name below.
#     'masterConfig': {
#       'imageUri': 'gcr.io/' + GOOGLE_CLOUD_PROJECT + '/tfx-pipeline'
#     },
#     # Note that if you do specify a custom container, ensure the entrypoint
#     # calls into TFX's run_executor script (tfx/scripts/run_executor.py)
# }

# A dict which contains the serving job parameters to be passed to Google
# Cloud AI Platform. For the full set of parameters supported by Google Cloud AI
# Platform, refer to
# https://cloud.google.com/ml-engine/reference/rest/v1/projects.models
# TODO(step 9): (Optional) Uncomment below to use AI Platform serving.
# GCP_AI_PLATFORM_SERVING_ARGS = {
#     'model_name': PIPELINE_NAME.replace('-','_'),  # '-' is not allowed.
#     'project_id': GOOGLE_CLOUD_PROJECT,
#     # The region to use when serving the model. See available regions here:
#     # https://cloud.google.com/ml-engine/docs/regions
#     # Note that serving currently only supports a single region:
#     # https://cloud.google.com/ml-engine/reference/rest/v1/projects.models#Model  # pylint: disable=line-too-long
#     'regions': [GOOGLE_CLOUD_REGION],
# }
