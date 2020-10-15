#!/bin/bash
BUCKET_NAME="build_hackathon_dnanyc"
JOB_NAME="dna_methylation_rf_multiclass_$(date +"%Y%m%d_%H%M%S")"
JOB_DIR="gs://$BUCKET_NAME/job_dir"
TRAINER_PACKAGE_PATH="./trainer"
MAIN_TRAINER_MODULE="trainer.random_forest_multiclass"
HPTUNING_CONFIG="./trainer/hptuning_config_random_forest.yaml"
RUNTIME_VERSION="2.2"
PYTHON_VERSION="3.7"
REGION="us-east1"
SCALE_TIER=CUSTOM
MASTER_MACHINE_TYPE="n1-standard-4"

gcloud ai-platform jobs submit training $JOB_NAME \
  --job-dir $JOB_DIR \
  --package-path $TRAINER_PACKAGE_PATH \
  --module-name $MAIN_TRAINER_MODULE \
  --region $REGION \
  --runtime-version=$RUNTIME_VERSION \
  --python-version=$PYTHON_VERSION \
  --scale-tier $SCALE_TIER \
  --master-machine-type $MASTER_MACHINE_TYPE \
  --config $HPTUNING_CONFIG

# Optional command to stream the logs in the console
#gcloud ai-platform jobs stream-logs $JOB_NAME