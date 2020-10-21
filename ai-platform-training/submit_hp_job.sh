#!/bin/bash
BUCKET_NAME="dna-methylation-cancer"
JOB_NAME="dna_methylation_xgboost_binary_$(date +"%Y%m%d_%H%M%S")"
IMAGE_URI="gcr.io/gcp-nyc/dna-methylation-cancer:xgboost-binary"
JOB_DIR="gs://$BUCKET_NAME/job_dir"
HPTUNING_CONFIG="./trainer/hptuning_config_xgboost.yaml"
REGION="us-east1"
SCALE_TIER=CUSTOM
MASTER_MACHINE_TYPE="n1-standard-4"

gcloud ai-platform jobs submit training $JOB_NAME \
  --region $REGION \
  --master-image-uri $IMAGE_URI \
  --scale-tier $SCALE_TIER \
  --master-machine-type $MASTER_MACHINE_TYPE \
  --config $HPTUNING_CONFIG
  -- \
  --model-dir $JOB_DIR \
