export BUCKET_NAME=dna-methylation-cancer
export JOB_NAME=dna_methylation_xgboost_binary_$(date +"%Y%m%d_%H%M%S")
export IMAGE_URI=gcr.io/gcp-nyc/dna-methylation-cancer:xgboost-binary
export MODEL_DIR=gs://$BUCKET_NAME/job_dir/$(date +%Y%m%d_%H%M%S)
export HPTUNING_CONFIG=trainer/hptuning_config_xgboost.yaml
export REGION=us-east1
export SCALE_TIER=custom
export MASTER_MACHINE_TYPE=n1-standard-4
echo $HPTUNING_CONFIG
gcloud ai-platform jobs submit training $JOB_NAME --region $REGION  --master-image-uri $IMAGE_URI --config $HPTUNING_CONFIG -- --model-dir=$MODEL_DIR