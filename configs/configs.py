# GCP config variables
PROJECT_ID = 'gcp-nyc'  # TO REPLACE WITH YOUR GCP PROJECT
DATASET = 'dna_cancer_prediction'
BETAS_TABLE_NAME = 'tcga_betas'
CPG_SITE_TABLE_NAME = 'cpg_site_list'
GCS_BUCKET = 'dna-methylation-cancer'  # TO REPLACE WITH YOUR GCS BUCKET
REGION = "us-east1"

# Preprocessing variables
RAW_DATASET_NAME = 'tcga-binary.csv'
RAW_DATASET_PATH = f'training_data/{RAW_DATASET_NAME}'
RAW_LABEL_NAME = 'sample_status'
RAW_INDEX_NAME = 'aliquot_barcode'

# Training variables
TRAINING_LABEL_NAME = 'labels'
TRAIN_DATASET_PATH = 'training_data/train.csv'
TEST_DATASET_PATH = 'training_data/test.csv'

# AI Platform training job
IMAGE_REPO_NAME = 'gcr.io/gcp-nyc/dna-methylation-cancer'
IMAGE_TAG = 'xgboost-binary'
IMAGE_URI = f"gcr.io/{PROJECT_ID}/{IMAGE_REPO_NAME}:{IMAGE_TAG}"
TRAINING_DOCKERFILE = 'ai-platform-training/Dockerfile'
HPTUNING_CONFIG = 'ai-platform-training/trainer/hptuning_config_xgboost.yaml'

# AI Platform prediction
MODEL_NAME = 'dna_cancer_prediction'
VERSION_NAME = 'xgboost_binary_test'
ORIGIN = f'gs://{GCS_BUCKET}/job_dir/20201021_204518/'
PACKAGE_URIS = f'gs://{GCS_BUCKET}/prediction_routine/my_custom_code-0.2.tar.gz'
PREDICTION_CLASS = 'predictor.MyPredictor'
RUNTIME_VERSION = '2.2'
PYTHON_VERSION = '3.7'
