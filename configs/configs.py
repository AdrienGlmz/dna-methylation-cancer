# GCP config variables
PROJECT_ID = 'gcp-nyc'
DATASET = 'dna_cancer_prediction'
GCS_BUCKET = 'dna-methylation-cancer'
REGION = "us-east1"

# Preprocessing variables
RAW_DATASET_PATH = 'training_data/tcga-binary.csv'
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
ORIGIN = 'gs://dna-methylation-cancer/job_dir/20201021_204518/'
PACKAGE_URIS = 'gs://dna-methylation-cancer/prediction_routine/my_custom_code-0.2.tar.gz'
PREDICTION_CLASS = 'predictor.MyPredictor'
RUNTIME_VERSION = '2.2'
PYTHON_VERSION = '3.7'
