import argparse
from google.cloud import storage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
from sklearn.externals import joblib
import pandas as pd
import hypertune

# Create the argument parser for each parameter plus the job directory
parser = argparse.ArgumentParser()

parser.add_argument(
    '--job-dir',  # Handled automatically by AI Platform
    help='GCS location to write checkpoints and export models',
    required=True
    )

parser.add_argument(
    '--ccp_alpha',  # Specified in the config file
    help='Constant that multiplies the regularization term',
    default=0.1,
    type=float
    )
parser.add_argument(
    '--max_features',  # Specified in the config file
    help='Max number of features to consider at each split.',
    default=70,
    type=int
    )

parser.add_argument(
    '--n_estimators',  # Specified in the config file
    help='Number of trees to fit in the forest',
    default=100,
    type=int
    )

parser.add_argument(
    '--max_depth',  # Specified in the config file
    help='MAx depth of each tree in the forest',
    default=20,
    type=int
    )

args = parser.parse_args()

# Define the GCS bucket the training data is in
bucket = storage.Client().bucket('build_hackathon_dnanyc')

# Define the source blob name (aka file name) for the training data
blob = bucket.blob('training_data/tcga-brca-data-pathologic_M-preprocessed.csv')
label_name = 'labels'

# Download the data into a file name
blob.download_to_filename('train.csv')

# Open the csv into a df
with open('./train.csv', 'r') as df_train:
    df = pd.read_csv(df_train)

X = df.drop(label_name, axis=1).values
y = df[label_name].values

# Train test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.3, shuffle=False)

# Define the model with the parameters we want to tune
model = RandomForestClassifier(
    ccp_alpha=args.ccp_alpha,
    n_estimators=args.n_estimators,
    max_features=args.max_features,
    max_depth=args.max_depth
    )

# Fit the training data and predict the test data
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Define the score we want to use to evaluate the classifier on
score = f1_score(y_test, y_pred, average='micro')

# Calling the hypertune library and setting our metric
hpt = hypertune.HyperTune()
hpt.report_hyperparameter_tuning_metric(
    hyperparameter_metric_tag='f1_score_micro',
    metric_value=score,
    global_step=1000
    )

# Export the model to a file. The name needs to be 'model.joblib'
model_filename = 'model-M-micro.joblib'
joblib.dump(model, model_filename)

# Define the job dir, bucket id and bucket path to upload the model to GCS
job_dir = args.job_dir.replace('gs://', '')  # Remove the 'gs://'

# Get the bucket Id
bucket_id = job_dir.split('/')[0]

# Get the path
bucket_path = job_dir.lstrip('{}/'.format(bucket_id))

# Upload the model to GCS
bucket = storage.Client().bucket(bucket_id)
blob = bucket.blob('{}/{}'.format(
    bucket_path,
    model_filename
    )
)
blob.upload_from_filename(model_filename)