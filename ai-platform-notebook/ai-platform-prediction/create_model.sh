MODEL_NAME='build_hackathon'
VERSION_NAME='xgboost_predict_proba'
ORIGIN='gs://build_hackathon_dnanyc/exported_models/xgboost_binary/'
PACKAGE_URIS='gs://build_hackathon_dnanyc/exported_models/prediction_routine/my_custom_code-0.2.tar.gz'
PREDICTION_CLASS='predictor.MyPredictor'
RUNTIME_VERSION='1.15'
PYTHON_VERSION='3.7'

gcloud beta ai-platform versions create $VERSION_NAME \
  --model $MODEL_NAME \
  --runtime-version $RUNTIME_VERSION \
  --python-version $PYTHON_VERSION \
  --origin $ORIGIN \
  --package-uris $PACKAGE_URIS \
  --prediction-class $PREDICTION_CLASS