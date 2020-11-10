# Predicting Cancer with DNA Methylation data

This repository holds the code for the "Predicting Cancer with DNA Methylation data" that 
was submitted to the 2020 _Build Hackathon.

## Objective
Using open-source data from The Cancer Genome Atlas (TCGA), our objective is to model the link between 
DNA Methylation data and cancer development.

## Repo structure

The main file in this repo is the `working-notebook` Jupyter notebook.
It walks through every steps in the pipeline from data acquisition to model deployment.

The jupyter notebook is importing code from the other folders in this repo.
 1. `configs`
    
    This folder contains  configuration files and environment variables
    
 2. `feature_engineering`
 
    This folder holds Python modules that are used for feature engineering.
    This folder creates a Python library that is then called by the `working-notebook` Jupyter Notebook. 

 3. `sql_queries`

    This folder contains text files that can be read as SQL queries.
    
 4. `ai-platform-training`
 
    This folder holds configuration scripts to launch a AI Platform Training job.
    This was used to do hyperparameter training and get the best performance out of the selected model by step 3.
 5. `ai-platform-prediction`
    
    This folder holds configuration scripts to deploy a model on the AI Platform service.
    This easily creates a callable API that is then used by the web-application.

The two folders below are not used as part of the Jupyter Notebook pipeline but are there for reference
 - `dataproc_processing` (Not used)
    
    This folder holds pySpark scripts that were used as pre-processing steps.
    Those scripts are designed to work with a Dataproc instance.
 - `tfx-pipeline` (Not used)
  
    A tfx pipeline to executes steps from beginning to end with one GCP service.
## Requirements

### Python and conda

Python 3 is the language used throughout this repo. The conda package manager was used.

To be able to run all scripts, you can re-create the conda used with
```
conda env create -f environment.yml
``` 

Activate the environment using

```
conda activate DNA_Methylation
```


### Google Cloud Credentials

Most of the scripts are leveraging google services. To be able to use then, you need to have access
to a Google Cloud Platform project and have the google cloud cli [installed](https://cloud.google.com/sdk/)
and configured using

```
gcloud init
```

## How to use

The pipeline has been built using the following steps
 1. **Create BigQuery tables**
 
   Using the `data_engineering` subfolder, create the BigQuery table using SQL scripts.
   Those table are created from the TCGA tables already stored in BigQuery.
   For more information, see the documentation [here](https://isb-cancer-genomics-cloud.readthedocs.io/en/latest/sections/BigQuery.html)

 2. **Get the CpG site list to keep**
 
   This CpG site list will be useful to create our training dataset.
   Because there are approximately 500,000 CpG sites, we are keeping a subset of this list only to include as features (columns) of our dataset.
   We are selection the CpG sites *to maximize variance between groups that we want to predict*.
   For instance, if we want to predict cancer development, we will choose CpG sites by maximizing variance between cancerous and non-cancerous observations.
   We are using DataProc ad PySpark scripts (in `dataproc_processing`) for this and saving this list in Google Cloud Storage (GCS).
 
 3. **Create the training dataset**
 
   Based on the CpG site list created at step 1, we are creating the training dataset.
   This will be done by querying the BigQuery table created at step 2. This dataset will be grouped by and pivoted to get one row per patient using Python and Pandas and saved in GCS. 
   This is done with a Jupyter Notebook script using Google AI Platform's Jupyter notebooks (located in `data_engineering`)
 
 4. **Modeling**
 
   This is the experimental modeling phase.
   Using Google AI Platform's Jupyter notebooks, we are training multiple models types by using the saved training data in GCS.
   The Jupyter Notebooks are saved in the `modeling` folder.
 
 5. **AI Platform Training**
 
   Once the model type has been chosen (e.g. XGBoost), we are retraining this model on GCP's AI Platform to optimize the hyper-parameters.
   This module is saved in `ai-platform-prediction`. The final model is saved in GCS.
 
 6. **AI Platform Prediction**
 
   The final saved model is finally deployed using AI Platform prediction.
   This allows us to get a callable API then integrated into the web-application - [repo here](https://github.com/Jeremy0dell/build-hackathon)