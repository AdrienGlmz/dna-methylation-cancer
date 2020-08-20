# Prediction Cancer with DNA Methylation data

This repository holds the code for the "Predicting Cancer with DNA Methylation data" that 
was submitted to the 2020 _Build Hackathon.

## Objective
Using open-source data from The Cancer Genome Atlas (TCGA), our objective is to model the link between 
DNA Methylation data and cancer development.

## Repo structure

This repository is divided into the following folders. Each folder will have its own README.
 1. `dataproc_processing`
    
    This folder holds pySpark scripts that were used as pre-processing steps.
    Those scripts are designed to work with a Dataproc instance. 
 2. `data_engineering`
 
    This folder holds Python scripts that are used for data engineering.
    This folder creates a Python library that is then called by the scripts in the `3.modeling` folder. 
 3. `modeling`
    
    This folder holds Jupyter Notebook files were used for modeling experiments.
 4. `ai-platform-training`
 
    This folder holds configuration scripts to launch a AI Platform Training job.
    This was used to do hyperparameter training and get the best performance out of the selected model by step 3.
 5. `ai-platform-prediction`
    
    This folder holds configuration scripts to deploy a model on the AI Platform service.
    This easily creates a callable API that is then used by the web-application.

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