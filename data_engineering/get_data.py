import pandas as pd
import numpy as np
import shutil
import traceback
import os

from google.cloud import storage

BUCKET_NAME = 'build_hackathon_dnanyc'
GCS_PREFIX = 'pivot_data_v2/'


def configure_gcs():
    client = storage.Client()
    return client


def download_data(client, bucket_name, gcs_prefix, nb_partition=10,
                  destination='input_data/', debug=False):
    # Set up bucket
    bucket = client.bucket(bucket_name)

    # Get file list
    partition_list = client.list_blobs(bucket_name, prefix=gcs_prefix, delimiter='/')
    partition_list = [elt.name for elt in partition_list]
    downloaded_files = []

    # Create folder if it does not exist
    if not os.path.isdir(destination):
        os.mkdir(destination)

    # Download files
    for blob_path in partition_list[:nb_partition]:
        blob = bucket.get_blob(blob_path)
        filename = blob_path.split('/')[-1]
        blob.download_to_filename(destination + filename)
        downloaded_files.append(destination + filename)
        if debug:
            print("Downloaded {} to {}".format(blob_path, destination + filename))

    return downloaded_files


def read_and_concatenate(files, label_names=['label'], project_filter=None, nb_files=100, debug=False):
    df0 = pd.read_csv(files[0], delimiter=',', index_col=0)
    if project_filter:
        df0 = df0[df0['project_short_name'] == project_filter]
    betas = df0.drop(label_names, axis=1).values
    if debug:
        print("Loading files...")
        print(betas.shape)
    cpg_sites = df0.drop(label_names, axis=1).columns
    index = df0.index.values
    labels = df0[label_names].values

    for file_path in files[1:nb_files]:
        try:
            df = pd.read_csv(file_path, delimiter=',', index_col=0)
            if project_filter:
                df = df[df['project_short_name'] == project_filter]
            betas_add = df.drop(label_names, axis=1).values
            index_add = df.index.values
            labels_add = df[label_names].values
            betas = np.concatenate((betas, betas_add), axis=0)
            index = np.concatenate((index, index_add), axis=0)
            labels = np.concatenate((labels, labels_add), axis=0)
            if debug:
                print(betas.shape)
        except:
            traceback.print_exc()
            print(f"WARNING: Skipping {file_path}")
    print(f"Loaded dataset. Shape = {betas.shape}")

    if labels.shape[1] == 1:
        n_rows = labels.shape[0]
        labels = labels.reshape(n_rows,)
    return betas, labels, cpg_sites, index


def read_dataset(file_path, label_names=['label'], project_filter=None,
                 gcs_prefix=GCS_PREFIX, nb_partition=10, debug=False):
    """
    Main function to download the dataset

    :param project_filter: Add a filter to restrict on one TCGA project
    :param gcs_prefix: Path in Google Cloud Storage
    :param file_path: folder in which to download the data
    :param nb_partition: Number of partitions to load
    :param debug: Boolean to print debug information
    :return: tuple (betas, labels, cpg_sites, index)
        :betas: dataset, np.array
        :labels: labels to predict (0 or 1)
        :cpg_sites: name of cpg_site. Matches the number of columns in betas
        :index: patient ids. Matches the number of rows in betas
    """
    if file_path[-1] != '/':
        file_path = file_path + '/'

    if not os.path.isdir(file_path):
        os.mkdir(file_path)
        print(f"Created folder {file_path}")
    if len(os.listdir(file_path)) != nb_partition:
        shutil.rmtree(file_path)
        os.mkdir(file_path)
        client = configure_gcs()
        print(f"Downloading data...")
        download_data(client, BUCKET_NAME, gcs_prefix, nb_partition, file_path, debug=debug)
    else:
        print(f"Using already downloaded data")
    files = os.listdir(file_path)
    files = [f'{file_path}/' + elt for elt in files]
    betas, labels, cpg_sites, index = read_and_concatenate(files, label_names=label_names,
                                                           project_filter=project_filter)

    return betas, labels, cpg_sites, index