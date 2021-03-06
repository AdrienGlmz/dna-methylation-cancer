import numpy as np
try:
    # If using sklearn v0.22
    from sklearn.impute import KNNImputer
except ImportError:
    # Else, assume using sklearn v0.20.3
    from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from pandas import DataFrame


def nan_by_columns(betas):
    count_nan = np.sum(np.isnan(betas), axis=0)
    freq_nan = count_nan / betas.shape[0]
    return freq_nan


def nan_by_rows(betas, labels):
    # Compute missing values by rows
    # Sorts the missing into two lists depending on the true label to evaluate the number of missing values per label
    count_nan = np.sum(np.isnan(betas), axis=1)
    freq_nan = count_nan / betas.shape[1]

    freq_and_label = list(zip(labels, freq_nan))
    freq_cancerous = [f for l, f in freq_and_label if l == 1]
    freq_normal = [f for l, f in freq_and_label if l == 0]

    return freq_cancerous, freq_normal


def drop_columns(betas, cpg_sites, threshold=0.1):
    freq_nan = nan_by_columns(betas)
    cols_to_drop = np.where(freq_nan > threshold, range(0, len(freq_nan)), None)
    cols_to_drop = [elt for elt in cols_to_drop if elt]
    print(f"{len(cols_to_drop)} columns will be dropped.")
    new_betas = np.delete(betas, cols_to_drop, axis=1)
    new_cpg_sites = np.delete(cpg_sites, cols_to_drop, axis=0)
    print(f"betas: New shape is {new_betas.shape}")
    print(f"cpg_sites: New shape is {new_cpg_sites.shape}")
    return new_betas, new_cpg_sites


def drop_rows(betas, labels, index, threshold=0.1):
    count_nan = np.sum(np.isnan(betas), axis=1)
    freq_nan = count_nan / betas.shape[0]

    rows_to_drop = np.where(freq_nan > threshold, range(0, len(freq_nan)), None)
    rows_to_drop = [elt for elt in rows_to_drop if elt]
    print(f"We will drop {len(rows_to_drop)} rows")
    new_betas = np.delete(betas, rows_to_drop, axis=0)
    new_labels = np.delete(labels, rows_to_drop, axis=0)
    new_index = np.delete(index, rows_to_drop, axis=0)
    print(f"betas: New shape is {new_betas.shape}")
    print(f"labels: New shape is {new_labels.shape}")
    return new_betas, new_labels, new_index


def fill_remaining_na(betas):
    imputer = KNNImputer(n_neighbors=5)
    betas = imputer.fit_transform(betas)
    return betas


def preprocessing(betas, labels, cpg_sites, index, threshold_to_drop=0.1, test_size=0.3, sampling_strategy=0.5,
                  fill_na_strategy='knn', smote=False, undersample=False, train_test=True):
    print(f"=== Drop Columns and Rows ===")
    # Dropping rows for which label is NA
    idx_to_delete = np.where(np.isnan(labels))[0]
    print(f"Dropping {idx_to_delete.shape[0]} because of missing labels")
    labels = np.delete(labels, idx_to_delete)
    betas = np.delete(betas, idx_to_delete, axis=0)
    index = np.delete(index, idx_to_delete)
    print(f"New Shape = {betas.shape}")

    # Dropping columns
    percent_threshold = threshold_to_drop * 100
    print(f"Dropping columns which have more than {percent_threshold:.0f}% of values missing")
    betas, cpg_sites = drop_columns(betas, cpg_sites, threshold=threshold_to_drop)

    # Dropping rows
    print(f"\nDropping rows which have more than {percent_threshold:.0f}% of values missing")
    betas, labels, index = drop_rows(betas, labels, index)

    # Filling remaining NA Values
    print(f"\n=== Fill remaining NAs ===")
    nb_nan = np.sum(np.sum(np.isnan(betas), axis=1), axis=0)
    if fill_na_strategy == 'knn':
        print(f"Filling remaining NA values using a KNNImputer")
        betas = fill_remaining_na(betas)
    elif fill_na_strategy == 'simple':
        print(f"Filling remaining NA values using a Simple Median Imputer")
        imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
        betas = imputer.fit_transform(betas)
    else:
        print(f"Filling remaining NAs with zeros")
        nan_idx = np.where(np.isnan(betas))
        betas[nan_idx] = 0
    print(f"{nb_nan} NA were filled, i.e. approximately {nb_nan / betas.shape[0]:.2f} per rows")

    if train_test:
        print(f"\n=== Train / Test Split ===")
        print(f"Splitting dataset into train and test")
        print(f"Train = {100 - test_size * 100:.0f} %")
        print(f"Test = {test_size * 100:.0f} %")
        X_train, X_test, y_train, y_test = train_test_split(betas, labels, test_size=test_size, random_state=123)

        print(f"\n=== Standardize dataset ===")
        scaler = StandardScaler().fit(X_train)
        X_train_scaled = scaler.transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        print(f"The average of column mean on train is {np.mean(np.mean(X_train_scaled, axis=1), axis=0):.2f}")
        print(f"The average of column mean on test is {np.mean(np.mean(X_test_scaled, axis=1), axis=0):.2f}")

        if smote:
            print("\n=== Balance dataset with oversample ===")
            # Computing multi-class ratio
            unique, count = np.unique(y_train, return_counts=True)
            print(list(zip(unique, count)))
            m = max(count)
            majority_class = unique[np.argmax(count)]

            # Every class will be oversampled to (ratio) * #observations in majority class
            # Except the majority class which is left as is
            resampling_strategy = {k: max(c, int(sampling_strategy * m)) for (k, c) in zip(unique, count)}
            resampling_strategy[majority_class] = m

            print(f"The resampling_strategy gives the following repartition {resampling_strategy}")
            sm = SMOTE(random_state=123, sampling_strategy=resampling_strategy)
            X_train_res, y_train_res = sm.fit_sample(X_train_scaled, y_train)
            print(f"{X_train_res.shape[0] - X_train_scaled.shape[0]} rows were added in the training data")
        elif undersample:
            print("=== Balance dataset with undersample ===")

            print(f"The resampling_strategy gives the following repartition {sampling_strategy}")
            under_sampling = RandomUnderSampler(sampling_strategy=sampling_strategy)
            X_train_res, y_train_res = under_sampling.fit_resample(X_train_scaled, y_train)
        else:
            X_train_res = X_train_scaled
            y_train_res = y_train

        return X_train_res, X_test_scaled, y_train_res, y_test, labels, cpg_sites

    else:
        df = DataFrame(betas, columns=cpg_sites, index=index)
        df['label'] = labels
        df['label'] = df['label'].astype(int)
        return df
