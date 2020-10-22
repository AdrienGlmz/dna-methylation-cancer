## Command to run
# gcloud dataproc jobs submit pyspark --cluster build-hackathon-nyc-cluster --region us-east1 --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar create_cpg_sites_list.py -- -gcs_bucket "build_hackathon_dnanyc_tfx_pipeline"

# Import modules
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Define user functions
target_column = 'pathologic_M'


def get_mapping_sub_labels_to_classes(target_column):
    if target_column == 'pathologic':
        map_sub_labels_to_classes = {
         'Stage I': 'stage_1',
         'Stage IA': 'stage_1',
         'Stage IB': 'stage_1',
         'Stage II': 'stage_2',
         'Stage IIA': 'stage_2',
         'Stage IIB': 'stage_2',
         'Stage III': 'stage_3',
         'Stage IIIA': 'stage_3',
         'Stage IIIB': 'stage_3',
         'Stage IIIC': 'stage_3',
         'Stage IV': 'stage_4',
         'Stage X': 'stage_5',
          None: None,
          'stage_0': 'stage_0'
        }
    elif target_column == 'pathologic_T':
        map_sub_labels_to_classes = {
         'T1b': 'stage_1',
         'TX': None,
         'T2': 'stage_2',
         'T2b': 'stage_2',
         'T4': 'stage_4',
         'T4b':'stage_4',
         'T1c': 'stage_1',
         'T1': 'stage_1',
         'T4d': 'stage_4',
         'T2a': 'stage_2',
         'T3a': 'stage_3',
         'T3': 'stage_3',
         'T1a': 'stage_1',
         'stage_0': 'stage_0'
        }
    elif target_column == 'pathologic_N':
        map_sub_labels_to_classes = {
         'N3c': 'stage_3',
         'N1c': 'stage_1',
         'N3': 'stage_3',
         'N2a': 'stage_2',
         'N3b': 'stage_3',
         'N3a': 'stage_3',
         'N0': 'stage_0',
         'NX': None,
         'N1': 'stage_1',
         'N0 (i-)': 'stage_0',
         'N2': 'stage_2',
         'N0 (mol+)': 'stage_0',
         'N1b': 'stage_1',
         'N1mi': 'stage_1',
         'N0 (i+)': 'stage_0',
         'N1a': 'stage_1',
         'stage_0': 'stage_0'
        }
    elif target_column == 'pathologic_M':
        map_sub_labels_to_classes = {
            'MX': None,
            'cM0 (i+)': 'stage_0',
            'M0': 'stage_0',
            'M1': 'stage_1',
            'stage_0': 'stage_0'
        }
    else:
        map_sub_labels_to_classes = {
            ''
        }

    mapping_values = sorted(list(set([v for k, v in map_sub_labels_to_classes.items() if v])))
    return mapping_values


def identify_non_cancerous_samples(sample_code, label_col):
    if sample_code <= 9:
        return label_col
    else:
        return 'stage_0'


def map_to_classes(elt):
    return map_sub_labels_to_classes[elt]


def get_cpg_sites_binary_target(df):
    CLASSES = ['tumor', 'normal']

    df = df.withColumnRenamed('sample_status', 'label')
    df = df.withColumnRenamed('aliquot_barcode', 'participant_id')
    df = df.withColumn("beta_value", df.beta_value.cast(DoubleType()))
    df = df.withColumn('sample_id', df.sample_id.cast(IntegerType()))

    # Transformation
    # | CpG_probe_id | tumor | normal | count | beta_value |
    # Count CpG sites
    df_count = df.groupby('CpG_probe_id').count()
    df_count = df_count.withColumnRenamed('count', 'count_cpg')

    # Count number of rows
    sample_count = df.select('participant_id').dropDuplicates().count()

    # Variance by group
    mean_by_cpg = df.groupby('CpG_probe_id').pivot('label', CLASSES).avg('beta_value')
    cpg_df = mean_by_cpg.join(df_count, ['CpG_probe_id'], 'left')

    # Discard if no value for more than 90% of patients
    cpg_df = cpg_df.withColumn('freq', cpg_df.count_cpg / sample_count)
    cpg_df = cpg_df.filter(cpg_df.freq > 0.9)
    cpg_df = cpg_df.drop('count_cpg').drop('freq')

    # Get mean overall
    mean = df.groupby('CpG_probe_id').avg('beta_value')
    mean = mean.withColumnRenamed("avg(beta_value)", 'avg_beta')

    # Join with mean overall
    values_to_select = ['a.CpG_probe_id'] + ['a.' + elt for elt in CLASSES] + ['b.avg_beta']
    mean_by_cpg = (cpg_df.alias('a')
                   .join(mean.alias('b'), mean.CpG_probe_id == cpg_df.CpG_probe_id, "left")
                   .select(values_to_select)
                   )

    # Get sum of squared
    for elt in CLASSES:
        mean_by_cpg = mean_by_cpg.withColumn(elt, coalesce(elt, 'avg_beta'))
        mea_by_cpg = mean_by_cpg.withColumn(elt, (mean_by_cpg[elt] - mean_by_cpg['avg_beta']) ** 2)

    mean_by_cpg = mean_by_cpg.withColumn("sum_squared_between_groups", sum(mean_by_cpg[elt] for elt in CLASSES))
    mean_by_cpg = mean_by_cpg.orderBy('sum_squared_between_groups', ascending=False)

    top_cpg = mean_by_cpg.select('CpG_probe_id').limit(5000)
    return top_cpg


def get_cpg_sites_multiclass_target(df, target_column):
    # Reformat columns
    df = df.withColumnRenamed('sample_status', 'label')

    df = df.withColumn("beta_value", df.beta_value.cast(DoubleType()))
    df = df.withColumn('sample_id', df['sample_id'].cast(IntegerType()))

    # # Filter on cancer observations only
    df = df.filter(df['label'] == 'tumor')

    # Import cancer stage data
    # Read from BigQuery
    cancer_stage_df = (
          spark
          .read
          .format("bigquery")
          .option("table", "gcp-nyc.build_hackathon_dnanyc.patient_cancer_stage_v4")
          .load())
    cancer_stage_df = cancer_stage_df.dropDuplicates()

    # Join Cancer stage data with main dataframe
    df = (
        df.alias('a')
        .join(cancer_stage_df.alias('b'), df.participant_id == cancer_stage_df.case_barcode, 'left')
        .select('a.beta_value', 'a.CpG_probe_id', 'a.sample_id', 'a.participant_id', 'b.pathologic_stage',
                'b.pathologic_T', 'b.pathologic_N', 'b.pathologic_M', 'b.project_short_name')
    )

    # Filter on BRCA study
    df = df.filter(df.project_short_name == 'TCGA-BRCA')
    df = df.drop('project_short_name')

    # Reformat columns
    df = df.withColumn(target_column, identify_non_cancerous_samples_udf('sample_id', target_column))
    df = df.withColumn(target_column, map_to_classes_udf(target_column))

    # Transformation
    # Count CpG sites
    df_count = df.groupby('CpG_probe_id').count()
    df_count = df_count.withColumnRenamed('count', 'count_cpg')

    sample_count = df.select('participant_id').dropDuplicates().count()

    # Variance by group
    mean_by_cpg = df.groupby('CpG_probe_id').pivot(target_column, mapping_values).avg('beta_value')
    cpg_df = mean_by_cpg.join(df_count, ['CpG_probe_id'], 'left')

    # Discard if no value for more than 90% of patients
    cpg_df = cpg_df.withColumn('freq', cpg_df.count_cpg / sample_count)
    cpg_df = cpg_df.filter(cpg_df.freq > 0.9)
    cpg_df = cpg_df.drop('count_cpg').drop('freq')

    # Get mean overall
    mean = df.groupby('CpG_probe_id').avg('beta_value')
    mean = mean.withColumnRenamed("avg(beta_value)", 'avg_beta')

    # Join with mean overall
    values_to_select = ['a.CpG_probe_id'] + ['a.' + elt for elt in mapping_values] + ['b.avg_beta']
    mean_by_cpg = (cpg_df.alias('a')
                   .join(mean.alias('b'), mean.CpG_probe_id == cpg_df.CpG_probe_id, "left")
                   .select(values_to_select)
                   )

    # Get sum of squared
    for elt in mapping_values:
        mean_by_cpg = mean_by_cpg.withColumn(elt, coalesce(elt, 'avg_beta'))
        mea_by_cpg = mean_by_cpg.withColumn(elt, (mean_by_cpg[elt] - mean_by_cpg['avg_beta']) ** 2)

    mean_by_cpg = mean_by_cpg.withColumn("sum_squared_between_groups", sum(mean_by_cpg[elt] for elt in mapping_values))
    mean_by_cpg = mean_by_cpg.orderBy('sum_squared_between_groups', ascending=False)

    top_cpg = mean_by_cpg.select('CpG_probe_id').limit(5000)
    top_cpg = top_cpg.rdd.map(lambda x: x.CpG_probe_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-gcs_bucket", help="GCS temporary bucket")
    # parser.add_argument("target", help="Choose between binary or multiclass target")
    args = parser.parse_args()
    print(args.gcs_bucket)
    identify_non_cancerous_samples_udf = udf(identify_non_cancerous_samples, StringType())
    map_to_classes_udf = udf(map_to_classes, StringType())

    spark = (SparkSession
             .builder
             .appName('my_app')
             .getOrCreate())

    spark.conf.set('temporaryGcsBucket', args.gcs_bucket)

    betas_df = (
          spark
          .read
          .format("bigquery")
          .option("table", "gcp-nyc.dna_cancer_prediction.tcga_betas")
          .load())

    top_cpg = get_cpg_sites_binary_target(betas_df)
    (top_cpg
     .write
     .format('bigquery')
     .option('table', 'gcp-nyc.dna_cancer_prediction.cpg_site_list')
     .save()
     )
    # top_cpg.repartition(6).saveAsTextFile('gs://build_hackathon_dnanyc/columns_to_keep_v4/' + target_column)
