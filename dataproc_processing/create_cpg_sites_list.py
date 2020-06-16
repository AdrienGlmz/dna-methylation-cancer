## Command to run
# gcloud dataproc jobs submit pyspark --cluster build-hackathon-nyc-cluster --region us-east1 create_cpg_sites_list.py --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar

# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Define user functions
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

mapping_values = sorted(list(set([v for k, v in map_sub_labels_to_classes.items() if v])))


def identify_non_cancerous_samples(sample_code, label_col):
    if sample_code <= 9:
        return label_col
    else:
        return 'stage_0'


def map_to_classes(elt):
    return map_sub_labels_to_classes[elt]


identify_non_cancerous_samples_udf = udf(identify_non_cancerous_samples, StringType())
map_to_classes_udf = udf(map_to_classes, StringType())


spark = (SparkSession
         .builder
         .appName('my_app')
         .getOrCreate())

# Import BRCA CSV
# To read the full dataset
print("Reading from Big Query")
# df = (
#       spark
#       .read
#       .format("bigquery")
#       .option("table", "gcp-nyc.build_hackathon_dnanyc.brca_betas_clustered_efficient")
#       .load())

# dataset in GCS
df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .load("gs://build_hackathon_dnanyc/raw_data_clustered_v2/betas_patients_partition_*.csv")
      .drop('BRCA_ParticipantCode').drop('row_number'))

# Reformat columns
df = df.withColumnRenamed('sample_status', 'label')
df = df.withColumn("beta_value", df.beta_value.cast(DoubleType()))
df = df.withColumn('sample_id', df['sample_id'].cast(IntegerType()))

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
    .select('a.beta_value', 'a.CpG_probe_id', 'a.sample_id', 'a.participant_id', 'b.pathologic_stage', 'b.pathologic_T',
           'b.pathologic_N', 'b.pathologic_M', 'b.project_short_name')
)

# Filter on BRCA study
df = df.filter(df.project_short_name == 'TCGA-BRCA')
df = df.drop('project_short_name')

# Reformat columns
df = df.withColumn("pathologic_stage", identify_non_cancerous_samples_udf('sample_id', 'pathologic_stage'))
df = df.withColumn("pathologic_stage", map_to_classes_udf('pathologic_stage'))

# Transformation
# Count CpG sites
df_count = df.groupby('CpG_probe_id').count()
df_count = df_count.withColumnRenamed('count', 'count_cpg')

sample_count = df.select('participant_id').dropDuplicates().count()

# Variance by group
mean_by_cpg = df.groupby('CpG_probe_id').pivot('pathologic_stage', mapping_values).avg('beta_value')
cpg_df = mean_by_cpg.join(df_count, ['CpG_probe_id'], 'left')

# Discard if no value for more than 90% of patients
cpg_df = cpg_df.withColumn('freq', cpg_df.count_cpg / sample_count)
cpg_df = cpg_df.filter(cpg_df.freq > 0.9)
cpg_df = cpg_df.drop('count_cpg').drop('freq')

# Get mean overall
mean = df.groupby('CpG_probe_id').avg('beta_value')
mean = mean.withColumnRenamed("avg(beta_value)", 'avg_beta')

# Join with mean overall
values_to_select = ['a.CpG_probe_id'] + ['a.'+ elt for elt in mapping_values] + ['b.avg_beta']
mean_by_cpg = (cpg_df.alias('a')
     .join(mean.alias('b'), mean.CpG_probe_id == cpg_df.CpG_probe_id, "left")
     .select(values_to_select)
    )

# Get sum of squared
for elt in mapping_values:
    mean_by_cpg = mean_by_cpg.withColumn(elt, coalesce(elt, 'avg_beta'))
    mea_by_cpg = mean_by_cpg.withColumn(elt, (mean_by_cpg[elt] - mean_by_cpg['avg_beta'])**2)

mean_by_cpg = mean_by_cpg.withColumn("sum_squared_between_groups", sum(mean_by_cpg[elt] for elt in mapping_values))
mean_by_cpg = mean_by_cpg.orderBy('sum_squared_between_groups', ascending=False)

top_cpg = mean_by_cpg.select('CpG_probe_id').limit(100)
top_cpg = top_cpg.rdd.map(lambda x: x.CpG_probe_id)

top_cpg.repartition(6).saveAsTextFile('gs://build_hackathon_dnanyc/columns_to_keep_v3/pathologic_stage')
