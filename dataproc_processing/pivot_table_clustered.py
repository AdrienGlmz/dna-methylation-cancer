## Command to run
# gcloud dataproc jobs submit pyspark --cluster build-hackathon-nyc-cluster --region us-east1 pivot_table_clustered.py

# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, monotonically_increasing_id, concat, lit, when
from pyspark.sql.types import DoubleType, StringType

spark = (SparkSession
         .builder
         .appName('pivot_app_clustered')
         .getOrCreate())


for i in range(1, 11001, 100):
    print('\n=================')
    print('Processing Partition {}. #{} of #109'.format(i, i // 100))
    print('=================\n')
    # Import BRCA CSV
    # To read the full dataset
    df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .load("gs://build_hackathon_dnanyc/raw_data_clustered/betas_patients_partition_{}_*.csv".format(i)))
    df = df.withColumn('label', when(df.BRCA_ParticipantCode == "non-BRCA", 0).otherwise(1))

    df = df.withColumn("beta_value",df.beta_value.cast(DoubleType()))

    df.printSchema()

    # Variance by group
    top_cpg = spark.sparkContext.textFile('gs://build_hackathon_dnanyc/columns_to_keep/')
    top_cpg = top_cpg.collect()


    # Pivot table
    pivot_df = (df
          .select(['beta_value', 'CpG_probe_id', 'participant_id'])
          .groupBy('participant_id')
          .pivot('CpG_probe_id', top_cpg)
          .avg('beta_value')
         )
    pivot_df = (pivot_df
                .join(df.select('participant_id', 'label').dropDuplicates(), ['participant_id'], 'left')
               )

    # Load data
    print("Data Engineering done!")
    print("Loading into GCS....")
    (pivot_df
    .coalesce(32)
    .write.csv('gs://build_hackathon_dnanyc/processed_data/pivot_partition_{}.csv'.format(i), header='true')
    )
