## Command to run
# gcloud dataproc jobs submit pyspark --cluster build-hackathon-nyc-cluster --region us-east1 create_cpg_sites_list.py

# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, monotonically_increasing_id, concat, lit, when
from pyspark.sql.types import DoubleType, StringType

spark = (SparkSession
         .builder
         .appName('my_app')
         .getOrCreate())

# Import BRCA CSV
# To read the full dataset
df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .load("gs://build_hackathon_dnanyc/raw_data_clustered/betas_patients_partition_*.csv"))
df = df.withColumn('label', when(df.BRCA_ParticipantCode == "non-BRCA", 0).otherwise(1))

df = df.withColumn("beta_value",df.beta_value.cast(DoubleType()))

df.printSchema()

# Count by CpG Sites
df_count = df.groupby('CpG_probe_id').count()
df_count = df_count.withColumnRenamed('count', 'count_cpg')

patient_count = df.select('participant_id').dropDuplicates().count()

# Variance by group
mean_by_cpg = df.groupby('CpG_probe_id').pivot('label', ['0', '1']).avg('beta_value')
mean_by_cpg = mean_by_cpg.withColumnRenamed('1', 'cancer').withColumnRenamed('0', 'normal')

cpg_df = mean_by_cpg.join(df_count, ['CpG_probe_id'], 'left')
cpg_df.printSchema()

cpg_df = cpg_df.withColumn('freq', cpg_df.count_cpg / patient_count)
cpg_df = cpg_df.filter(cpg_df.freq > 0.9)
cpg_df = cpg_df.drop('count_cpg').drop('freq')


mean = df.groupby('CpG_probe_id').avg('beta_value')
mean = mean.withColumnRenamed("avg(beta_value)", 'avg_beta')

mean_by_cpg = (cpg_df.alias('a')
     .join(mean.alias('b'), mean.CpG_probe_id == cpg_df.CpG_probe_id, "left")
     .select('a.CpG_probe_id', 'a.normal', 'a.cancer', 'b.avg_beta')
    )

mean_by_cpg = mean_by_cpg.withColumn("sum_squared_between_groups",
                                     ((mean_by_cpg.normal - mean_by_cpg.avg_beta)**2
                                      + (mean_by_cpg.cancer - mean_by_cpg.avg_beta)**2))
mean_by_cpg = mean_by_cpg.orderBy('sum_squared_between_groups', ascending=False)

top_cpg = mean_by_cpg.select('CpG_probe_id').limit(5000)
top_cpg = top_cpg.rdd.map(lambda x: x.CpG_probe_id)


# Save to GCS
top_cpg.repartition(12).saveAsTextFile('gs://build_hackathon_dnanyc/columns_to_keep')
