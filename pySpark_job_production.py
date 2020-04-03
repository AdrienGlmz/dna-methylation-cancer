import pandas as pd
import pandas_gbq as pd_bq
from google.cloud import bigquery as bq
from datetime import datetime as dt
from time import sleep
import pyspark
import json
from pyspark.sql.functions import col
import numpy as np
from pyspark.sql.types import StringType



global sc, project_id, bqClient, session, staging_directory, staging_directory_claims
project_id = 'youtubebq-prod'
bqClient = bq.Client(project=project_id)
session = pyspark.sql.SparkSession.builder.master("yarn-client").appName("testing").getOrCreate()
session._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version","2")
session.conf.set("spark.sql.shuffle.partitions", "2500")
staging_directory = "gs://youtubebq_prod/staging_data/"
staging_directory_claims = "gs://youtubebq_prod/staging_data_claims/"
sc = session.sparkContext

def checkForNewDates():
  query_joinedReady = "SELECT DatesPulled , DatesToS3 FROM `youtubebq-prod.Logging.Assets_MasterLog`"
  query_joinedReady_results = pd_bq.read_gbq(query_joinedReady, project_id, dialect='standard')
  datesToSend = list(set(query_joinedReady_results['DatesPulled'].values) - set(query_joinedReady_results['DatesToS3'].values))
  if not datesToSend == [] : 
    print("Sending data for dates : {}".format(datesToSend))
    sendData(datesToSend)


def checkForNewDates_claims():
  query_claims = "SELECT max(ClaimsDate) as ClaimsDate  FROM `youtubebq-prod.Logging.Claims_MasterLog`"
  query_newClaims = "SELECT max(_LATEST_DATE) as latestDate FROM `youtubebq-prod.Youtube.content_owner_active_claims_a1_wmg`"
  query_result = pd_bq.read_gbq(query_claims, project_id, dialect='standard')
  queryNew_result = pd_bq.read_gbq(query_newClaims, project_id, dialect='standard')
  currentDate = query_result['ClaimsDate'].values[0]
  np_datetime = queryNew_result['latestDate'].values[0]
  newDate = pd.to_datetime(np_datetime).strftime('%Y%m%d')
  if currentDate == None : 
    sendClaimsData(newDate)
  else : 
    if newDate > currentDate:
      sendClaimsData(newDate)
  

def sendClaimsData(newDate):
  nameOfTempTable = 'temporary_claims'
  dset_ref = bqClient.dataset("Youtube")
  table_ref = dset_ref.table(nameOfTempTable)
  queryOption_obj = bq.job.QueryJobConfig()
  queryOption_obj.allow_large_results = True
  queryOption_obj.create_disposition = "CREATE_IF_NEEDED"
  queryOption_obj.destination = table_ref
  queryOption_obj.use_legacy_sql = False
  queryOption_obj.write_disposition = "WRITE_TRUNCATE"
  queryOption_obj.default_dataset = dset_ref
  queryJob_Id = "moveAssets_{}".format(dt.now().strftime('%Y%m%dT%H%m%s'))
  query_move = "SELECT * FROM `youtubebq-prod.Youtube.content_owner_active_claims_a1_wmg` WHERE _LATEST_DATE = _DATA_DATE"
  queryJob = bqClient.query(query_move, job_config = queryOption_obj, job_id=queryJob_Id)
  while not queryJob.done():
      print("job not done yet")
      sleep(5)
  if queryJob.done():
      print("finished!")
  table_conf = {}
  table_conf["mapred.bq.input.project.id"]="youtubebq-prod"
  table_conf["mapred.bq.input.dataset.id"]="Youtube"
  table_conf["mapred.bq.input.table.id"]= nameOfTempTable
  table_conf['mapred.bq.project.id']= project_id
  table_conf['mapred.bq.gcs.bucket']= 'youtubebq_prod'
  table_conf['mapred.bq.temp.gcs.path']= staging_directory_claims
  table_conf["mapred.bq.input.sharded.export.enable"] = "false"
  claims_Df = rddToDf(table_conf)
  claims_Df.cache()
  print("Beginning write to S3 ")
  claims_Df.write.partitionBy("_DATA_DATE").csv("s3a://youtubebq-reports/claims/",compression='gzip',mode='append')
  claims_Df.unpersist()
  input_path = sc._jvm.org.apache.hadoop.fs.Path(staging_directory_claims)
  input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path,True) 
  updateClaims_Log(newDate)   


def updateClaims_Log(newDate):
  temp_dict = {"ClaimsDate" : [newDate], "timeLogged" : [dt.now().isoformat() ]}
  df = pd.DataFrame(temp_dict)
  write_results = pd_bq.to_gbq(df,"Logging.Claims_MasterLog",project_id, if_exists='append')



def sendData(datesToSend):
  table_conf = {}
  table_conf["mapred.bq.input.project.id"]="youtubebq-prod"
  table_conf["mapred.bq.input.dataset.id"]="Youtube"
  table_conf["mapred.bq.input.table.id"]="p_content_owner_asset_combined_a2_wmg"   # POINT TO content_owner_assets_combined_a2
  table_conf['mapred.bq.project.id']= project_id
  table_conf['mapred.bq.gcs.bucket']= 'youtubebq_prod'
  table_conf['mapred.bq.temp.gcs.path']= staging_directory
  table_conf["mapred.bq.input.sharded.export.enable"] = "false"
  assets_Df = rddToDf(table_conf)
  schema_ = assets_Df.schema 
  dateDf = assets_Df.filter(assets_Df.date.isin(datesToSend))    # NEW LINE OF CODE!!! CHECK TO SEE IF IT WORKS!!
  dateDf.cache()
  counts_dict, totalCount = getCountryCodeCounts(dateDf)
  ratio_dict = getRatioDict(counts_dict,totalCount)
  global partition_bv
  partition_bv, numOfPartitions = broadCast_partitionDict(ratio_dict)
  print("Starting to key RDD  \n \n ")
  keyedRdd = dateDf.rdd.keyBy(lambda x : x["country_code"])
  print("Finished keying RDD  \n \n ")
  print("Starting to repartition RDD \n \n ")
  newRdd = keyedRdd.partitionBy(numOfPartitions,partKeyCreator).map(backTodict)
  print("Finished repartition-ing RDD  \n \n ")
  final_df = newRdd.toDF(schema_)
  print("Beginning write to S3 ")
  final_df.write.partitionBy("date","country_code").csv("s3a://youtubebq-reports/assets/",compression='gzip',mode='append')
  dateDf.unpersist()
  partition_bv.unpersist()
  del(dateDf,partition_bv)
  assets_Df.unpersist()
  input_path = sc._jvm.org.apache.hadoop.fs.Path(staging_directory)
  input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path,True)
  sleep(10)
  try :
    checkForNewDates_claims()
  except Exception as e:
    print("checking for claims with error : {}".format(e))
  finally : 
    update_Log(datesToSend)
    


def update_Log(datesToSend):
  for date in datesToSend:
    query_updateLog = "UPDATE Logging.Assets_MasterLog SET DatesToS3 = '{1}', logEntryTime_toS3 = '{0}' WHERE DatesPulled = '{1}'".format(dt.now().isoformat(), date)
    bqClient.query(query_updateLog) 
    sleep(5)


def deleteData():
  table_iter = bqClient.list_tables('youtubebq-prod.Youtube')
  table_list = []
  for row in table_iter: 
    table_list.append(row.table_id)
  else : 
    None 
  for table in table_list:
    print("deleting : {}".format(table))
    bqClient.delete_table("youtubebq-prod.Youtube.{}".format(table))
    sleep(5)
  else : 
    exit()
  





def backTodict(x):
  temp = {}
  temp = x[1].asDict()
  temp.update({"country_code":x[0]})
  return temp


def partKeyCreator(cc):
  global partition_bv
  bucket_list = partition_bv.value[cc]
  if len(bucket_list) == 1 :
    bucket_id = bucket_list[0]
  elif len(bucket_list)>1 : 
    bucket_id = np.random.randint(bucket_list[0], bucket_list[-1]+1)
  else : 
    bucket_id = 1
  return bucket_id



def broadCast_partitionDict(ratio_dict):
  cnt = 0
  ratio_list = {}
  for cc, num in ratio_dict.items():
    if num == 1:
      ratio_list[cc] = [cnt]
      cnt += num
    else : 
      ratio_list[cc] = list(range(cnt,cnt+num))
      cnt += num
  else : 
    ratio_bv = session.sparkContext.broadcast(ratio_list)
    numOfPartitions = cnt
    return ratio_bv, numOfPartitions




def getCountryCodeCounts(dateDf):
  counts = dateDf.groupBy("country_code").count().collect()
  totalCount = dateDf.count()
  counts_dict = {}
  for row in counts : 
    dict_ = row.asDict()
    counts_dict[dict_["country_code"]] = int(dict_["count"])
  else : 
    return counts_dict, totalCount

def getRatioDict(counts_dict,totalCount):
  ratio_dict = {}
  for cc, count in counts_dict.items():
    pct = int((count/totalCount)*100)
    ratio_dict[cc] = 1 if pct == 0 else pct*35
  else :
    return ratio_dict




def rddToDf(table_conf):
  sc = session.sparkContext
  data_table = sc.newAPIHadoopRDD('com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat','org.apache.hadoop.io.LongWritable','com.google.gson.JsonObject',conf=table_conf)
  json_data = data_table.map(lambda x : x[1])
  df_assets = session.read.json(json_data)
  print("Finished reading in bigquery table \n")
  # df_assets_datePart = df_assets.repartition(col("date"))
  return df_assets



if __name__ == '__main__':
  try : 
    checkForNewDates()
  except Exception as e: 
    print("Exception as {}".format(e))
    checkForNewDates_claims()
  finally : 
    print("deleting all tables from Youtube dataset!")
    deleteData()







