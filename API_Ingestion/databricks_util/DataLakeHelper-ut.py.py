# Databricks notebook source
print('dataLakeConnect(accountName)')
def dataLakeConnect(accountName):
    keyName = "{0}-account-key".format(accountName)
    accountKey = dbutils.secrets.get(scope='azure-key-vault',key=keyName)
    spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(accountName), accountKey)
  
print('dataLakePath(accountName,dataLakeZone,folder="",dataSetName="")')
def dataLakePath(accountName,dataLakeZone,folder="",dataSetName=""):
  dsn = ("{0}/{1}".format(folder,dataSetName) if (folder and dataSetName) else 
         folder if (folder and not dataSetName) else 
         "")
  return "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(dataLakeZone,accountName,dsn)


#Add parameter for escape character to avoid problems with text fields which terminate with a backslash.  
#For example in pcpprms0, sos 160, part 160CTR-200895 the source data HOSE ASSY,1"x42 1\ appears in raw zone as "HOSE ASSY,1\"x42 1\" 
# when datalakeReadCsv treats the final \ as an escape for the double quote terminating the field, 
# so treats the following characters up to the next quote as part of the same field.
#ADLS_raw Dataset has been changed to use ϡ as the escape character.    
print('dataLakeReadCsv(inferSchema="true",header="true",sep="|",dataLakePath="")')
def dataLakeReadCsv(inferSchema="false",header="true",sep="|",dataLakePath="",escape="\\"):
  if dataLakePath=="":
    print("Please provide valid options")
    return
  return spark.read.format("csv").options(inferSchema=inferSchema, header=header,sep=sep,multiline="true",escape=escape).load(dataLakePath)

print('dataLakeWriteCsv(df,header="true",sep="|",dataLakePath="")')
def dataLakeWriteCsv(df,header="true",sep="|",dataLakePath=""):
  if dataLakePath=="":
    print("Please provide valid options")
    return
  df.repartition(1).write.csv(dataLakePath,mode="overwrite",header=header,sep=sep,quoteAll="true",escapeQuotes="false")

print('dataLakeReadParquet(dataLakePath="")')
def dataLakeReadParquet(dataLakePath=""):
  if dataLakePath=="":
    print("Please provide valid options")
    return
  return spark.read.parquet(dataLakePath)
  
print('dataLakeWriteParquet(df,dataLakePath="")')
def dataLakeWriteParquet(df,dataLakePath=""):
  if dataLakePath=="":
    print("Please provide valid options")
    return
  df.write.parquet(dataLakePath,mode="overwrite")

print('dataLakeAppendParquet(df,dataLakePath="")')
def dataLakeAppendParquet(df,dataLakePath=""):
  if dataLakePath=="":
    print("Please provide valid options")
    return
  df.write.parquet(dataLakePath,mode="append")
  
print('dataLakeDeltaOverwrite(dataFrame,path,tableName,dbName="STD")')
def dataLakeDeltaOverwrite(dataFrame,path,tableName,dbName="STD"):
  # Check if datatabase exists, if not create it
  # incoming dbName should be STD_<ProcessType>
  df = spark.sql("SHOW DATABASES")
  rows = df.collect()
  dbs = [r.databaseName for r in rows]
  if dbName not in dbs:
    spark.sql(f"create database IF NOT EXISTS {dbName}")
    print(f"Database {dbName} created")
  
  #Save the df to the dataframe
  dataFrame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("{0}.{1}".format(dbName,tableName),path=path)
  

print('dataLakeDeltaAppend(dataFrame,path,tableName,dbName="STD")')
def dataLakeDeltaAppend(dataFrame,path,tableName,dbName="STD"):
  dataFrame.write.format("delta").mode("append").saveAsTable("{0}.{1}".format(dbName,tableName),path=path)
