# Databricks notebook source
dbutils.widgets.text('dstAccount','','')

# COMMAND ----------

parameters = dict(
     dstAccName = dbutils.widgets.get('dstAccount')
)

# COMMAND ----------

# MAGIC %run ./databricks_util/DataFrameHelper-ut.py

# COMMAND ----------

# MAGIC %run ./databricks_util/DataLakeHelper-ut.py

# COMMAND ----------

# MAGIC %run ./databricks_util/OAuthAPIHelper

# COMMAND ----------

user = dbutils.secrets.get(scope='azure-key-vault',key="PartsVizUser") 
password = dbutils.secrets.get(scope='azure-key-vault',key="PartsViz")
token_url = dbutils.secrets.get(scope='azure-key-vault',key="PartsVizTokenUrl")
API_url = dbutils.secrets.get(scope='azure-key-vault',key='PartsVizUrl')

token = getOAuthToken(user,password,token_url)

# COMMAND ----------

import requests
import json
import pandas as pd
from dateutil.relativedelta import *
from datetime import *
from dateutil import parser
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

#returns todays date in %Y-%m-%dT00:00:00 UTC
strTodayDt = f"{datetime.today().strftime('%Y-%m-%d')}T00:00:00" 
#Converts today to datetime object  endDt for the date range
todayDt = datetime.strptime(strTodayDt,"%Y-%m-%dT%H:%M:%S")

fromDt = todayDt - relativedelta(days=2)
toDt = fromDt + relativedelta(days=1)

todayDate = f"{todayDt.strftime('%Y%m%d')}"
dealerCode = 'T080'
dstAcctName = parameters['dstAccName']
entity = 'dbtPartsViz'
firstRecordID = ''

# COMMAND ----------

def getPartsVizData(API_url,token,dealerCode,fromDateTime,toDateTime,orderStatus,lastRecordID):
  url = f"{API_url}/?dealerCode={dealerCode}&fromDateTime={fromDateTime}&toDateTime={toDateTime}&orderStatus={orderStatus}&lastRecordID={str(lastRecordID)}&includePurgedRows=true"
  payload={}
  headers = {
  'Authorization':'Bearer '+token
  }
  
  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 200:
    return json.loads(response.text)
  else:
    return f"An error has occured -{response.status_code}!"

# COMMAND ----------

def saveToRaw(dstAcctName,entity,df):
  dataLakeConnect(dstAcctName)
  rawpath = dataLakePath(dstAcctName,"raw",entity,f"{todayDate}/{entity}.snappy.parquet")
  dataLakeWriteParquet(df,rawpath)
  print(f"Data for {todayDate} saved to {rawpath}")

# COMMAND ----------

def writeToDest(dstAcctName,entity):
  rawpath = dataLakePath(dstAcctName,"raw",entity,f"{todayDate}/{entity}.snappy.parquet")
  writtenData = dataLakeReadParquet(rawpath)
  destinationPath = dataLakePath(dstAcctName,'bronze','dbt_bronze','parts_tmp')
  writtenData.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbt_bronze.parts_tmp",path=destinationPath)


# COMMAND ----------

def voidtype_detection(idf):
  cols = list(idf.columns)
  types = list(idf.dtypes)
 
  for column, data_type in zip(cols, types):
    if "void" in str(data_type):
      print(f"Detected - {data_type}")
      column_str = str(column)
      idf = idf.withColumn(column_str, col(column_str).cast(StringType()))
  
  return idf

# COMMAND ----------

fromDtSave = fromDt
orderStatus = 'Open'
lastRecordID = firstRecordID

while fromDt <= todayDt:
  toDt = fromDt + relativedelta(days=1)
  fromDateTime = f"{fromDt.strftime('%Y-%m-%d')}T00:00:00Z"
  toDateTime =   f"{toDt.strftime('%Y-%m-%d')}T00:00:00Z"
  print(f"Fetching Data For: {fromDateTime} - {toDateTime}")
  # For insertDateTime
  currentDt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') 
  nowDt = datetime.strptime(currentDt,"%Y-%m-%dT%H:%M:%S") + relativedelta(hours = 8)
  insertDt = nowDt.strftime("%Y-%m-%dT%H:%M:%S")
  err = None
  while err is None:
    print(lastRecordID)
    try:
      json_data = getPartsVizData(API_url,token,dealerCode,fromDateTime,toDateTime,orderStatus,lastRecordID)
      pdf = pd.json_normalize(json_data['partOrderLines'])
      pdf['catOrderStatus'] = orderStatus
      pdf['insertDatetime'] = insertDt
      if lastRecordID == '':
        df = spark.createDataFrame(pdf)
      else:
        df = df.union(spark.createDataFrame(pdf))
      lastRecordID = json_data['lastRecordID']
    except:
      err = 'an error has happened'
      pass
  fromDt = fromDt + relativedelta(days=1)

fromDt = fromDtSave
orderStatus = 'Closed'
lastRecordID = firstRecordID
 
while fromDt <= todayDt:
  toDt = fromDt + relativedelta(days=1)
  currentDt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') 
  fromDateTime = f"{fromDt.strftime('%Y-%m-%d')}T00:00:00Z"
  toDateTime =   f"{toDt.strftime('%Y-%m-%d')}T00:00:00Z"
  # For insertDateTime
  currentDt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') 
  nowDt = datetime.strptime(currentDt,"%Y-%m-%dT%H:%M:%S") + relativedelta(hours = 8)
  insertDt = nowDt.strftime("%Y-%m-%dT%H:%M:%S")
  err = None
  while err is None:
    print(lastRecordID)
    try:
      json_data = getPartsVizData(API_url,token,dealerCode,fromDateTime,toDateTime,orderStatus,lastRecordID)
      pdf = pd.json_normalize(json_data['partOrderLines'])
      pdf['catOrderStatus'] = orderStatus
      pdf['insertDatetime'] = insertDt
      df = df.union(spark.createDataFrame(pdf))
      lastRecordID = json_data['lastRecordID']
    except:
      err = 'an error has happened'
      pass
  fromDt = fromDt + relativedelta(days=1)

df1 = voidtype_detection(df)
 
saveToRaw(dstAcctName,entity,df1)

# COMMAND ----------

writeToDest(dstAcctName,entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into dbt_bronze.parts tgt using
# MAGIC (select * from dbt_bronze.parts_tmp) as src
# MAGIC on tgt.polExternalID = src.polExternalID
# MAGIC when matched then update
# MAGIC  set 
# MAGIC  tgt.transportMode                 = src.transportMode                
# MAGIC ,tgt.trackingNumber                 = src.trackingNumber                
# MAGIC ,tgt.status                         = src.status                        
# MAGIC ,tgt.sourcedPartNumber             = src.sourcedPartNumber            
# MAGIC ,tgt.sourcedItemQuantity         = src.sourcedItemQuantity        
# MAGIC ,tgt.sourceCustomerCode             = src.sourceCustomerCode            
# MAGIC ,tgt.shippedPGIDate                 = src.shippedPGIDate                
# MAGIC ,tgt.shipId                         = src.shipId                        
# MAGIC ,tgt.reservePrimPSO              = src.reservePrimPSO
# MAGIC ,tgt.releaseItemQuantity         = src.releaseItemQuantity        
# MAGIC ,tgt.pso                         = src.pso                        
# MAGIC ,tgt.polExternalID                 = src.polExternalID                
# MAGIC ,tgt.pendingItemQuantity         = src.pendingItemQuantity
# MAGIC ,tgt.partNumber                     = src.partNumber                    
# MAGIC ,tgt.orderType                     = src.orderType                    
# MAGIC ,tgt.orderlineName                 = src.orderlineName                
# MAGIC ,tgt.orderEntryDate                 = src.orderEntryDate                
# MAGIC ,tgt.orderedQuantity             = src.orderedQuantity            
# MAGIC ,tgt.modifiedDateTime             = src.modifiedDateTime            
# MAGIC ,tgt.materialRequestQuantity     = src.materialRequestQuantity    
# MAGIC ,tgt.lastActivityDateTime         = src.lastActivityDateTime        
# MAGIC ,tgt.isTransferOrder             = src.isTransferOrder
# MAGIC ,tgt.isTrackingActive             = src.isTrackingActive            
# MAGIC ,tgt.isPurged                    = src.isPurged
# MAGIC ,tgt.invoiceReferenceNumber         = src.invoiceReferenceNumber        
# MAGIC ,tgt.invoiceNumber                 = src.invoiceNumber                
# MAGIC ,tgt.invoicedItemQuantity         = src.invoicedItemQuantity        
# MAGIC ,tgt.huNumber                     = src.huNumber                    
# MAGIC ,tgt.facilityPSO                 = src.facilityPSO                
# MAGIC ,tgt.facilityName                 = src.facilityName                
# MAGIC ,tgt.estimatedShipDate             = src.estimatedShipDate            
# MAGIC ,tgt.customerSuppliedItemNumber     = src.customerSuppliedItemNumber
# MAGIC ,tgt.customerReference             = src.customerReference            
# MAGIC ,tgt.customerCode                 = src.customerCode                
# MAGIC ,tgt.crmBillNumber                 = src.crmBillNumber                
# MAGIC ,tgt.createdDateTime             = src.createdDateTime            
# MAGIC ,tgt.containerNumber             = src.containerNumber            
# MAGIC ,tgt.carrierName                 = src.carrierName                
# MAGIC ,tgt.carrierCode                 = src.carrierCode            
# MAGIC ,tgt.airwayBillNumber            = src.airwayBillNumber
# MAGIC ,tgt.catOrderStatus                 = src.catOrderStatus        
# MAGIC ,tgt.customerStorageLocation     = src.customerStorageLocation  -- below onwards are new columns added on the 16/06/2022 PartsViz API Update
# MAGIC ,tgt.keyCatCustomerCode          = src.keyCatCustomerCode
# MAGIC ,tgt.profile                     = src.profile
# MAGIC ,tgt.relatedCustomer             = src.relatedCustomer
# MAGIC ,tgt.shipByDate                  = src.shipByDate
# MAGIC ,tgt.changedDateTime             = src.insertDateTime
# MAGIC when not matched then insert
# MAGIC ( transportMode                     
# MAGIC ,trackingNumber                     
# MAGIC ,status                             
# MAGIC ,sourcedPartNumber                 
# MAGIC ,sourcedItemQuantity         
# MAGIC ,sourceCustomerCode                 
# MAGIC ,shippedPGIDate                     
# MAGIC ,shipId                             
# MAGIC ,reservePrimPSO
# MAGIC ,releaseItemQuantity         
# MAGIC ,pso                         
# MAGIC ,polExternalID                     
# MAGIC ,pendingItemQuantity
# MAGIC ,partNumber                         
# MAGIC ,orderType                         
# MAGIC ,orderlineName                     
# MAGIC ,orderEntryDate                     
# MAGIC ,orderedQuantity             
# MAGIC ,modifiedDateTime             
# MAGIC ,materialRequestQuantity     
# MAGIC ,lastActivityDateTime         
# MAGIC ,isTransferOrder
# MAGIC ,isTrackingActive             
# MAGIC ,isPurged
# MAGIC ,invoiceReferenceNumber             
# MAGIC ,invoiceNumber                     
# MAGIC ,invoicedItemQuantity         
# MAGIC ,huNumber                     
# MAGIC ,facilityPSO                 
# MAGIC ,facilityName                 
# MAGIC ,estimatedShipDate                 
# MAGIC ,customerSuppliedItemNumber     
# MAGIC ,customerReference                 
# MAGIC ,customerCode                 
# MAGIC ,crmBillNumber                     
# MAGIC ,createdDateTime             
# MAGIC ,containerNumber             
# MAGIC ,carrierName                 
# MAGIC ,carrierCode
# MAGIC ,airwayBillNumber
# MAGIC ,catOrderStatus
# MAGIC ,customerStorageLocation -- new columns added (16/06/2022)
# MAGIC ,keyCatCustomerCode
# MAGIC ,profile
# MAGIC ,relatedCustomer
# MAGIC ,shipByDate
# MAGIC ,insertDateTime
# MAGIC )
# MAGIC values
# MAGIC ( src.transportMode            
# MAGIC ,src.trackingNumber            
# MAGIC ,src.status                    
# MAGIC ,src.sourcedPartNumber        
# MAGIC ,src.sourcedItemQuantity        
# MAGIC ,src.sourceCustomerCode        
# MAGIC ,src.shippedPGIDate            
# MAGIC ,src.shipId                    
# MAGIC ,src.reservePrimPSO
# MAGIC ,src.releaseItemQuantity        
# MAGIC ,src.pso                        
# MAGIC ,src.polExternalID            
# MAGIC ,src.pendingItemQuantity
# MAGIC ,src.partNumber                
# MAGIC ,src.orderType                
# MAGIC ,src.orderlineName            
# MAGIC ,src.orderEntryDate            
# MAGIC ,src.orderedQuantity            
# MAGIC ,src.modifiedDateTime            
# MAGIC ,src.materialRequestQuantity    
# MAGIC ,src.lastActivityDateTime        
# MAGIC ,src.isTransferOrder
# MAGIC ,src.isTrackingActive            
# MAGIC ,src.isPurged
# MAGIC ,src.invoiceReferenceNumber    
# MAGIC ,src.invoiceNumber            
# MAGIC ,src.invoicedItemQuantity        
# MAGIC ,src.huNumber                    
# MAGIC ,src.facilityPSO                
# MAGIC ,src.facilityName                
# MAGIC ,src.estimatedShipDate        
# MAGIC ,src.customerSuppliedItemNumber
# MAGIC ,src.customerReference        
# MAGIC ,src.customerCode                
# MAGIC ,src.crmBillNumber            
# MAGIC ,src.createdDateTime            
# MAGIC ,src.containerNumber            
# MAGIC ,src.carrierName                
# MAGIC ,src.carrierCode
# MAGIC ,src.airwayBillNumber
# MAGIC ,src.catOrderStatus
# MAGIC ,src.customerStorageLocation
# MAGIC ,src.keyCatCustomerCode
# MAGIC ,src.profile
# MAGIC ,src.relatedCustomer
# MAGIC ,src.shipByDate
# MAGIC ,src.insertDateTime
# MAGIC );
