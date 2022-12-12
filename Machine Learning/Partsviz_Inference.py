# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
from sklearn.linear_model import LinearRegression
import xgboost as xgb

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load the batch data

# COMMAND ----------

df = spark.sql('''select polExternalID, transportMode, orderType, orderEntrydate, orderedQuantity, shippedPGIDate, carrierCode, estimatedShipDate as ArrivalDate 
                  from hive_metastore.dbt_gold.factparts
                  where transportMode is not Null
                  and estimatedShipDate is not null
                  and polExternalID not in (select polExternalID from hive_metastore.dbt_gold.PartsvizDeliveryPrediction)
                  and shippedPGIDate > (select max(Prediction_Datetime) from hive_metastore.dbt_gold.PartsvizDeliveryPrediction);''').toPandas()
df

# COMMAND ----------

df.shape[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Prep

# COMMAND ----------

df['orderEntrydate'] = pd.to_datetime(df['orderEntrydate'], format="%Y/%m/%d")
df['shippedPGIDate'] = pd.to_datetime(df['shippedPGIDate'], format="%Y/%m/%d")
df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'], format="%Y/%m/%d")
df

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###Data Quality checks

# COMMAND ----------

df[df['orderEntrydate'] > df['shippedPGIDate']]

# COMMAND ----------

df = df[~(df['shippedPGIDate'] > df['ArrivalDate'])]
df.reset_index(inplace = True, drop = True)
df

# COMMAND ----------

df['DaysToShip'] = (df['shippedPGIDate'] - df['orderEntrydate']).dt.days
#df['DaysToDelivery'] = (df['ArrivalDate'] - df['orderEntrydate']).dt.days
df

# COMMAND ----------

df_final = df[['transportMode', 'orderType',  'orderedQuantity', 'DaysToShip']]
df_final

# COMMAND ----------

trans_dict = {'TRUCK': 1, 'AIR':2}
order_dict= {'STOCK': 1,'CPRO' : 2, 'FDO' : 3,'EMERGENCY':4, 'WARRANTY':5, 'MEGA':6, 'DOWN':7, 'LCPRO':8}

# COMMAND ----------

df_final['transportMode'] = df_final['transportMode'].map(trans_dict)
df_final['orderType'] = df_final['orderType'].map(order_dict)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the model and make predictions

# COMMAND ----------

if df_final.shape[0] != 0:
  import mlflow
  logged_model = 'runs:/051adc0af04545238031b5a95f75f3ae/model'

  # Load model as a PyFuncModel.
  loaded_model = mlflow.pyfunc.load_model(logged_model)

  inf_dmatrix  = xgb.DMatrix(data = df_final)

  pred = loaded_model.predict(df_final)
  

  predicted_df = pd.DataFrame(data=pred, columns=['PredictedDaysForDelivery'], 
                            index=df.index.copy())


  df_out = pd.merge(df,predicted_df, how = 'left',left_index = True, right_index = True)
  df_out = pd.merge(df_out[['PredictedDaysForDelivery']], df[['polExternalID', 'transportMode', 'orderType', 'orderedQuantity',        'DaysToShip']], how = 'left',left_index = True, right_index = True)
  df_out['Prediction_Datetime']= pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
  df_out

  spark_df = spark.createDataFrame(df_out)
  spark_df.write.mode("append").saveAsTable("dbt_gold.PartsvizDeliveryPrediction")

else:
  numrows = df_final.shape[0]
  print(f"The size of the batch is {numrows}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


