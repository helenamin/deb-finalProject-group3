# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
from sklearn.linear_model import LinearRegression
import xgboost as xgb
import mlflow
import mlflow.xgboost
import mlflow.sklearn

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the Table

# COMMAND ----------

df = spark.sql('''select polExternalID, transportMode, orderType, orderEntrydate, orderedQuantity, shippedPGIDate, carrierCode, estimatedShipDate as ArrivalDate 
                  from hive_metastore.dbt_gold.factparts
                  where transportMode is not Null
                  and estimatedShipDate is not null;''').toPandas()
df

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Data Pre Processing

# COMMAND ----------

# Convert the Date column data types from sting to Date

df['orderEntrydate'] = pd.to_datetime(df['orderEntrydate'], format="%Y/%m/%d")
df['shippedPGIDate'] = pd.to_datetime(df['shippedPGIDate'], format="%Y/%m/%d")
df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'], format="%Y/%m/%d")
df

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data Quality Checks

# COMMAND ----------

df[df['orderEntrydate'] > df['shippedPGIDate']]

# COMMAND ----------

df = df[~(df['shippedPGIDate'] > df['ArrivalDate'])]
df.reset_index(inplace = True, drop = True)
df

# COMMAND ----------

# Create new columsn to cacluate the number of days taken to Ship and Delivery

df['DaysToShip'] = (df['shippedPGIDate'] - df['orderEntrydate']).dt.days
df['DaysToDelivery'] = (df['ArrivalDate'] - df['orderEntrydate']).dt.days
df

# COMMAND ----------

df_val = df.iloc[-10000:]
df_val

# COMMAND ----------

df_final = df.iloc[:-10000]
df_final

# COMMAND ----------

sel_cols = ['transportMode', 'orderType',  'orderedQuantity', 'DaysToShip', 'DaysToDelivery']

# COMMAND ----------

df_final = df_final[['transportMode', 'orderType',  'orderedQuantity', 'DaysToShip', 'DaysToDelivery']]
df_final

# COMMAND ----------

# Convert the categorical values to Integers aka One Hot Encoding 
trans_dict = {'TRUCK': 1, 'AIR':2}
order_dict= {'STOCK': 1,'CPRO' : 2, 'FDO' : 3,'EMERGENCY':4, 'WARRANTY':5, 'MEGA':6, 'DOWN':7, 'LCPRO':8}

# COMMAND ----------

df_final['transportMode'] = df_final['transportMode'].map(trans_dict)
df_final['orderType'] = df_final['orderType'].map(order_dict)


# COMMAND ----------

# Split the data set for trainig and testing 
X, y = df_final[['transportMode', 'orderType',  'orderedQuantity', 'DaysToShip']], df_final['DaysToDelivery']

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# COMMAND ----------

import xgboost as xgb

# define model
#model = xgb.XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8, enable_categorical = True)

train_dmatrix = xgb.DMatrix(data = X_train, label = y_train)
test_dmatrix = xgb.DMatrix(data = X_test, label = y_test )

# COMMAND ----------

# Parameter dictionary specifying base learner
param = {"booster":"gblinear", "objective":"reg:squarederror"}

# COMMAND ----------

from sklearn.metrics import mean_squared_error as MSE

xgb_model = xgb.train(params = param, dtrain = train_dmatrix, num_boost_round = 20)
pred = xgb_model.predict(test_dmatrix)
 
# RMSE Computation
rmse = np.sqrt(MSE(y_test, pred))
print("RMSE : % f" %(rmse))

# COMMAND ----------

#xgb_model.save_model("Partsviz.pkl")
#mlflow.sklearn.log_model(xgb_model, "Partsviz_Model")

# COMMAND ----------

y_test.to_frame()

# COMMAND ----------

predicted_df = pd.DataFrame(data=pred, columns=['PredictedDaysForDelivery'], 
                            index=X_test.index.copy())

# COMMAND ----------


df_out = pd.merge(X_test,predicted_df, how = 'left',left_index = True, right_index = True)
df_out = pd.merge(df_out[['PredictedDaysForDelivery']], df[['polExternalID', 'transportMode', 'orderType', 'orderedQuantity', 'DaysToShip']], how = 'left',left_index = True, right_index = True)
df_out['Prediction_Datetime']= pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
df_out

# COMMAND ----------

# spark_df = spark.createDataFrame(df_out)
# spark_df.write.mode("overwrite").saveAsTable("dbt_gold.PartsvizDeliveryPrediction")

# COMMAND ----------


