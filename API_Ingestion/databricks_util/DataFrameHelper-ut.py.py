# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
print('clean(string)')
def clean(string):
#   return string.replace(" ","_").replace(".","_").replace("[","").replace("]","").replace("(","").replace(")","").replace("__","_") #Add replace for a carriage return in this function
  return string.replace(" ","_").replace(".","_").replace("[","").replace("]","").replace("(","").replace(")","").replace("__","_").replace("\r","")
  


print('cleanDFColumns(df)')
def cleanDFColumns(df):
  cols = df.columns
  newColumns = [clean(col) for col in cols]
#   df.toDF(*newColumns)
#   pandas_df = df.toPandas()
#   pandas_df.columns = pandas_df.columns.str.replace('\r', '') #need not convert to a pandas df
#   return spark.createDataFrame(pandas_df)
  return df.toDF(*newColumns)
  
import pyspark.sql.functions as f

print('add_md(df,sourceName)')
def add_md(df,sourceName):
  #df = df.withColumn('md_RowCount', f.lit(int(df.count())))
  df = df.withColumn('md_Date', f.current_timestamp().cast('timestamp')+ f.expr('INTERVAL 8 HOURS'))
  df = df.withColumn('SourceSystem', f.lit(sourceName))
  return df

print('def applySchema(df,schema)')
def applySchema(df,schema):
  for field in schema:
      df = df.withColumn(field.name,trim(df[field.name]).cast(field.dataType))
  return df


print('dedupeDelta(delta_df,keys)')
def dedupeDelta(delta_df,keys):
  w = Window.partitionBy(keys)
  delta_df = delta_df.withColumn('maxJOTSTP', max(trim(col('JOTSTP'))).over(w))\
      .withColumn('maxJOSEQN',max(trim(col('JOSEQN'))).over(w))\
      .where(trim(col('JOTSTP')) == col('maxJOTSTP'))\
      .where(trim(col('JOSEQN')) == col('maxJOSEQN'))\
      .drop('maxJOTSTP')\
      .drop('maxJOSEQN')
  
  return delta_df



#Replace NAN with null
print('NanToNull(df)')
def NanToNull(df):
  columns = df.columns
  for column in columns:
    if column != 'md_Date':  # to Prevent error : "cannot resolve 'isnan(md_Date)' due to data type mismatch: argument 1 requires (double or float) type, however, 'md_Date' is of timestamp type."
      df= df.withColumn(column,when(isnan(col(column)),None).otherwise(col(column)))
  return df


# CHANGE STRING COLUMNS TO INTEGER
print('String_to_Integer(df, columns)')
def String_to_Integer(df, columns):
  for column in columns:
    df = df.withColumn(column,col(column).cast(IntegerType()))
  return df


# CHANGE STRING TO TimeStamp (Converts yyyy-MM-ddThh:mm:ss string format to TimeStamp format)
print('String_to_TimeStamp(df, columns) - Converts yyyy-MM-ddThh:mm:ss string format to TimeStamp format')
def String_to_TimeStamp(df, columns):
  for column in columns:
    df = df.withColumn(column,to_timestamp(col(column)))
  return df


# CHANGE 'MM/dd/yyyy' STRING TO Date ( Converts MM/dd/yyyy string format to yyyy-MM-dd date format)
print('dateString_to_Date(df, columns) - Converts MM/dd/yyyy string format to yyyy-MM-dd date format')
def dateString_to_Date(df, columns):
  for column in columns:
    df = df.withColumn(column,to_date(from_unixtime(unix_timestamp(col(column),'MM/dd/yyyy'))))
  return df


# CHANGE 'yyyy-MM-ddThh:mm:ss' STRING TO Date ( Converts yyyy-MM-ddThh:mm:ss string format to yyyy-MM-dd date format)
print('timestampString_to_Date(df, columns) - Converts yyyy-MM-ddThh:mm:ss string format to yyyy-MM-dd date format')
def timestampString_to_Date(df, columns):
  for column in columns:
    df = df.withColumn(column,to_date(col(column)))
  return df

# COMMAND ----------

# DBTITLE 1,Functions to convert pandas DataFrame to spark DataFrame 
# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    elif pandasType == 'bytes': return ByteType()
    else: return StringType()

def define_structure(colName, dataType):
    try: typo = equivalent_type(dataType)
    except: typo = StringType()
    return StructField(colName, typo)

# Given pandas dataframe, it will return a spark's dataframe.
print('pandas_to_spark(pandas_df)')
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)
