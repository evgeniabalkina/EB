# Databricks notebook source
from pyspark.sql import functions as F
import pyspark.sql.types
main_col = ['CTRYCODE', 'POSTCODE', 'NAME']

# COMMAND ----------

# MAGIC %md
# MAGIC ####Input Sociodemographics by BU

# COMMAND ----------

#dbutils.fs.rm("/FileStore/usr_shared_cat_analytics/Sociodemographics/maps")
# dbutils.fs.rm("/FileStore/usr_shared_cat_analytics/Sociodemographics/maps/MBI_SE_2.SHP")
# dbutils.fs.rm("/FileStore/usr_shared_cat_analytics/Sociodemographics/maps/MBI_SE_3.SHP")
# dbutils.fs.rm("/FileStore/usr_shared_cat_analytics/Sociodemographics/maps/MBI_SE_4.SHP")
# dbutils.fs.rm("/FileStore/usr_shared_cat_analytics/Sociodemographics/maps/MBI_SE_5.SHP")
#/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/maps/MBI_SE_1.DBF

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SE, DK, NO, IE, PL - not updated

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sweden

# COMMAND ----------

# # Loading MBI data by country
# # Sweden
# filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_SE_Postcodes5.csv'
# sd_SE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
#       .withColumnRenamed('ADMINCODE', 'POSTCODE')
#       )

# col_list_orig = sd_SE.columns
# col_list_new  = [c.replace("\r","") for c in col_list_orig]

# for c_orig, c_new in zip(col_list_orig, col_list_new):
#   sd_SE = sd_SE.withColumnRenamed(c_orig, c_new)


# COMMAND ----------

# if sd_SE.count() > sd_SE.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# #display(sd_SE.head(4))

# COMMAND ----------

# sd_SE = sd_SE.withColumn('POSTCODE', F.translate(F.col('POSTCODE'), " ", ""))

# COMMAND ----------

# if sd_SE.count() > sd_SE.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')
    
# #display(sd_SE.head(4))

# COMMAND ----------

#display(sd_SE.head(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Denmark

# COMMAND ----------

# #Denmark
# filePath_old = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_DK_Postcodes4.csv'
# filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_DK_Postcodes4_20221011.csv'

# sd_DK = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
#        .withColumnRenamed('ADMINCODE', 'POSTCODE')
#       )

# col_list_orig = sd_DK.columns
# col_list_new  = [c.replace("\r","") for c in col_list_orig]

# for c_orig, c_new in zip(col_list_orig, col_list_new):
#   sd_DK = sd_DK.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

# if sd_DK.count() > sd_DK.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Ireland

# COMMAND ----------

# #Ireland
# filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_IE_Postcodes3.csv'
# sd_IE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
#        .withColumnRenamed('ADMINCODE', 'POSTCODE')
#       )

# col_list_orig = sd_IE.columns
# col_list_new  = [c.replace("\r","") for c in col_list_orig]

# for c_orig, c_new in zip(col_list_orig, col_list_new):
#   sd_IE = sd_IE.withColumnRenamed(c_orig, c_new)
  
# # #Lithuania
# # filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_LT_Savivaldybes.csv'
# # sd_LT = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
# #        .withColumnRenamed('ADMINCODE', 'POSTCODE')
# #       )

# # #Latvia
# # filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_LV_Novadi.csv'
# # sd_LV = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
# #        .withColumnRenamed('ADMINCODE', 'POSTCODE')
# #       )

# COMMAND ----------

# if sd_IE.count() > sd_IE.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# from string import ascii_lowercase


# LETTERS = {letter: str(index) for index, letter in enumerate(ascii_lowercase, start=1)} 

# def alphabet_position(text):
#   try:
#     text = text.lower()
#     numbers = [LETTERS[character] for character in text if character in LETTERS]
#     output = "%s%s"%(*numbers, text[1:3])
#   except: 
#     output = text
#   return output

# COMMAND ----------

# convertUDF = F.udf(lambda text: alphabet_position(text))

# COMMAND ----------

# #sd_eur = sd_eur.withColumn('POSTCODE', F.translate(F.col('POSTCODE'), " -", ""))
# sd_IE = sd_IE.withColumn('POSTCODE', convertUDF(F.col('POSTCODE')))
# sd_IE = sd_IE.withColumn('POSTCODE', F.regexp_replace(F.col('POSTCODE'), "d6w", "99999"))

# COMMAND ----------

#display(sd_IE.head(4))

# COMMAND ----------

#display(sd_IE.filter(F.col('POSTCODE')=='99999'))

# COMMAND ----------

# if sd_IE.count() > sd_IE.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Norway

# COMMAND ----------

# #Norway
# filePath_old = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_NO_Postcodes4.csv'
# filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_NO_Postcodes4_20221011.csv'

# sd_NO = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
#        .withColumnRenamed('ADMINCODE', 'POSTCODE')
#       )

# col_list_orig = sd_NO.columns
# col_list_new  = [c.replace("\r","") for c in col_list_orig]

# for c_orig, c_new in zip(col_list_orig, col_list_new):
#   sd_NO = sd_NO.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

# if sd_NO.count() > sd_NO.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# convertUDFfill = F.udf(lambda text: text.zfill(4))

# COMMAND ----------

# sd_NO = sd_NO.withColumn("POSTCODE", F.col('POSTCODE').cast(StringType()))
# sd_NO = sd_NO.withColumn('POSTCODE', convertUDFfill(F.col('POSTCODE')))
# #display(sd_NO.filter(F.col('POSTCODE').like('%169%')))

# COMMAND ----------

# if sd_NO.count() > sd_NO.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Poland

# COMMAND ----------

# #Poland
# filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_PL_Postcodes5.csv'
# sd_PL = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
#        .withColumnRenamed('ADMINCODE', 'POSTCODE')
#       )

# col_list_orig = sd_PL.columns
# col_list_new  = [c.replace("\r","") for c in col_list_orig]

# for c_orig, c_new in zip(col_list_orig, col_list_new):
#   sd_PL = sd_PL.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

# if sd_PL.count() > sd_PL.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

#display(sd_PL.head(4))

# COMMAND ----------

#display(sd_PL.tail(4))

# COMMAND ----------

# sd_PL = sd_PL.withColumn('POSTCODE', F.translate(F.col('POSTCODE'), "-", ""))

# COMMAND ----------

# if sd_PL.count() > sd_PL.dropDuplicates(['POSTCODE']).count():
#     raise ValueError('Data has duplicates')

# COMMAND ----------

# sd_PL.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Estonia

# COMMAND ----------

#Estonia
filePath_old = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_EE_Omavalitsuste_20221011.csv'
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/alternative/MBI_MarketData_2021_EE_Postcodes5_20221011.csv'

sd_EE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
       .withColumnRenamed('ADMINCODE', 'POSTCODE')
      )

col_list_orig = sd_EE.columns
col_list_new  = [c.replace("\r","") for c in col_list_orig]

for c_orig, c_new in zip(col_list_orig, col_list_new):
  sd_EE = sd_EE.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

#display(sd_EE.head(25))

# COMMAND ----------

if sd_EE.count() > sd_EE.dropDuplicates(['POSTCODE']).count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

sd_EE.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Latvia

# COMMAND ----------

#Latvia
filePath_old = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_LV_Novadi_20221011.csv'
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/alternative/MBI_MarketData_2021_LV_Postcodes4_20221011.csv'
sd_LV = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
       .withColumnRenamed('ADMINCODE', 'POSTCODE')
      )

col_list_orig = sd_LV.columns
col_list_new  = [c.replace("\r","") for c in col_list_orig]

for c_orig, c_new in zip(col_list_orig, col_list_new):
  sd_LV = sd_LV.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

if sd_LV.count() > sd_LV.dropDuplicates(['POSTCODE']).count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

#display(sd_LV.head(5))

# COMMAND ----------

sd_LV.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lithuania

# COMMAND ----------

#Estonia
filePath_old = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/MBI_MarketData_2021_LT_Savivaldybes_20221011.csv'
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/alternative/MBI_MarketData_2021_LT_Postcodes5_20221011.csv'
sd_LT = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
       .withColumnRenamed('ADMINCODE', 'POSTCODE')
      )

col_list_orig = sd_LV.columns
col_list_new  = [c.replace("\r","") for c in col_list_orig]

for c_orig, c_new in zip(col_list_orig, col_list_new):
  sd_LT = sd_LT.withColumnRenamed(c_orig, c_new)

# COMMAND ----------

if sd_LT.count() > sd_LT.dropDuplicates(['POSTCODE']).count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

#display(sd_LT.head(5))

# COMMAND ----------

sd_LT.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge

# COMMAND ----------

# Finding the amount of common columns
sd = [
  #sd_SE.columns, sd_IE.columns, sd_PL.columns, sd_DK.columns, sd_NO.columns, 
      sd_EE.columns, sd_LV.columns, sd_LT.columns
     ]

common_cols = set(sd[0])
for s in sd[1:]:
    common_cols.intersection_update(s)


sd_common_cols = list(common_cols)

print(f'Number of common columns with data for all countries: {len(sd_common_cols)}')
print(sd_common_cols)

# COMMAND ----------

# Finding columns that fall out
all_cols = list(
  #set(sd_SE.columns) | set(sd_IE.columns) | set(sd_PL.columns) | set(sd_NO.columns) | set(sd_DK.columns)| 
  set(sd_EE.columns) | set(sd_LT.columns) | set(sd_LV.columns) 
                       )
falled_out_cols = list((
  #set(sd_SE.columns) | set(sd_IE.columns) | set(sd_PL.columns) | set(sd_NO.columns) | set(sd_DK.columns)| 
  set(sd_EE.columns) | set(sd_LT.columns) | set(sd_LV.columns) 
                       ) - set(sd_common_cols))

print(f'Number of columns with nulls for some BUs: {len(falled_out_cols)}')
print(falled_out_cols)

# COMMAND ----------

print(f'Number of all sociodemog data: {len(all_cols)}')

# COMMAND ----------

# Option will be available form the version 3.1.1 (will alow to have columns filled with nulls)
#sd_eur = sd_SE.unionByName(sd_EE, allowMissingColumns=True)

# COMMAND ----------

sd_list = {
  #'SE': sd_SE, 'IE': sd_IE, 'PL': sd_PL, 'DK': sd_DK, 'NO': sd_NO, 
  'EE': sd_EE, 'LV': sd_LV, 'LT': sd_LT}

sd_disct = {}
for bu in sd_list.keys():
  for column in [column for column in all_cols if column not in sd_list[bu].columns]:
    sd_list[bu] = sd_list[bu].withColumn(column, F.lit(None))
  #sd_disct[bu] = sd_list[bu]

# COMMAND ----------

# Union all the inputs bu BU
sd_eur = (sd_list['EE']
          #.unionByName(sd_list['IE'])
          #.unionByName(sd_list['PL'])
          #.unionByName(sd_list['DK'])
          #.unionByName(sd_list['NO'])
          .unionByName(sd_list['LV'])
          .unionByName(sd_list['LT'])
          #.unionByName(sd_list['SE'])
         )


# COMMAND ----------

#display(sd_eur.filter(F.col('CTRYCODE')=='LT'))

# COMMAND ----------

# # Union all the inputs bu BU
# sd_eur = (sd_SE.select(sd_common_cols)
#           #.unionByName(sd_EE.select(sd_common_cols))
#           .unionByName(sd_IE.select(sd_common_cols))
#           .unionByName(sd_PL.select(sd_common_cols))
#           #.unionByName(sd_LT.select(sd_common_cols))
#           #.unionByName(sd_LV.select(sd_common_cols))
#           .unionByName(sd_NO.select(sd_common_cols))
#           .unionByName(sd_DK.select(sd_common_cols))
#          )


# COMMAND ----------

if sd_eur.count() > sd_eur.dropDuplicates(['POSTCODE', 'CTRYCODE']).count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

x = sd_eur.count() - sd_eur.dropDuplicates(['POSTCODE']).count()
x

# COMMAND ----------

sd_eur = sd_eur.withColumn('POSTCODE', F.concat_ws('_',F.col('CTRYCODE'), F.col('POSTCODE')))

# COMMAND ----------

#display(sd_eur)

# COMMAND ----------

if sd_eur.count() > sd_eur.dropDuplicates(['POSTCODE']).count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

print(sd_eur.count())

# COMMAND ----------

BU_list = sd_eur.select('CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect()
print(f'Includes data for the following BUs: {BU_list}')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Correct datatypes

# COMMAND ----------

sd_eur_col = list(set(sd_eur.columns)-set(main_col))

# COMMAND ----------

#sd_eur_col

# COMMAND ----------

for c in sd_eur_col:
  sd_eur = sd_eur.withColumn(c, F.translate(F.col(c), "$, ", "").cast("double"))

# COMMAND ----------

#display(sd_eur)

# COMMAND ----------

#display(sd_eur.describe())

# COMMAND ----------

#sd_eur.printSchema()

# COMMAND ----------


