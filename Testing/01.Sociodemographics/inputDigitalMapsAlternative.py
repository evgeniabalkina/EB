# Databricks notebook source
# from pyspark.sql import functions as F
# dbutils.library.installPyPI('geopandas')
# import geopandas as gps
# import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ####Input Sociodemographics by BU

# COMMAND ----------

#Loading postcodes digital shapes by country

#Estonia

pc_EE_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shpAlternative/Digital_Boundaries_EE/MBI_EE_4.SHP")

pc_EE = (pc_EE_4
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

#Latvia
pc_LV_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shpAlternative/Digital_Boundaries_LV/MBI_LV_4.SHP")

pc_LV = (pc_LV_4
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

#Lithuania
pc_LT_3 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shpAlternative/Digital_Boundaries_LT/MBI_LT_3.SHP")

pc_LT = (pc_LT_3
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Standardizing POSTCODES

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Estonia

# COMMAND ----------

#pc_EE.head(4)

# COMMAND ----------

assert pc_EE.POSTCODE.count() == pc_EE.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_EE.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Latvia

# COMMAND ----------

#pc_LV.head(4)

# COMMAND ----------

assert pc_LV.POSTCODE.count() == pc_LV.POSTCODE.nunique()
pc_LV.POSTCODE.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lithuania

# COMMAND ----------

#pc_LT.head(4)

# COMMAND ----------

assert pc_LT.POSTCODE.count() == pc_LT.POSTCODE.nunique()
pc_LT.POSTCODE.count()

# COMMAND ----------

pc_LT['POSTCODE'] = pc_LT['POSTCODE'].apply(lambda x: x.lstrip("0"))

# COMMAND ----------

#pc_LT.head(4)

# COMMAND ----------

assert pc_LT.POSTCODE.count() == pc_LT.POSTCODE.nunique()
pc_LT.POSTCODE.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge

# COMMAND ----------

# Merge BU into EUR dataframe
pc_list = [pc_EE, pc_LV, pc_LT]
pc_eur = pd.concat(pc_list)

# COMMAND ----------

pc_eur[['POSTCODE']].dtypes

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

pc_eur['POSTCODE'] = pc_eur['CTRYCODE'] + "_" +pc_eur['POSTCODE'].astype(str)
#pc_eur

# COMMAND ----------

assert pc_eur.POSTCODE.count() == pc_eur.POSTCODE.nunique()
print(pc_eur.POSTCODE.count())

# COMMAND ----------

# Finding the amount of common columns
pc = [
  #pc_SE.columns, pc_DK.columns, pc_NO.columns, pc_PL.columns, pc_IE.columns, 
  pc_LV.columns, pc_LT.columns, pc_EE.columns]

common_cols = set(pc[0])
for s in pc[1:]:
    common_cols.intersection_update(s)


pc_common_cols = list(common_cols)

print(f'Number of common columns: {len(pc_common_cols)}')
print(pc_common_cols)

# COMMAND ----------

# Finding columns that fall out
falled_out_cols = list((
  #set(pc_SE.columns)  | set(pc_DK.columns)| set(pc_NO.columns)
                        #| set(pc_PL.columns) | set(pc_IE.columns) | 
  set(pc_LV.columns) | set(pc_LT.columns)| set(pc_EE.columns)
                       ) - set(pc_common_cols))

print(f'Number of falled out columns: {len(falled_out_cols)}')
print(falled_out_cols)

# COMMAND ----------

BU_list = list(set(pc_eur.CTRYCODE.values.tolist()))
print(f'Includes data for the following BUs - "BU_list": {BU_list}')
