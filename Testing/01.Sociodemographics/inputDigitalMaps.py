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
#Sweden
pc_SE_1 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_SE/MBI_SE_1.SHP")
pc_SE_2 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_SE/MBI_SE_2.SHP")
pc_SE_3 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_SE/MBI_SE_3.SHP")
pc_SE_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_SE/MBI_SE_4.SHP")
pc_SE_5 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_SE/MBI_SE_5.SHP")

pc_SE = (pc_SE_5
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

#Denmark
pc_DK_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_DK/MBI_DK_4.SHP")
pc_DK = (pc_DK_4
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

#Norway
pc_NO_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_NO/MBI_NO_4.SHP")
pc_NO = (pc_NO_4
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )



#Irland
pc_IE_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_IE/MBI_IE_4.SHP")

pc_IE = (pc_IE_4
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )



# COMMAND ----------

#Poland
#pc_PL_4 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_PL/MBI_PL_4.SHP")
pc_PL_5 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_PL/MB_N_PL_1.SHP")

pc_PL = (pc_PL_5
        .to_crs('EPSG:4326')
        .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
       )


# COMMAND ----------

#Estonia
pc_EE_1 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_EE/MBI_EE_1.SHP")
pc_EE_2 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_EE/MBI_EE_2.SHP")
pc_EE_3 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_EE/MBI_EE_3.SHP")

pc_EE = (pc_EE_3
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )


# COMMAND ----------

#Latvia
pc_LV_2 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_LV/MBI_LV_2.SHP")

pc_LV = (pc_LV_2
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

#Lithuania
pc_LT_3 = gps.read_file("/dbfs/FileStore/usr_shared_cat_analytics/Sociodemographics/shp/Digital_Boundaries_LT/MBI_LT_3.SHP")

pc_LT = (pc_LT_3
         .to_crs('EPSG:4326')
         .rename({'ADMINCODE': 'POSTCODE'}, axis='columns')
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Standardizing POSTCODES

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Poland

# COMMAND ----------

assert pc_PL.POSTCODE.count() == pc_PL.POSTCODE.nunique()
#print(pc_PL.POSTCODE.count())

# COMMAND ----------

#Function for Poland (5-digit code with zeros)

def standard_PL(txt):
  y1 = txt.split('-',1)[0]
  y2 = txt.split('-',1)[1]
  x2 = y2.zfill(3)
  x1 = y1.zfill(2)
  fsting = x1 + x2
  return fsting

# COMMAND ----------

pc_PL['POSTCODE'] = pc_PL['POSTCODE'].apply(lambda x: standard_PL(x))

# COMMAND ----------

#pc_PL.head(4)

# COMMAND ----------

assert pc_PL.POSTCODE.count() == pc_PL.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_PL.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Ireland

# COMMAND ----------

assert pc_IE.POSTCODE.count() == pc_IE.POSTCODE.nunique()
#print(pc_IE.POSTCODE.count())

# COMMAND ----------

#Function for Ireland (trnaslates letters into numbers)

from string import ascii_lowercase
LETTERS = {letter: str(index) for index, letter in enumerate(ascii_lowercase, start=1)} 
def alphabet_position(text):
  try:
    text = text.lower()
    numbers = [LETTERS[character] for character in text if character in LETTERS]
    output = "%s%s"%(*numbers, text[1:3])
  except: 
    output = text
  return output

# COMMAND ----------

pc_IE['POSTCODE'] = pc_IE['POSTCODE'].apply(lambda x: alphabet_position(x)).str.replace("D6W","99999")
#.str.replace(" ","").str.replace("-","").str.replace("D6W","99999")

# COMMAND ----------

#pc_IE.head(4)

# COMMAND ----------

assert pc_IE.POSTCODE.count() == pc_IE.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_IE.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sweden

# COMMAND ----------

#pc_SE.head(4)

# COMMAND ----------

assert pc_SE.POSTCODE.count() == pc_SE.POSTCODE.nunique()
print(pc_SE.POSTCODE.count())

# COMMAND ----------

pc_SE['POSTCODE'] = pc_SE['POSTCODE'].str.replace(" ","")

# COMMAND ----------

#pc_SE.head(4)

# COMMAND ----------

assert pc_SE.POSTCODE.count() == pc_SE.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_SE.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Norway

# COMMAND ----------

#pc_NO.head(4)

# COMMAND ----------

assert pc_NO.POSTCODE.count() == pc_NO.POSTCODE.nunique()
#print(pc_NO.POSTCODE.count())

# COMMAND ----------

pc_NO['POSTCODE'] = pc_NO['POSTCODE'].apply(lambda x: x.zfill(4))

# COMMAND ----------

assert pc_NO.POSTCODE.count() == pc_NO.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_NO.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Estonia

# COMMAND ----------

#pc_EE.head(4)

# COMMAND ----------

assert pc_EE.POSTCODE.count() == pc_EE.POSTCODE.nunique()

# COMMAND ----------

pc_EE['POSTCODE'] = pc_EE['POSTCODE'].apply(lambda x: x.lstrip("0"))

# COMMAND ----------

#pc_EE.head(4)

# COMMAND ----------

assert pc_EE.POSTCODE.count() == pc_EE.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_EE.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Latvia

# COMMAND ----------

pc_LV.head(4)

# COMMAND ----------

assert pc_LV.POSTCODE.count() == pc_LV.POSTCODE.nunique()
pc_LV.POSTCODE.count()

# COMMAND ----------

pc_LV['POSTCODE'] = pc_LV['POSTCODE'].apply(lambda x: x.lstrip("0"))

# COMMAND ----------

pc_LV.head(4)

# COMMAND ----------

assert pc_LV.POSTCODE.count() == pc_LV.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_LV.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lithuania

# COMMAND ----------

pc_LT.head(4)

# COMMAND ----------

assert pc_LT.POSTCODE.count() == pc_LT.POSTCODE.nunique()
pc_LT.POSTCODE.count()

# COMMAND ----------

pc_LT['POSTCODE'] = pc_LT['POSTCODE'].apply(lambda x: x.lstrip("0"))

# COMMAND ----------

pc_LT.head(4)

# COMMAND ----------

assert pc_LT.POSTCODE.count() == pc_LT.POSTCODE.nunique()
print(f'Number of POSTCODEs: {pc_LT.POSTCODE.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge

# COMMAND ----------

# Merge BU into EUR dataframe
pc_list = [pc_SE, pc_DK, pc_NO, pc_PL, pc_IE, pc_LV, pc_LT, pc_EE]
pc_eur = pd.concat(pc_list)

# COMMAND ----------

#display(pc_eur)

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

# pc_eur['POSTCODE'] = pc_eur['POSTCODE'].str.replace(" ","").str.replace("-","").str.replace("D6W","99999")
# pc_eur['POSTCODE'] = pc_eur['POSTCODE'].apply(lambda x: alphabet_position(x)).astype({'POSTCODE':'int'})
# pc_eur

# COMMAND ----------

pc_eur['POSTCODE'] = pc_eur['CTRYCODE'] + "_" +pc_eur['POSTCODE'].astype(str)
#pc_eur

# COMMAND ----------

assert pc_eur.POSTCODE.count() == pc_eur.POSTCODE.nunique()
print(pc_eur.POSTCODE.count())

# COMMAND ----------

# Finding the amount of common columns
pc = [pc_SE.columns, pc_DK.columns, pc_NO.columns, pc_PL.columns, pc_IE.columns, pc_LV.columns, pc_LT.columns, pc_EE.columns]

common_cols = set(pc[0])
for s in pc[1:]:
    common_cols.intersection_update(s)


pc_common_cols = list(common_cols)

print(f'Number of common columns: {len(pc_common_cols)}')
print(pc_common_cols)

# COMMAND ----------

# Finding columns that fall out
falled_out_cols = list((set(pc_SE.columns)  | set(pc_DK.columns)| set(pc_NO.columns)
                        | set(pc_PL.columns) | set(pc_IE.columns) 
                        | set(pc_LV.columns) | set(pc_LT.columns)| set(pc_EE.columns)
                       ) - set(pc_common_cols))

print(f'Number of falled out columns: {len(falled_out_cols)}')
print(falled_out_cols)

# COMMAND ----------

BU_list = list(set(pc_eur.CTRYCODE.values.tolist()))
print(f'Includes data for the following BUs - "BU_list": {BU_list}')

# COMMAND ----------


