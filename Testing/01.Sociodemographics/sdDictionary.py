# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Sweden
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_DK_Postcodes4_Variables.csv'
dict_SE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionSE')
           .withColumnRenamed('type', 'typeSE')
      )

# Estonia
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_EE_Omavalitsuste_Variables.csv'
dict_EE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionEE')
           .withColumnRenamed('type', 'typeEE')
      )

# Ireland
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_IE_Postcodes3_Variables.csv'
dict_IE = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionIE')
           .withColumnRenamed('type', 'typeIE')
      )

# Lithuania
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_LT_Savivaldybes_Variables.csv'
dict_LT = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionLT')
           .withColumnRenamed('type', 'typeLT')
      )

# Latvia
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_LV_Novadi_Variables.csv'
dict_LV = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionLV')
           .withColumnRenamed('type', 'typeLV')
      )

# Norway
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_NO_Postcodes4_Variables.csv'
dict_NO = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionNO')
           .withColumnRenamed('type', 'typeNO')
      )

# Poland
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_PL_Postcodes5_Variables.csv'
dict_PL = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionPL')
           .withColumnRenamed('type', 'typePL')
      )

# Denmark
filePath = 'dbfs:/FileStore/usr_shared_cat_analytics/Sociodemographics/dictionary/MBI_MarketData_2021_DK_Postcodes4_Variables.csv'
dict_DK = (spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", sep = ";", escape='"')
           .filter(F.col('Variables').isNotNull())
           .filter(F.col('_c1').isNotNull())
           .withColumnRenamed('_c1', 'DescriptionDK')
           .withColumnRenamed('type', 'typeDK')
      )

# COMMAND ----------

#display(dict_EE)

# COMMAND ----------



# COMMAND ----------

disct_eur = (dict_SE
             .join(dict_EE, 'Variables', how = 'outer')
             .join(dict_IE, 'Variables', how = 'outer')
             .join(dict_LT, 'Variables', how = 'outer')
             .join(dict_LV, 'Variables', how = 'outer')
             .join(dict_NO, 'Variables', how = 'outer')
             .join(dict_PL, 'Variables', how = 'outer')
             .join(dict_DK, 'Variables', how = 'outer')
            )

# COMMAND ----------

#display(disct_eur)

# COMMAND ----------

var_list = disct_eur.select('Variables').distinct().rdd.flatMap(lambda x: x).collect()
#len(var_list)

# COMMAND ----------

disct_eur_pd = disct_eur.toPandas().set_index('Variables')
#disct_eur_pd

# COMMAND ----------

sd_dict = {}
type_dict = {}

description_label = ['DescriptionSE', 'DescriptionIE', 'DescriptionDK', 'DescriptionPL', 'DescriptionLT', 'DescriptionLV', 'DescriptionEE']
type_label = ['typeSE', 'typeIE', 'typeDK', 'typePL', 'typeLT', 'typeLV', 'typeEE']

for key in var_list:
  for descr, t in zip(description_label, type_label):
    if not (disct_eur_pd.loc[key, descr] is None):
      sd_dict[key]   =  disct_eur_pd.loc[key, descr]
      type_dict[key] =  disct_eur_pd.loc[key, t]
           
      break
  else:
    sd_dict[key] = None
    type_dict[key] = None


# COMMAND ----------

sd_dict.keys()

# COMMAND ----------

group_label = ['Population', 'Unemployed', 'Household','Power', 'Spending']
group_label_dict = {'Population': 'population', 'Unemployed': 'population', 'Household': 'population', 'Power': 'income', 'Spending': 'spending'}

gr_dict = {}

for key in sd_dict.keys():
  if type_dict[key] == 'main':
    gr_dict[key] = type_dict[key]
  for f in group_label:
    if f in sd_dict[key]:
      gr_dict[key] = group_label_dict[f]
    
    #else: group_dict[key] = 'None'

# COMMAND ----------

assert len(sd_dict.keys()) == len(gr_dict.keys())

# COMMAND ----------

assert len(sd_dict) == len(var_list)
assert len(type_dict) == len(var_list)

# COMMAND ----------

print(f'Length of Sociodemographics dictionaty: {len(sd_dict)}')
print(f'Sociodemographics dictionaty - sd_dict: {sd_dict}')
#sd_dict

# COMMAND ----------

print(f'Length of Types dictionaty: {len(type_dict)}')
print(f'Types Dictionaty - type_dict : {type_dict}')
#type_dict

# COMMAND ----------

print(f'Length of Group dictionaty: {len(gr_dict)}')
print(f'Group Dictionaty - group_dict : {gr_dict}')

# COMMAND ----------

type_dict_inv = {}
for k, v in type_dict.items():
    type_dict_inv[v] = type_dict_inv.get(v, []) + [k]

# COMMAND ----------

print(f'Length of Sociodemographics Dictionaty by type: {len(type_dict_inv)}')
print(f'Sociodemographics Dictionaty by type - type_dict_inv: {type_dict_inv}')
#type_dict_inv

# COMMAND ----------

#type_dict_inv

# COMMAND ----------

#len(type_dict_inv)

# COMMAND ----------

main_col = type_dict_inv['main']
print(f'"main_col": {main_col}')

# COMMAND ----------


