# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### General Configuration
# MAGIC + Running configuration notebook

# COMMAND ----------

# MAGIC %pip install geopandas

# COMMAND ----------

# MAGIC %pip install shapely

# COMMAND ----------

# MAGIC %pip install rtree

# COMMAND ----------

# MAGIC %run ../configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### SD Dictionary
# MAGIC - Running a notebook with the Sociodemogs Dictionary (upload the dicts from each country and combines them): 'sd_dict'
# MAGIC - Creating a final dictionary with Buffers: 'sd_dict_buffer'

# COMMAND ----------

# MAGIC %run ./sdDictionary

# COMMAND ----------

main_col

# COMMAND ----------

# adding "total_area" as total catchment are of a site to the dictionary:
sd_dict['total_area'] = 'Total catchment area'
gr_dict['total_area'] = 'main'

# COMMAND ----------

#Creating a final dictionary with Buffers
sd_dict_buffer = {}
gr_dict_buffer = {}

for b in buffer:
  for key in sd_dict:
    if key in main_col:
      sd_dict_buffer[key] = sd_dict[key]
      gr_dict_buffer[key] = gr_dict[key]
    else: 
      sd_dict_buffer[f'{key}_{b}'] = f'{sd_dict[key]} within {b}m'
      gr_dict_buffer[f'{key}_{b}'] = gr_dict[key]

# COMMAND ----------

#sd_dict_buffer

# COMMAND ----------

#gr_dict_buffer

# COMMAND ----------

# ensure we created the right amount of columns
assert len(sd_dict_buffer) == ((len(sd_dict)-len(main_col)) * len(buffer))+len(main_col)
assert len(gr_dict_buffer) == ((len(gr_dict)-len(main_col)) * len(buffer))+len(main_col)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook-specific Configuration
# MAGIC + Uploading geopandas and related

# COMMAND ----------

print(f'Last Date Update: {date.today()}')

# COMMAND ----------

#dbutils.library.installPyPI('geopandas')
#dbutils.library.installPyPI('shapely')
#dbutils.library.installPyPI('rtree')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import geopandas as gps
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sites
# MAGIC + Creating GeoDataframe for all EUR sites

# COMMAND ----------

additiona_site_columns = ['site_bu', 'site_bu_type_code', 'site_ownership', 'site_format', 'site_status']

LOC = (spark.read.table("common.site_d")
       .filter(site_filter)       
       .select('site_number', 'site_name', 'site_latitude', 'site_longitude', *additiona_site_columns).distinct()
       .toPandas()
      )

# Creating GeoDataframe for sites
gdf_loc = gps.GeoDataFrame(
    LOC, geometry=gps.points_from_xy(LOC.site_longitude, LOC.site_latitude), crs='epsg:4326')

# COMMAND ----------

gdf_loc.head(4)

# COMMAND ----------

gdf_loc.plot()

# COMMAND ----------

#gdf_loc[gdf_loc['site_bu']=='IE'].count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Postcodes
# MAGIC + Runnig Digital Maps Notebook

# COMMAND ----------

# MAGIC %run ./inputDigitalMaps

# COMMAND ----------

#%run ./inputDigitalMapsAlternative

# COMMAND ----------

# MAGIC %md
# MAGIC ####Visuals

# COMMAND ----------

# select BU to visualize
BU = 'PL'
pc = pc_eur[pc_eur['CTRYCODE']==BU]

# COMMAND ----------

pc.head(4)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(4, 3))
pc.plot(ax=ax, alpha=0.5, edgecolor='k')

# COMMAND ----------

#gdf_loc[gdf_loc['site_bu']=='NO'].plot()
gdf_loc[gdf_loc['site_bu']=='PL'].plot()

# COMMAND ----------

fig, ax = plt.subplots(figsize=(4, 3))
pc_eur.plot(ax=ax, alpha=0.5, edgecolor='k')

# COMMAND ----------

# to crs that uses meters as distance measure
pc_3395 = pc.to_crs('epsg:3395')
gdf_loc_3395 = gdf_loc.to_crs('epsg:3395')

#setting abuffer to make site visible
gdf_loc_3395['geometry']= gdf_loc_3395.buffer(5000) 

difference = pc_3395.overlay(gdf_loc_3395, how = 'difference')

# COMMAND ----------

difference.plot(alpha=0.5, edgecolor='k', cmap='tab10')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculating weights

# COMMAND ----------

# buffer values (in meters) from the configuration notebook
buffer

# COMMAND ----------

# Create a dictionary with buffer as a key and a corresponding GeoDataframes (Poligones around each site within buffer radius)
gdf_loc_km_buffer= {}

for b in buffer:
  gdf_loc_km = gdf_loc.to_crs("EPSG:32634")
  gdf_loc_km_buffer[b] = gdf_loc_km
  gdf_loc_km_buffer[b].geometry =gdf_loc_km_buffer[b].geometry.buffer(b)

# COMMAND ----------

#gdf_loc_km_buffer[500].head(4)

# COMMAND ----------

# # create a dictionary with buffer as a key and corresponding GeoDataframe as value

# pc_weights = {}
# site_cores_d = {}

# pc_eur_3395 = pc_eur.to_crs('epsg:3395')
# pc_eur_3395['area_pc'] = pc_eur_3395['geometry'].area/10**6
  
# for b in buffer:

#   gdf_loc_km_buffer_3395 = gdf_loc_km_buffer[b].to_crs('epsg:3395')
    
#   # overlay postcodes and the sites' catchment areas
#   site_cores = pc_eur_3395.overlay(gdf_loc_km_buffer_3395, how = 'intersection')
#   site_cores = site_cores.to_crs('epsg:32633')

 
#   # calculating the area of each postal code within buffer
#   site_cores['area'] = site_cores['geometry'].area/10**6
#   site_cores_d[b] = site_cores
 
#   # calculating the total catchment area of a buffer (sum up individual areas in case of missing geography)
#   site_cores_agg = site_cores.groupby(['site_number'])['area'].agg('sum').reset_index().rename({'area': 'total_area'}, axis='columns')
 
#   # join the two and calculate the weigth of each postal code in the catchment area and a share of the buffer in the total postal code area
#   site_cores_joined = (site_cores
#                        .merge(site_cores_agg, on = 'site_number')
#                                             )
#   # adding "weight" column that contains share of each postcode in the catchment area (aren within buffer)
#   site_cores_joined['weight'] = site_cores_joined['area']/site_cores_joined['total_area']
#   # adding "wight_pc" column that contains a share of catchment area within a specific postcode to the postcode area
#   site_cores_joined['weight_pc'] = site_cores_joined['area']/site_cores_joined['area_pc']
  
#   # save in the dictionary
#   pc_weights[b] = site_cores_joined

# COMMAND ----------

# create a dictionary with buffer as a key and corresponding GeoDataframe as value

pc_weights = {}
site_cores_d = {}

pc_eur_3395 = pc_eur.to_crs('epsg:32633')
pc_eur_3395['area_pc'] = pc_eur_3395['geometry'].area/10**6
  
for b in buffer:

  gdf_loc_km_buffer_3395 = gdf_loc_km_buffer[b].to_crs('epsg:32633')
    
  # overlay postcodes and the sites' catchment areas
  site_cores = pc_eur_3395.overlay(gdf_loc_km_buffer_3395, how = 'intersection')
  #site_cores = site_cores.to_crs('epsg:32633')

 
  # calculating the area of each postal code within buffer
  site_cores['area'] = site_cores['geometry'].area/10**6
  site_cores_d[b] = site_cores
 
  # calculating the total catchment area of a buffer (sum up individual areas in case of missing geography)
  site_cores_agg = site_cores.groupby(['site_number'])['area'].agg('sum').reset_index().rename({'area': 'total_area'}, axis='columns')
 
  # join the two and calculate the weigth of each postal code in the catchment area and a share of the buffer in the total postal code area
  site_cores_joined = (site_cores
                       .merge(site_cores_agg, on = 'site_number')
                                            )
  # adding "weight" column that contains share of each postcode in the catchment area (aren within buffer)
  site_cores_joined['weight'] = site_cores_joined['area']/site_cores_joined['total_area']
  # adding "wight_pc" column that contains a share of catchment area within a specific postcode to the postcode area
  site_cores_joined['weight_pc'] = site_cores_joined['area']/site_cores_joined['area_pc']
  
  # save in the dictionary
  pc_weights[b] = site_cores_joined

# COMMAND ----------

# pc_eur_32633 = pc_eur.to_crs('epsg:32633')
# pc_eur_32633['area_pc'] = pc_eur_32633['geometry'].area/10**6
# pc_eur_32633_f = pc_eur_32633.groupby(['POSTCODE'])['area_pc'].agg('sum').reset_index()

# COMMAND ----------

#pc_eur_32633_f_spark = spark.createDataFrame(pc_eur_32633_f)

# COMMAND ----------

#pc_eur_32633_f

# COMMAND ----------

#pc_eur_3395_f = pc_eur_3395.groupby(['POSTCODE'])['area_pc'].agg('sum').reset_index()

# COMMAND ----------

#pc_weights[5000][pc_weights[5000]['site_bu']=='NO'].plot()

# COMMAND ----------

#pc_weights[500][pc_weights[500]['site_bu']=='NO']

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checks

# COMMAND ----------

BU_list

# COMMAND ----------

# creating dicionaries for each BU with buffer as a key and number of site numbers as value
site_list = {}

for bu in BU_list:
  site_list[bu] = {}
  for b in buffer:    
    slicer = pc_weights[b][pc_weights[b]['site_bu'] == bu]
    site_list[bu][b] = len(list(set(slicer['site_number'].tolist())))
  
site_list 

# COMMAND ----------

# creating dicionaries for all BUs combined with buffer as a key and number of site numbers as value
site_list_merged = {}
list_merged = {}

for b in buffer:    
  slicer = pc_weights[b] [pc_weights[b]['site_bu'].isin(BU_list)]
  site_list_merged[b] = len(list(set(slicer['site_number'].tolist())))
  list_merged[b] = list(set(slicer['site_number'].tolist()))
  
site_list_merged 

# COMMAND ----------

#Check Norway (2 sites in Swalbard wich are out of postcodes data)
st1 = gdf_loc[gdf_loc['site_bu']=='NO']['site_number'].count()
st2 = site_list['NO'][500]
print(f'Number of station in the Digital Map: {st2}')
print(f'Total number of Sites: {st1}')

# COMMAND ----------

#gdf_loc

# COMMAND ----------

# Norway Sites wich are out of postcodes data
slicer_NO = pc_weights[b][pc_weights[b]['site_bu'] == 'NO']
site_NO = list(set(slicer['site_number'].tolist()))

gdf_loc_NO = gdf_loc[gdf_loc['site_bu']=='NO']
x = gdf_loc_NO[~(gdf_loc_NO['site_number'].isin(site_NO))]
x

# COMMAND ----------

BU_list_other = BU_list.copy()
BU_list_other.remove('NO')
BU_list_other

# COMMAND ----------

# check other BUS

for bu in BU_list_other:
  for b in buffer:
    if not (gdf_loc[gdf_loc['site_bu']==bu]['site_number'].count() == site_list[bu][b]):
      raise ValueError(f'Error with BU {bu} and Buffer {b}')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ####Visuals

# COMMAND ----------

# select BU and catchment area (radius in meters) to visualize
BU = 'SE'
b = 1000

# COMMAND ----------

pc_weight = pc_weights[b][pc_weights[b]['site_bu']==BU]
pc_weight.head(4)

# COMMAND ----------

pc_weight.plot(alpha=0.5, edgecolor='k', cmap='tab10')

# COMMAND ----------

bu = 'SE'
ncols = len(buffer)
nrows = 1

fig, axs = plt.subplots(nrows, ncols, figsize=(4*ncols,5*nrows))
for b, ax in zip(buffer, axs.flat):
  pc_weight = pc_weights[b][pc_weights[b]['site_bu']==bu]
  pc_weight.plot(ax=ax, alpha=0.5, edgecolor='k', cmap='tab10')
plt.tight_layout()

# COMMAND ----------

bu = 'NO'

fig, axs = plt.subplots(ncols=len(buffer), figsize=(4*ncols,5*nrows))
for b, ax in zip(buffer, axs.flat):
  pc_weight = pc_weights[b][pc_weights[b]['site_bu']==bu]
  pc_weight.plot(ax=ax, alpha=0.5, edgecolor='k', cmap='tab10')
plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SocioDemographics

# COMMAND ----------

# MAGIC %run ./inputSociodemographics

# COMMAND ----------

# MAGIC %md
# MAGIC ####Check

# COMMAND ----------

#display(sd_eur.filter(F.col('CTRYCODE').isin(['EE'])))

# COMMAND ----------

total_POSTCODE_count = len(sd_eur.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
SE_POSTCODE_count    = len(sd_SE.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
NO_POSTCODE_count    = len(sd_NO.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
DK_POSTCODE_count    = len(sd_DK.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
IE_POSTCODE_count    = len(sd_IE.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
PL_POSTCODE_count    = len(sd_PL.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
EE_POSTCODE_count    = len(sd_EE.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
LT_POSTCODE_count    = len(sd_LT.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())
LV_POSTCODE_count    = len(sd_LV.select('POSTCODE', 'CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())

assert total_POSTCODE_count == SE_POSTCODE_count + NO_POSTCODE_count + DK_POSTCODE_count + IE_POSTCODE_count + PL_POSTCODE_count + EE_POSTCODE_count + LT_POSTCODE_count + LV_POSTCODE_count

# COMMAND ----------

countryCountSD = len(sd_eur.select('CTRYCODE').distinct().rdd.flatMap(lambda x: x).collect())

assert countryCountSD == len(BU_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge

# COMMAND ----------

buffer

# COMMAND ----------

index_col = []
for c in sd_eur_col:
  if c in type_dict_inv['index']:
    index_col.append(c)
  
total_col = []
for c in sd_eur_col:
  if c in type_dict_inv['total']:
    total_col.append(c)

# COMMAND ----------

print(f'Number of Index columns: {len(index_col)}')
print(index_col)

print(f'Number of Total columns: {len(total_col)}')
print(total_col)

# COMMAND ----------

assert len(sd_eur_col) == len(index_col) +len(total_col)

# COMMAND ----------

sd_eur_col_x = [f'{c}_w' for c in sd_eur_col]

merged_d= {}
weighted_d = {}

for b in buffer:
  pd_weights = pd.DataFrame(pc_weights[b])[['site_number', 'POSTCODE', 'total_area', 'weight', 'weight_pc']]
  weights = spark.createDataFrame(pd_weights)
  merged = weights.join(sd_eur, 'POSTCODE')
  for c in sd_eur_col:
    merged = (merged
              .withColumn(f'{c}_w', F.col(c)*F.col('weight'))
              .withColumn(f'{c}_wpc', F.col(c)*F.col('weight_pc'))
             )
    merged_d[b] = merged
  
  weighted = (merged_d[b]    
                .groupBy('site_number', F.col('total_area').alias(f'total_area_{b}'))
                .agg(*[F.round(F.sum(f'{c}_w'),2).alias(f'{c}_{b}') for c in index_col], 
                     *[F.round(F.sum(f'{c}_wpc'),2).alias(f'{c}_{b}') for c in total_col])
               )
  weighted_d[b]=weighted
             

# COMMAND ----------

sd_final = weighted_d[buffer[0]]

for b in buffer[1: len(buffer)]:
  sd_final = sd_final.join(weighted_d[b], 'site_number', how = 'outer')

# COMMAND ----------

#display(sd_final.select('site_number', 'MALE_500', 'MALE_5000', 'FEMALE_500', 'FEMALE_5000'))

# COMMAND ----------

#display(weighted_d[500])

# COMMAND ----------

#display(weighted_d[1000])

# COMMAND ----------

#display(weighted_d[3000])

# COMMAND ----------

#display(weighted_d[5000])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check

# COMMAND ----------

BU_list

# COMMAND ----------

# check if we have data for all sites

for b in buffer:
  site_data  = weighted_d[b].select('site_number').distinct().rdd.flatMap(lambda x: x).collect()
  site_shape = list_merged[b]
  x = list(set(site_shape) - set(site_data))
  if not (len(x) == 0):
    raise ValueError(f'Error with buffer {b}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dictionary to Dataframe

# COMMAND ----------

notakey = [x for x in sd_final.columns if not x in sd_dict_buffer]
print(len(notakey))
print(notakey)

# COMMAND ----------

assert len(notakey)==1

# COMMAND ----------

#sd_dict_buffer

# COMMAND ----------

sd_dict_buffer_pd = pd.DataFrame(sd_dict_buffer, index=[0])
gr_dict_buffer_pd = pd.DataFrame(gr_dict_buffer, index=[0])

# COMMAND ----------

sd_dict_buffer_pd_t = sd_dict_buffer_pd.T
sd_dict_buffer_pd_t.reset_index(inplace= True)

gr_dict_buffer_pd_t = gr_dict_buffer_pd.T
gr_dict_buffer_pd_t.reset_index(inplace= True)

# COMMAND ----------

#sd_dict_buffer_pd_t

# COMMAND ----------

#gr_dict_buffer_pd_t

# COMMAND ----------

sdDictionary_spark = spark.createDataFrame(sd_dict_buffer_pd_t).withColumnRenamed('0', 'name')
grDictionary_spark = spark.createDataFrame(gr_dict_buffer_pd_t).withColumnRenamed('0', 'group_2')

Dictionary_spark = sdDictionary_spark.join(grDictionary_spark, 'index').withColumn('group_1', F.lit('sociodemographics'))

display(Dictionary_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## to Datbricks

# COMMAND ----------

#sd_final

# COMMAND ----------

print(f'"project_name": {project_name}')
print(f'"user_name": {user_name}')

# COMMAND ----------

#Writing to DataBricks
def to_databriccks(df_name, table_name, user_name, project_name):
  table_name_full = "{}.{}_{}_{}".format(user_name, project_name, table_name, 'EUR')
  df_name.write.format("parquet").mode("overwrite").saveAsTable(table_name_full)
  return print("saved in DB as ",table_name_full)

# COMMAND ----------

table_name = 'sociodemographics_main'
print(f'"table_name": {table_name}')
to_databriccks(sd_final, table_name, user_name, project_name)

# COMMAND ----------

table_name = 'sdDictionary'
print(f'"table_name": {table_name}')
to_databriccks(Dictionary_spark, table_name, user_name, project_name)

# COMMAND ----------

#table_name = 'sdByPostcode'
#print(f'"table_name": {table_name}')
#to_databriccks(sd_eur, table_name, user_name, project_name)

# COMMAND ----------

#table_name = 'areaByPostcode'
#print(f'"table_name": {table_name}')
#to_databriccks(pc_eur_32633_f_spark, table_name, user_name, project_name)

# COMMAND ----------


