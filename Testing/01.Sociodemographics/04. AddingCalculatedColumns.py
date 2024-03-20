# Databricks notebook source
# MAGIC %md
# MAGIC ## Meta Data

# COMMAND ----------

# MAGIC %md
# MAGIC **Creator: *Evgenia Balkina***
# MAGIC
# MAGIC **Date Created: *16/12/2022***

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### General Configuration
# MAGIC + Running configuration notebook

# COMMAND ----------

# MAGIC %run ../configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Derived Attributes
# MAGIC + Population Dencity by Buffer
# MAGIC + Day-Time/Night-Time differences by Buffer
# MAGIC + Percentage of population by age

# COMMAND ----------

spark.catalog.refreshTable("usr_shared_cat_analytics.archetypes_sociodemographics_EUR")  
sd_clean = spark.read.table('usr_shared_cat_analytics.archetypes_sociodemographics_EUR')

# COMMAND ----------

# population Density

for b in buffer:
   sd_clean = (sd_clean
              .withColumn(f'P_DENS_{b}', F.col(f'P_T_{b}')/F.col(f'total_area_{b}'))
              .withColumn(f'P_D_DENS_{b}', F.col(f'P_D_{b}')/F.col(f'total_area_{b}'))
              .withColumn(f'P_DIFF_{b}', (F.col(f'P_D_{b}')-F.col(f'P_T_{b}'))/F.col(f'P_T_{b}'))
              .withColumn(f'AGE_T0014_pct_{b}', F.col(f'AGE_T0014_{b}')/F.col(f'P_T_Consistent_{b}'))
              .withColumn(f'AGE_T1529_pct_{b}', F.col(f'AGE_T1529_{b}')/F.col(f'P_T_Consistent_{b}'))
              .withColumn(f'AGE_T3044_pct_{b}', F.col(f'AGE_T3044_{b}')/F.col(f'P_T_Consistent_{b}'))
              .withColumn(f'AGE_T4559_pct_{b}', F.col(f'AGE_T4559_{b}')/F.col(f'P_T_Consistent_{b}'))
              .withColumn(f'AGE_T60PL_pct_{b}', F.col(f'AGE_T60PL_{b}')/F.col(f'P_T_Consistent_{b}'))
              )

# Day/Night population differences


# COMMAND ----------

#display(sd_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dictionary for Derived Attributes
# MAGIC + Population Dencity by Buffer
# MAGIC + Day-Time/Night-Time differences by Buffer

# COMMAND ----------

spark.catalog.refreshTable("usr_shared_cat_analytics.archetypes_sdDictionary_EUR") 
sd_dict_clean = spark.read.table('usr_shared_cat_analytics.archetypes_sdDictionary_EUR')

# COMMAND ----------

new_columns = []
for b in buffer:
  cols = [f'P_DENS_{b}',
          f'P_D_DENS_{b}',
          f'P_DIFF_{b}',
          f'AGE_T0014_pct_{b}',
          f'AGE_T1529_pct_{b}',
          f'AGE_T3044_pct_{b}',
          f'AGE_T4559_pct_{b}',
          f'AGE_T60PL_pct_{b}']
  
  new_columns = new_columns + cols
  
new_names = []
for b in buffer:
  cols = [f'Population Density within {b}m',
          f'Daytime Population Density within {b}m',
          f'Daytime minus Nighttime Population as a % of Total Nighttime Population within {b}m',
          f'Population Age: 0 - 14 years, as a % of the total population within {b}m',
          f'Population Age: 15 - 29 years, as a % of the total population within {b}m',
          f'Population Age: 30 - 44 years, as a % of the total population within {b}m',
          f'Population Age: 45 - 59 years, as a % of the total population within {b}m',
          f'Population Age: 60 years and above, as a % of the total population within {b}m']
  new_names = new_names + cols
  


# COMMAND ----------

#new_names

# COMMAND ----------

# specify column names
columns = ['index', 'name']
  
# creating a dataframe by zipping the two lists
df_append_to_dict = (spark.createDataFrame(zip(new_columns, new_names), columns)
                     .withColumn('group_1', F.lit('sociodemographics'))
                     .withColumn('group_2', F.lit('population'))
                    )

# COMMAND ----------

#display(df_append_to_dict)

# COMMAND ----------

sd_dict = sd_dict_clean.unionByName(df_append_to_dict)

# COMMAND ----------

display(sd_dict.filter(F.col('group_2')=='population'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## To Databricks

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

table_name = 'sociodemographics_f'
to_databriccks(sd_clean, table_name, user_name, project_name)
print(f'table_name: {table_name}')

# COMMAND ----------

table_name = 'sdDictionary_f'
to_databriccks(sd_dict, table_name, user_name, project_name)
print(f'table_name: {table_name}')

# COMMAND ----------


