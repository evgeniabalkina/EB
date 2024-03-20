# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ../configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combination

# COMMAND ----------

sociodemographicsAlternative = spark.read.table('usr_shared_cat_analytics.archetypes_sociodemographicsalternative_eur')
sociodemographics            = spark.read.table('usr_shared_cat_analytics.archetypes_sociodemographics_main_eur')

# COMMAND ----------

# duplicating "P_T_buffer" (night-time population) for further use 
sociodemographics            = (sociodemographics
                                .select(*sociodemographics.columns, *[(F.col(f'P_T_{b}').alias(f'P_T_Consistent_{b}')) for b in buffer])
                               )

# COMMAND ----------

sd_columns = sociodemographics.columns
len(sd_columns)

# COMMAND ----------

#sd_columns

# COMMAND ----------

sda_columns = sociodemographicsAlternative.columns
sda_columns.remove('site_number')
#sda_columns
len(sda_columns)

# COMMAND ----------

#sda_columns

# COMMAND ----------

clean_columns = list(set(sd_columns)-set(sda_columns))
clean_columns.remove('site_number')
len(clean_columns)

# COMMAND ----------

#clean_columns

# COMMAND ----------

sociodemographicsAlternative = sociodemographicsAlternative.select('site_number', *[F.col(c).alias(c+"_a") for c in sda_columns])

# COMMAND ----------

sd_final =(sociodemographics
             .join(sociodemographicsAlternative, 'site_number', how='left')
             .select('site_number', *clean_columns, *[((F.when((F.col(f'{c}_a').isNull()), F.col(c)).otherwise(F.col(f'{c}_a'))).alias(c)) for c in sda_columns])
            )

# COMMAND ----------

len(sd_final.columns)

# COMMAND ----------

#display(sd_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## to Datbricks

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

#spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

table_name = 'sociodemographics'
print(f'"table_name": {table_name}')
to_databriccks(sd_final, table_name, user_name, project_name)

# COMMAND ----------

#test = spark.read.table('usr_shared_cat_analytics.archetypes_sociodemographics_main__EUR')

# COMMAND ----------

#display(test.filter(F.col('site_number').isin(['61001', '61002'])).select(['site_number', 'PP_CI_5000', 'PP_CI_500']))

# COMMAND ----------


