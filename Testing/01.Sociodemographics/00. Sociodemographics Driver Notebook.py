# Databricks notebook source
# MAGIC %md
# MAGIC ## Meta Data

# COMMAND ----------

# MAGIC %md
# MAGIC + Notebook Created by: Evgenia Balkina (evgenia.balkina@circlekeurope.com)
# MAGIC + Date Created: 11/12/2023
# MAGIC + Description: The Notebook runs the update on Sociodemographics for the new sites and included into the pipeline for new sites archetyping
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sociodemographics Calculations

# COMMAND ----------

dbutils.notebook.run("./01. Sociodemographics", 0)

# COMMAND ----------

dbutils.notebook.run("./02. SociodemographicsAlternative", 0)

# COMMAND ----------

dbutils.notebook.run("./03. SociodemographicsRefined", 0)

# COMMAND ----------

dbutils.notebook.run("./04. AddingCalculatedColumns", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updaing Final Data Table
# MAGIC

# COMMAND ----------

dbutils.notebook.run("../PBInputFinal", 0)
