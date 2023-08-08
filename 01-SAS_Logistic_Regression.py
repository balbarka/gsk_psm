# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC To show that the results of the SAS and pyspark models are comparable, we are going to run our model on the adult data in SAS as well.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC First, we will establish a conneciton to Databricks and download the data that we used for training the pyspark model:

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC libname i_local clear;
# MAGIC libname i_local JDBC 
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="/export/sas-viya/data/drivers/"
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443;transportMode=http;ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;ConnCatalog=main;ConnSchema=I_LOCAL;"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC proc contents data=i_local._all_ nods;
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS main.I_LOCAL.zos97_sas_predict;

# COMMAND ----------

# MAGIC %%SAS log lst
# MAGIC
# MAGIC PROC LOGISTIC data=I_LOCAL.ZOS97_MATCHING(drop=age);
# MAGIC    class vax agegrp medication_use sex race COST_LVL COMOR CONCO_VAX nb_INPAT nb_OUTPT_ER Region Prev_use ;
# MAGIC    model Vax = agegrp medication_use index_dt sex race COST_LVL COMOR CONCO_VAX nb_INPAT nb_OUTPT_ER Region Prev_use / link=cloglog;
# MAGIC    output out=I_LOCAL.ZOS97_SAS_PREDICT p=pscore;
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.I_LOCAL.zos97_sas_predict;

# COMMAND ----------

dat = 
