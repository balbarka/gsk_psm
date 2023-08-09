# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Compare model output results
# MAGIC
# MAGIC So since both SAS and Pyspark are actually using iterative solvers for fitting the estimator, we have some non-deterministic characteristics of these (or at least a random initialization would make them stochastic).
# MAGIC
# MAGIC To over come the differences that could arise from this we will want a metric to determine how well these model predicts have the same ranking for predictions. For this we'll use Kendall corelation.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit, element_at
from pyspark.ml.functions import vector_to_array

# We'll need to do a couple transforms to get the p value
pred_pyspark = spark.table('main.I_LOCAL.zos97_pyspark_predict') \
                    .withColumn('pred_pyspark', element_at(vector_to_array(col('probability')),1))

# COMMAND ----------

pred_sas = spark.table('main.I_LOCAL.zos97_sas_predict')

# COMMAND ----------

# NOTE: we are sub sampling for faster runs

join_cols = ["agegrp","COST_LVL","COMOR","Prev_use","medication_use","CONCO_VAX","race","sex","nb_INPAT","nb_OUTPT_ER","index_dt","Region"]

rslt = (pred_sas.alias('sas').join(pred_pyspark.alias('pyspark'), join_cols, "left") 
               .select(col('sas.pscore').alias('pred_sas'),col('pyspark.pred_pyspark'))
               #.sample(fraction=0.2, seed=42)
               .toPandas()
)

# COMMAND ----------

rslt.head(100)

# COMMAND ----------

p_sas = rslt['pred_sas']
p_pyspark = rslt['pred_pyspark']

# COMMAND ----------

from scipy.stats import kendalltau
tau, p_value = kendalltau(p_sas,p_pyspark)
print(f"Tau: {tau}" )
