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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Just a sample query to inspect those records with the largest rank difference. Be aware that the previous notebooks didn't train on the same data (a miss because the notebooks were done by different authors). A watchout that should be corrected before run on production data.

# COMMAND ----------

from pyspark.sql.window import Window as W
from pyspark.sql.functions import rank, abs

top_10_diff = pred_sas.alias('sas').join(pred_pyspark.alias('pyspark'), join_cols, "left")  \
                      .withColumn('pred_sas', col('sas.pscore')) \
                      .drop('sas.pscore') \
                      .withColumn("rank_sas",rank().over(W.orderBy('pred_sas'))) \
                      .withColumn("rank_pyspark",rank().over(W.orderBy('pred_pyspark'))) \
                      .withColumn("rank_diff", abs(col("rank_sas") - col("rank_pyspark"))) \
                      .sort(col('rank_diff').desc()) \
                      .limit(10)

display(top_10_diff)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Visual confirmation that the p-value distributions are similar (although the pred_sas has a larger spread).

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Create a single plot with both KDEs
sns.kdeplot(rslt['pred_sas'], label='pred_sas', shade=True)
sns.kdeplot(rslt['pred_pyspark'], label='pred_pyspark', shade=True)

plt.legend()  # Add legend
plt.xlabel('Values')    # Add x-label
plt.ylabel('Density')   # Add y-label
plt.title('KDE Plot of Empirical Distributions') # Add title

plt.show()  # Show plot

# COMMAND ----------

import matplotlib.pyplot as plt

fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10,5))

# Plot first column
axes[0].hist(rslt.iloc[:,0], bins=20)
axes[0].set_title('Distribution of Column 1')

# Plot second column
axes[1].hist(rslt.iloc[:,1], bins=20)
axes[1].set_title('Distribution of Column 2')

plt.show()
