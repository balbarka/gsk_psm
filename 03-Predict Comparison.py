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
                    .withColumn('pred_pyspark', element_at(vector_to_array(col('probability')),2))

# COMMAND ----------

pred_sas = spark.table('main.I_LOCAL.zos97_sas_predict')

# COMMAND ----------

# NOTE: we are sub sampling for faster runs

join_cols = ["agegrp","COST_LVL","COMOR","Prev_use","medication_use","CONCO_VAX","race","sex","nb_INPAT","nb_OUTPT_ER","index_dt","Region"]

rslt = pred_sas.alias('sas').join(pred_pyspark.alias('pyspark'), join_cols, "left") \
               .select(col('sas.pscore').alias('pred_sas'),
                       col('pyspark.pred_pyspark')) \
               .sample(fraction=0.05, seed=42) \
               .toPandas()

# COMMAND ----------

p_sas = rslt['pred_sas']
p_pyspark = rslt['pred_pyspark']

# COMMAND ----------

from operator import add 

#zip sample data and convert to rdd
example_data = zip(p_sas, p_pyspark)
example_rdd = sc.parallelize(example_data)

#take the cartesian product of example data (generate all possible combinations)
all_pairs = example_rdd.cartesian(example_rdd)

#function calculating concorant and disconordant pairs
def calc(pair):
    p1, p2 = pair
    x1, y1 = p1
    x2, y2 = p2
    if (x1 == x2) and (y1 == y2):
        return ("t", 1) #tie
    elif ((x1 > x2) and (y1 > y2)) or ((x1 < x2) and (y1 < y2)):
        return ("c", 1) #concordant pair
    else:
        return ("d", 1) #discordant pair

#rank all pairs and calculate concordant / disconrdant pairs with calc() then return results
results  = all_pairs.map(calc)

#aggregate the results
results = results.aggregateByKey(0, add, add)

#count and collect
n  = example_rdd.count()
d = {k: v for (k, v) in results.collect()}

# http://en.wikipedia.org/wiki/Kendall_tau_rank_correlation_coefficient
tau = (d["c"] - d["d"]) / (0.5 * n * (n-1))

# COMMAND ----------

tau
