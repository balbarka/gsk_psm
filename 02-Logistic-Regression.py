# Databricks notebook source
# MAGIC %md
# MAGIC ### SparkML 
# MAGIC [MLlib](https://spark.apache.org/docs/latest/ml-guide.html) is Spark’s machine learning (ML) library. Its goal is to make practical machine learning scalable and easy. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data
# MAGIC Now let's read the data from table `main.I_LOCAL.zos97_matching`

# COMMAND ----------

data = spark.table('main.I_LOCAL.zos97_matching').withColumnRenamed("vax","label")
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training and Test data creation
# MAGIC Randomly split data into training and test sets, and set seed for reproducibility.

# COMMAND ----------

train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)
print(train_df.cache().count()) 
print(test_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature processing
# MAGIC The logistic regression model that we use in this notebook requires numeric features. We have fields that have categorical values that are of type `string`. In order to use these fields
# MAGIC 1) We need to convert the string values into numeric indexes
# MAGIC 2) Then perform One Hot Encoding on the numeric indexes to convert them to a binary vector with one value
# MAGIC
# MAGIC Spark ML provides two apis to do this
# MAGIC - [StringIndexer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StringIndexer.html) 
# MAGIC - [OneHotEncoder](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.OneHotEncoder.html) 
# MAGIC
# MAGIC Most Spark ML Models also require single feature column as input. Each row in this column contains a vector of data points corresponding to the set of features used for prediction. Spark ML provides a [VectorAssembler](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.VectorAssembler.html#pyspark.ml.feature.VectorAssembler) api to achieve this. 
# MAGIC
# MAGIC Now let us build our features for model training

# COMMAND ----------

#Lets identify categorical columns
label_column = "vax"
numerical_columns = ["nb_INPAT","nb_OUTPT_ER","index_dt"]
categorical_columns = ["agegrp","COST_LVL","COMOR","Prev_use","medication_use","CONCO_VAX","race","sex","Region"]

feature_columns = categorical_columns + numerical_columns

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

#feature cols
string_indexer = StringIndexer(inputCols=categorical_columns, outputCols=[col + "Index" for col in categorical_columns]) 
ohe_encoder = OneHotEncoder(inputCols=string_indexer.getOutputCols(), outputCols=[col + "OHE" for col in categorical_columns]) 

assembler_inputs = [col + "OHE" for col in categorical_columns] + numerical_columns
vector_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

#label col
#label_indexer = StringIndexer(inputCol=label_column, outputCol="label")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model and Pipeline
# MAGIC Three important concepts in Spark ML are Transformers, Estimators, and Pipelines.
# MAGIC
# MAGIC [Transformers](https://spark.apache.org/docs/latest/ml-pipeline.html#transformers): Takes a DataFrame as input, and returns a new DataFrame. Transformers do not learn any parameters from the data and simply apply rule-based transformations to either prepare data for model training or generate predictions using a trained MLlib model. You call a transformer with a .transform() method.
# MAGIC
# MAGIC [Estimators](https://spark.apache.org/docs/latest/ml-pipeline.html#estimators): Learns (or "fits") parameters from your DataFrame via a .fit() method and returns a Model, which is a transformer.
# MAGIC
# MAGIC [Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html#pipeline): Combines multiple steps into a single workflow that can be easily run. Creating a machine learning model typically involves setting up many different steps and iterating over them. Pipelines help you automate this process.
# MAGIC
# MAGIC Now let us build our model. We will also create a pipeline that chains the indexer, one hot encoder, vector assembler and the model, so that we can use the pipeline for inference

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
lr_model = LogisticRegression(featuresCol="features", labelCol="label", regParam=1.0, family="binomial")
#lr_model = LogisticRegression(featuresCol="features", labelCol="label", regParam=1.0, family="binomial",link="cloglog")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training

# COMMAND ----------

from pyspark.ml import Pipeline
 
pipeline = Pipeline(stages=[string_indexer, ohe_encoder, vector_assembler, lr_model])

#train the model
pipeline_model = pipeline.fit(train_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Inferencing
# MAGIC Now that we have trained our model with test data, we will test our model by making predictions using test data

# COMMAND ----------

#test the model
test_preds = pipeline_model.transform(test_df)

# COMMAND ----------

full_preds = pipeline_model.transform(data)
full_preds.write.mode("overwrite").saveAsTable("main.I_LOCAL.zos97_pyspark_predict")

# COMMAND ----------

sas_results = spark.table(" main.I_LOCAL.zos97_sas_predict")

# COMMAND ----------

sas_results.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Plot ROC

# COMMAND ----------

display(pipeline_model.stages[-1], test_preds.drop("prediction", "rawPrediction", "probability"), "ROC")

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Evaluate the model using area under ROC curve as the metric
evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label')
auc = evaluator.evaluate(test_preds)
print('Area under ROC curve:', auc)

# COMMAND ----------


