# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Source Data Generation
# MAGIC
# MAGIC To be able to demonstrate how running a similar estimator fits in SAS and pyspark and to avoid running examples with PII data, we'll go ahead and repurpose and existing [adult](https://archive.ics.uci.edu/dataset/2/adult) dataset but apply out desired model column names. thus we will do the following mapping:
# MAGIC
# MAGIC | Col Type | Variable Type | Original Name    | Cohort Name       | Comment |
# MAGIC | -------- | ------------- | ---------------- | ----------------- | ------- |
# MAGIC | Feature  | Continuous    | age              | agegrp            | Convert to age groups |
# MAGIC | Feature  | Categorical   | workclass        | COST_LVL          |         |
# MAGIC | Feature  | Continuous    | fnlwgt           | age               | Included to show drop |
# MAGIC | Feature  | Categorical   | education        | COMOR             |         |
# MAGIC | Feature  | Continuous    | education-num    | \<not used\>      | Dropped before save to Delta |
# MAGIC | Feature  | Categorical   | marital-status   | Prev_use          |         |
# MAGIC | Feature  | Categorical   | occupation       | medication_use    |         |
# MAGIC | Feature  | Categorical   | relationship     | CONCO_VAX         |         |
# MAGIC | Feature  | Categorical   | race             | race              |         |
# MAGIC | Feature  | Categorical   | sex              | sex               |         |
# MAGIC | Feature  | Continuous    | capital-gain     | nb_INPAT          |         |
# MAGIC | Feature  | Continuous    | capital-loss     | nb_OUTPT_ER       |         |
# MAGIC | Feature  | Continuous    | hours-per-week   | index_dt          |         |
# MAGIC | Feature  | Categorical   | native-country   | Region            |         |
# MAGIC | Target   | Categorical   | class            | vax               |         |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Import as Pandas DataFrame
# MAGIC
# MAGIC We won't need the source data in raw format, so we can simply call into DataFrame directly from source:
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC
# MAGIC cols = ["agegrp", "COST_LVL", "age", "COMOR", "education-num",
# MAGIC         "Prev_use", "medication_use", "CONCO_VAX", "race", "sex",
# MAGIC         "nb_INPAT", "nb_OUTPT_ER", "index_dt", "Region", "vax"]
# MAGIC
# MAGIC url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
# MAGIC
# MAGIC adult_df = pd.read_csv(url, names=cols) \
# MAGIC              .drop("education-num", axis=1)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transform columns to resemble columns in propensity score matching
# MAGIC
# MAGIC So that column types match our target table schema, we'll add the following:
# MAGIC  - convert agegrp to catagorical column
# MAGIC  - add noise/inaccuracy to the response column (done as discovery for exceptions, not required)
# MAGIC  - convert the vax column to 1/0 (not required, but a common response type for a binary response)
# MAGIC
# MAGIC We'll save source data with transforms to table `main.I_LOCAL.zos97_matching`.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import when, col, rand, lit
# MAGIC
# MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS main.I_LOCAL")
# MAGIC spark.sql("DROP TABLE IF EXISTS main.I_LOCAL.zos97_matching")
# MAGIC
# MAGIC adult_sdf = spark.createDataFrame(adult_df) \
# MAGIC                  .withColumn('agegrp',  when(col('agegrp') < 20, "Under 20")
# MAGIC                                        .when(col('agegrp') < 40, "20-39")
# MAGIC                                        .when(col('agegrp') < 60, "40-59")
# MAGIC                                        .otherwise("60+")) \
# MAGIC                  .withColumn('vax', when(rand() >= 0.90, when(col('vax')==' <=50K',lit(' >50K')).otherwise(' <=50K')) \
# MAGIC                                    .otherwise(col('vax'))) \
# MAGIC                  .withColumn('vax', when(col('vax')==' <=50K',1).otherwise(0)) \
# MAGIC                 .dropna()               
# MAGIC
# MAGIC adult_sdf.write.format("delta").mode("overwrite") \
# MAGIC          .saveAsTable("main.I_LOCAL.zos97_matching")
# MAGIC
# MAGIC display(spark.table("main.I_LOCAL.zos97_matching"))
