// Databricks notebook source
// MAGIC %python
// MAGIC import urllib
// MAGIC urllib.urlretrieve("https://s3-ap-southeast-2.amazonaws.com/variant-spark/datasets/hipsterIndex/hipster.vcf", "/tmp/hipster.vcf")
// MAGIC urllib.urlretrieve("https://s3-ap-southeast-2.amazonaws.com/variant-spark/datasets/hipsterIndex/hipster_labels.txt", "/tmp/hipster_labels.txt")
// MAGIC dbutils.fs.mv("file:/tmp/hipster.vcf", "dbfs:/vs-datasets/hipsterIndex/hipster.vcf")
// MAGIC dbutils.fs.mv("file:/tmp/hipster_labels.txt", "dbfs:/vs-datasets/hipsterIndex/hipster_labels.txt")
// MAGIC display(dbutils.fs.ls("dbfs:/vs-datasets/hipsterIndex"))

// COMMAND ----------

import au.csiro.variantspark.api.VSContext
import au.csiro.variantspark.api.ImportanceAnalysis
implicit val vsContext = VSContext(spark)

val featureSource = vsContext.featureSource("/vs-datasets/hipsterIndex/hipster.vcf")
val labelSource  = vsContext.labelSource("/vs-datasets/hipsterIndex/hipster_labels.txt", "label")
val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = 1000)
val variableImportance = importanceAnalysis.variableImportance
variableImportance.cache().registerTempTable("importance")
display(variableImportance)

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC importance = sqlContext.sql("select * from importance order by importance desc limit 25")
// MAGIC importanceDF = importance.toPandas()
// MAGIC ax = importanceDF.plot(x="variable", y="importance",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
// MAGIC ax.set_xlabel("variable")
// MAGIC ax.set_ylabel("importance")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.grid(True)
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Credit
// MAGIC
// MAGIC CSIRO Transformational Bioinformatics team has developed VariantSpark and put together this illustrative example. Thank you to Lynn Langit for input on the presentation of this notebook. 
