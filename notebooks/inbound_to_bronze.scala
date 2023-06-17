// Databricks notebook source
// MAGIC %md **Conferindo se os dados foram montados e o acesso a pasta inbound**
// MAGIC

// COMMAND ----------

// MAGIC %python 
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

// MAGIC %md **<h2> Lendo os dados da camada Inbound </h2>**
// MAGIC

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md <h2>Removendo Colunas</h2>

// COMMAND ----------

val dados_anuncio = dados.drop("imagens","usuario")

// COMMAND ----------

display(dados_anuncio)

// COMMAND ----------

// MAGIC %md <H2>Criando uma coluna de identificação</H2>

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md <h2>Salvando a camda bronze</h2>

// COMMAND ----------

val path =  "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


