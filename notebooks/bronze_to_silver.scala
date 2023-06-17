// Databricks notebook source
// MAGIC %md <h3> Conferindo acesssos</h3>

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md <h3> Lendo os dados</h3>

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md <h3> Transformando Json em colunas</h3>

// COMMAND ----------

display(df.select("anuncio.*"))


// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")
)

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md <H3> Removendo colunas</H3>

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")

// COMMAND ----------

display(df_silver)

// COMMAND ----------

// MAGIC %md <H3> Salvando camada Silver</H3>

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


