# Databricks notebook source
df = spark.read.format("csv",)\
    .option("header",True)\
    .option("inferschema",True)\
    .load("/Volumes/pysparkdbt/source/source_data/customers/")

# COMMAND ----------

schema_customers = df.schema
schema_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Spark Streaming**

# COMMAND ----------

entities = ['customers','trips','locations','payments','vehicles','drivers']

# COMMAND ----------

for entity in entities:
        df_batch = spark.read.format("csv",)\
        .option("header",True)\
        .option("inferschema",True)\
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}/")

        schema_entity = df_batch.schema

        df = spark.readStream.format("csv")\
        .option("header",True)\
        .schema(schema_entity)\
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}/") 

        df.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"/Volumes/pysparkdbt/bronze/checkpoint/{entity}")\
            .trigger(once=True)\
            .toTable(f"pysparkdbt.bronze.{entity}")