# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install dlt-meta

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_date

def custom_transform(input_df, _) -> DataFrame:
    layer = spark.conf.get("layer", None)
    if layer == "bronze":
        dummy_param = spark.conf.get("dummy_param", None)
    else:
        dummy_param = "Test NA"

    return (
        input_df
        .withColumn("last_updated_on", current_date())
        .withColumn("some_dummy_from_task_param", lit(dummy_param))
    )

# COMMAND ----------
layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer,
    bronze_custom_transform_func=custom_transform,
    silver_custom_transform_func=custom_transform,
)
