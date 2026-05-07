# Databricks notebook source
# spark.conf.set("spark.sql.streaming.stateStore.providerClass",
#                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %run ../utils/common_utils

# COMMAND ----------

config = get_config(env = f"{env}", 
                    path = "../utils/config.json")

# COMMAND ----------

catalog = config.get("catalog")

silver_schema = config.get("silver_schema")
gold_schema = config.get("gold_schema")

silver_volume = config.get("silver_volume")
gold_volume = config.get("gold_volume")

# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{gold_schema}/{gold_volume}/{gold_schema}/checkpoints"
source_table = f"{catalog}.{silver_schema}.silver_invoice_details"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

silver_df = spark.readStream.table(source_table)

# COMMAND ----------

class gold:
    def __init__(self, source_table, catalog, volume_path, watermark_delay, window_time):
        self.source_table = source_table
        self.catalog = catalog
        self.volume_path = volume_path
        self.watermark_delay = watermark_delay
        self.window_time = window_time
    def read_source(self, table):
        return (
            spark.readStream.table(table)
        )
    def store_aggregates(self, df, watermark_delay, window_time):
        agg_df = (df.withWatermark("CreatedTime", f"{watermark_delay}")
            .groupBy(
                window(col("CreatedTime"), f"{window_time}"),
                "StoreID"
            )
            .agg(sum(col("TotalAmount")).alias("Total_Sales"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("StoreID"),
                col("Total_Sales"),
                current_timestamp().alias("ProcessedTime")
            )
        )
        return agg_df
    def upsert_merge(self, microbatchDF, batchId, catalog):
        microbatchDF.createOrReplaceTempView("src")
        query = f"""
            MERGE INTO {catalog}.gold.store_analysis_upsert tgt
            USING src
            ON (tgt.StoreID = src.StoreID) AND
            (tgt.window_start = src.window_start) AND 
            (tgt.window_end = src.window_end)
            WHEN MATCHED THEN
            UPDATE SET
                tgt.Total_Sales = src.Total_Sales,
                tgt.ProcessedTime = src.ProcessedTime
            WHEN NOT MATCHED THEN
            INSERT *
            """
        microbatchDF.sparkSession.sql(query)
    def write_store_analysis(self, agg_df, catalog):
        return(
            agg_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{volume_path}/checkpoint")
            .trigger(availableNow=True)
            .outputMode("append")
            .toTable(f"{catalog}.gold.store_analysis")
        )
    def write_store_analysis_upsert(self, agg_df, catalog):
        return(
            agg_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{volume_path}/checkpoint_1")
            .trigger(availableNow=True)
            .outputMode("update")
            .foreachBatch(lambda microbatchDF, batchId: self.upsert_merge(
                microbatchDF, batchId, catalog)
            )
            .start()
        )
    def Start_Implementation(self):
        silver_data = self.read_source(self.source_table)
        agg_df = self.store_aggregates(silver_data, self.watermark_delay, self.window_time)
        write_query = self.write_store_analysis(agg_df, self.catalog)
        return write_query
    def Start_Implementation_1(self):
        silver_data = self.read_source(self.source_table)
        agg_df = self.store_aggregates(silver_data, self.watermark_delay, self.window_time)
        write_query_1 = self.write_store_analysis_upsert(agg_df, self.catalog)
        print("success")

# COMMAND ----------

#Implementation
gold_obj = gold(source_table, catalog, volume_path, "3 minutes", "30 minutes")
'''
below query is append mode watermark concept --> if specific window end date is less than watermark arrival time then that window wont complete and that record wont available in gold layer
'''
gold_query = gold_obj.Start_Implementation()
gold_query.awaitTermination()

'''
below query is update mode with foreach batch watermark concept --> if specific window end date is less than watermark arrival time then that window will create in final layer but multiple updates takes place in final layer as long new data arrives
'''
gold_query_1 = gold_obj.Start_Implementation_1()



# COMMAND ----------

# display(df.groupBy(window(col("CreatedTime"), "15 minutes"),
#     "StoreID", "CustomerType", "CustomerCardNo").agg(
#     sum(col("TotalAmount")).alias("Total_Sales")
# ))
