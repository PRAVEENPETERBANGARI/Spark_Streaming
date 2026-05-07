# Databricks notebook source
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

bronze_schema = config.get("bronze_schema")
silver_schema = config.get("silver_schema")

bronze_volume = config.get("bronze_volume")
silver_volume = config.get("silver_volume")

# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{silver_schema}/{silver_volume}"
source_table = f"{catalog}.{bronze_schema}.bronze_invoice_details"
target_table = f"{catalog}.{silver_schema}.silver_invoice_details"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

class Silver_Load:
    def __init__(self, volume_path, layer, source_table, target_table):
        self.volume_path = volume_path
        self.layer = layer
        self.checkpoint_location = f"{self.volume_path}/{self.layer}/checkpoints"
        self.source_table = source_table
        self.target_table = target_table

    def read_bronze_data(self, table):
        input_df = spark.readStream.table(table)
        return input_df

    def transform_flatten_data(self, input_df):
        input_df = input_df.select(
        col("InvoiceNumber"),
        (col("CreatedTime")/1000).cast("timestamp").alias("CreatedTime"),
        col("StoreID"),
        col("PosID"),
        col("CashierID"),
        col("CustomerType"),
        col("CustomerCardNo"),
        col("TotalAmount"),
        col("PaymentMethod"),
        col("DeliveryType"),
        col("InputFilePath"),
        col("DeliveryAddress.AddressLine").alias("Address")
        ,col("DeliveryAddress.City").alias("City")
        ,col("DeliveryAddress.ContactNumber").alias("ContactNumber")
        ,col("DeliveryAddress.PinCode").alias("PinCode")
        ,col("DeliveryAddress.State").alias("State")
        , explode(col("InvoiceLineItems")).alias("InvoiceLineItems")
        )

        flatten_df = input_df.withColumn("ItemCode", col("InvoiceLineItems.ItemCode"))\
        .withColumn("ItemDescription", col("InvoiceLineItems.ItemDescription"))\
        .withColumn("ItemPrice", col("InvoiceLineItems.ItemPrice"))\
        .withColumn("ItemQty", col("InvoiceLineItems.ItemQty"))\
        .withColumn("TotalValue", col("InvoiceLineItems.TotalValue"))\
        .drop("InvoiceLineItems")\
        .withColumn("FileName", substring_index(
                                    col("InputFilePath"), "/", -1))\
        .withColumn("ProcessedTime", current_timestamp())\
        .drop("InputFilePath")
        return flatten_df

    def write_stream_silver(self, flatten_df, processing_time, checkpoint_location, table):
        write_query = (
        flatten_df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            # .trigger(processingTime = f"{processing_time}")
            .trigger(availableNow=True)
            .queryName("bronze_invoice_details")
            .toTable(f"{table}")
        )
        return write_query

    def start_implementation(self, processing_time):

        bronze_data = self.read_bronze_data(self.source_table)
        print("Read Bronze Data")
        flatten_data = self.transform_flatten_data(bronze_data)
        print("Transformed and Flatten Data")
        write_silver_data = self.write_stream_silver(flatten_data, processing_time,
                            self.checkpoint_location, self.target_table)
        print("Write Stream Silver Data")
        return write_silver_data

# COMMAND ----------

## Implementation
silver_process = Silver_Load(volume_path = volume_path, layer = "silver",
                             source_table = source_table,
                             target_table = target_table)
# df = silver_process.transform_flatten_data(silver_process.read_bronze_data(silver_process.source_table))
# print(df.printSchema())
silver_query = silver_process.start_implementation(processing_time= "30 seconds")
silver_query.awaitTermination()
