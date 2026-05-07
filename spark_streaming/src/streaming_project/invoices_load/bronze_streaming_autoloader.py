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
bronze_volume = config.get("bronze_volume")

volume_path = f"/Volumes/{catalog}/{bronze_schema}/{bronze_volume}"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

class Bronze_Load:
    def __init__(self, volume_path, bronze_layer, ingested_layer):
        self.volume_path = volume_path
        self.bronze_layer = bronze_layer
        self.ingested_layer = ingested_layer
        self.schema_location = f"{self.volume_path}/{self.bronze_layer}/schemas"
        self.checkpoint_location = f"{self.volume_path}/{self.bronze_layer}/checkpoints"
        self.file_path = (
            f"{self.volume_path}/{self.ingested_layer}/invoices_data/*.json"
        )

    def get_bronze_data(self, schema, schema_location, file_path):
        input_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .schema(schema)
            .load(file_path)
        )

        input_df = input_df.withColumn("IngestionTime", current_timestamp()).withColumn(
            "InputFilePath", col("_metadata.file_path")
        )
        return input_df

    def get_schema(self):
        schema = StructType([
        StructField('InvoiceNumber', StringType(), True),
        StructField('CreatedTime', LongType(), True),
        StructField('StoreID', StringType(), True),
        StructField('PosID', StringType(), True),
        StructField('CashierID', StringType(), True),
        StructField('CustomerType', StringType(), True),
        StructField('CustomerCardNo', StringType(), True),
        StructField('TotalAmount', DoubleType(), True),
        StructField('NumberOfItems', LongType(), True),
        StructField('PaymentMethod', StringType(), True),
        StructField('TaxableAmount', DoubleType(), True),
        StructField('CGST', DoubleType(), True),
        StructField('SGST', DoubleType(), True),
        StructField('CESS', DoubleType(), True),
        StructField('DeliveryType', StringType(), True),
        StructField('DeliveryAddress', StructType([
            StructField('AddressLine', StringType(), True),
            StructField('City', StringType(), True),
            StructField('ContactNumber', StringType(), True),
            StructField('PinCode', StringType(), True),
            StructField('State', StringType(), True)
        ]), True),
        StructField('InvoiceLineItems', ArrayType(StructType([
            StructField('ItemCode', StringType(), True),
            StructField('ItemDescription', StringType(), True),
            StructField('ItemPrice', DoubleType(), True),
            StructField('ItemQty', LongType(), True),
            StructField('TotalValue', DoubleType(), True)
        ])), True)
        ])
        return schema

    def write_stream_bronze(self, input_df, processing_time, checkpoint_location, table):
        write_query = (
            input_df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            # .trigger(processingTime=f"{processing_time}")
            .trigger(availableNow=True)
            .queryName("bronze_invoice_details")
            .toTable(f"{table}")
        )
        return write_query

    def start_implementation(self, processing_time, table):

        bronze_data = self.get_bronze_data(self.get_schema(), self.schema_location, self.file_path)
        print("Bronze Data")
        write_bronze_data = self.write_stream_bronze(
            bronze_data, processing_time, self.checkpoint_location, table
        )
        print("Write Stream Bronze Data")
        return write_bronze_data

# COMMAND ----------

## Implementation
bronze_process = Bronze_Load(volume_path, "bronze", "landing")
bronze_query = bronze_process.start_implementation(processing_time= "30 seconds",
                                    table = f"{catalog}.{bronze_schema}.bronze_invoice_details")
bronze_query.awaitTermination()
