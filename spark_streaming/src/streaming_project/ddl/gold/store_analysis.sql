CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table}
(
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  StoreID STRING,
  Total_Sales DOUBLE,
  ProcessedTime TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
)