CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table}
(
  InvoiceNumber STRING,
  CreatedTime TIMESTAMP,
  StoreID STRING,
  PosID STRING,
  CashierID STRING,
  CustomerType STRING,
  CustomerCardNo STRING,
  TotalAmount DOUBLE,
  PaymentMethod STRING,
  DeliveryType STRING,
  Address STRING,
  City STRING,
  ContactNumber STRING,
  PinCode STRING,
  State STRING,
  ItemCode STRING,
  ItemDescription STRING,
  ItemPrice DOUBLE,
  ItemQty BIGINT,
  TotalValue DOUBLE,
  FileName STRING,
  ProcessedTime TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
)