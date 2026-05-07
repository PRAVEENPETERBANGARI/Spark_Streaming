CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} 
(
  InvoiceNumber STRING,
  CreatedTime BIGINT,
  StoreID STRING,
  PosID STRING,
  CashierID STRING,
  CustomerType STRING,
  CustomerCardNo STRING,
  TotalAmount DOUBLE,
  NumberOfItems BIGINT,
  PaymentMethod STRING,
  TaxableAmount DOUBLE,
  CGST DOUBLE,
  SGST DOUBLE,
  CESS DOUBLE,
  DeliveryType STRING,
  DeliveryAddress STRUCT<
    AddressLine: STRING,
    City: STRING,
    ContactNumber: STRING,
    PinCode: STRING,
    State: STRING
  >,
  InvoiceLineItems ARRAY<STRUCT<
    ItemCode: STRING,
    ItemDescription: STRING,
    ItemPrice: DOUBLE,
    ItemQty: BIGINT,
    TotalValue: DOUBLE
  >>,
  _rescued_data STRING,
  IngestionTime TIMESTAMP NOT NULL,
  InputFilePath STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
)