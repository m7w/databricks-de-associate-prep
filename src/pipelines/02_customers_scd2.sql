CREATE OR REFRESH STREAMING TABLE customer_updates
AS
SELECT *
FROM STREAM read_files('/Volumes/${catalog_name}/${schema_name}/files/customers', --noqa
format => 'csv');

CREATE OR REFRESH STREAMING TABLE customers;

CREATE FLOW customers_scd2_flow AS
AUTO CDC INTO customers
FROM STREAM customer_updates
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY process_date
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 2;
