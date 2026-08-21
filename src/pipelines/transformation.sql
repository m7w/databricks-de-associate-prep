CREATE OR REFRESH STREAMING TABLE raw_events (
    CONSTRAINT valid_currency EXPECT (currency IN (
        'USD',
        'BYN'
    )) ON VIOLATION DROP ROW
)
SELECT *
FROM STREAM read_files('/Volumes/${catalog_name}/${schema_name}/files/events', --noqa
format => 'csv');

CREATE OR REFRESH MATERIALIZED VIEW currency_rates
AS
SELECT 'USD' AS currency, 3.04 AS rate
UNION ALL
SELECT 'BYN' AS currency, 1.0 AS rate;

CREATE OR REFRESH STREAMING TABLE enriched_events
AS SELECT
    r.event_id,
    r.customer_id,
    r.amount * c.rate AS amount_byn,
    r.date
FROM STREAM raw_events AS r --noqa
LEFT JOIN currency_rates AS c
ON r.currency = c.currency;

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

CREATE OR REFRESH MATERIALIZED VIEW amount_by_date
AS SELECT date, sum(amount_byn) AS date_total_byn
FROM enriched_events e
GROUP BY date;

CREATE OR REFRESH MATERIALIZED VIEW customer_amount_by_date
AS SELECT name, address, date, sum(amount_byn) AS date_total_byn
FROM enriched_events e
JOIN customers c
ON
e.customer_id = c.customer_id
AND date >= __start_at AND (date < __end_at OR __end_at IS null)
GROUP BY name, address, date;
