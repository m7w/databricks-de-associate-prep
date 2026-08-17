CREATE OR REFRESH STREAMING TABLE raw_events (
    CONSTRAINT valid_currency EXPECT (currency IN (
        'USD',
        'BYN'
    )) ON VIOLATION DROP ROW
)
SELECT * FROM STREAM read_files('/Volumes/${catalog_name}/${schema_name}/files',
format => 'csv');

CREATE OR REFRESH MATERIALIZED VIEW currency_rates
AS
SELECT 'USD' AS currency, 3.04 AS rate
UNION ALL
SELECT 'BYN' AS currency, 1.0 AS rate;

CREATE OR REFRESH STREAMING TABLE enriched_events
AS SELECT
    r.event_id,
    r.amount * c.rate AS amount_byn,
    r.date
FROM STREAM raw_events AS r
LEFT JOIN currency_rates AS c
    ON r.currency = c.currency;

CREATE OR REFRESH MATERIALIZED VIEW amount_by_date
AS SELECT date, sum(amount_byn) AS date_total_byn
FROM enriched_events
GROUP BY date;
