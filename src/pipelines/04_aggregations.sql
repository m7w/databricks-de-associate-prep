CREATE OR REFRESH MATERIALIZED VIEW amount_by_date
AS SELECT date, round(sum(amount_byn), 2) AS date_total_byn
FROM enriched_events
GROUP BY date;

CREATE OR REFRESH MATERIALIZED VIEW customer_amount_by_date
AS
SELECT
    coalesce(name, 'Unknown/Deleted') AS name,
    address,
    date,
    round(sum(amount_byn), 2) AS date_total_byn
FROM enriched_events e
LEFT JOIN customers c
    ON
        e.customer_id = c.customer_id
        AND date >= __start_at AND (date < __end_at OR __end_at IS null)
GROUP BY name, address, date;
