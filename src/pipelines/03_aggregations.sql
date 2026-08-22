CREATE OR REFRESH MATERIALIZED VIEW amount_by_date
AS SELECT date, sum(amount_byn) AS date_total_byn
FROM enriched_events
GROUP BY date;

CREATE OR REFRESH MATERIALIZED VIEW customer_amount_by_date
AS SELECT name, address, date, sum(amount_byn) AS date_total_byn
FROM enriched_events e
JOIN customers c
    ON
        e.customer_id = c.customer_id
        AND date >= __start_at AND (date < __end_at OR __end_at IS null)
GROUP BY name, address, date;
