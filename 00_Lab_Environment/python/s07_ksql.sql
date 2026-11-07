-- S07 lab: ksqlDB over the orders topic. Run in the ksqlDB CLI (make ksql), statement by statement.
-- The first statement makes every query read each topic from the beginning:
SET 'auto.offset.reset' = 'earliest';
-- Input is JSON. The record key is order_id (the producer keys every record by order_id).
-- event_time is an ISO string; S10 turns it into a real timestamp in Spark.

CREATE STREAM orders_raw (
  order_id VARCHAR KEY,
  event_id VARCHAR,
  customer_id VARCHAR,
  store_id VARCHAR,
  event_type VARCHAR,
  amount DOUBLE,
  event_time VARCHAR
) WITH (KAFKA_TOPIC='orders.events.v1', VALUE_FORMAT='JSON', PARTITIONS=3);

-- Step 1: keep only paid events (a filter). Derived stream, written to its own topic.
CREATE STREAM payments AS
  SELECT * FROM orders_raw
  WHERE event_type = 'PaymentAccepted' AND amount IS NOT NULL
  EMIT CHANGES;

-- Step 2: revenue per store per hour (a tumbling window aggregate).
-- Windows here use the Kafka record timestamp (the time the producer sent the record),
-- not event_time. This is why S10 teaches event time and watermarks.
CREATE TABLE revenue_by_store_hour AS
  SELECT store_id, SUM(amount) AS revenue, COUNT(*) AS payments
  FROM payments
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY store_id
  EMIT CHANGES;

-- Step 3: watch the table update live (push query). Stop with Ctrl+C, or LIMIT n.
SELECT store_id, revenue, payments FROM revenue_by_store_hour EMIT CHANGES LIMIT 5;

-- Step 4: enrich each payment with the customer's region (a stream-table join).
-- The customer table comes from the topic customers.v1, loaded in the lab from data/customers.csv.
CREATE TABLE customers (
  customer_id VARCHAR PRIMARY KEY,
  region VARCHAR,
  updated_at VARCHAR
) WITH (KAFKA_TOPIC='customers.v1', VALUE_FORMAT='DELIMITED', PARTITIONS=3);

-- Join needs the same key on both sides, so payments are repartitioned by customer_id first.
CREATE STREAM payments_by_customer AS
  SELECT * FROM payments PARTITION BY customer_id
  EMIT CHANGES;

CREATE STREAM payments_enriched AS
  SELECT p.customer_id, p.store_id, p.amount, c.region
  FROM payments_by_customer p
  LEFT JOIN customers c ON p.customer_id = c.customer_id
  EMIT CHANGES;

-- Check the enrichment: push query, stop with Ctrl+C or change LIMIT.
SELECT customer_id, region, amount FROM payments_enriched EMIT CHANGES LIMIT 5;
