-- ============================================================
-- Task 2: Windowing TVFs & Real-Time Revenue Aggregation
-- ============================================================

-- ============================================================
-- 1. Kafka Source
-- ============================================================

CREATE TABLE ecommerce_events (
    event_time STRING,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DOUBLE,
    user_id BIGINT,
    user_session STRING,

    -- Convert string event time to Flink Event Time
    event_ts AS TO_TIMESTAMP(
        REPLACE(event_time, ' UTC', '')
    ),

    -- Allow events to arrive up to 5 seconds late
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'kafka-flink-topic',
    'properties.bootstrap.servers' = 'kafka:9092',    --  ports: - "9092:9092"       
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- ============================================================
-- 2. Print Sink
-- ============================================================

CREATE TABLE brand_window_sales (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    brand STRING,
    total_orders BIGINT,
    gross_revenue DECIMAL(20, 2),
    avg_order_value DOUBLE,
    unique_buyers BIGINT
) WITH (
    'connector' = 'print'
);

-- ============================================================
-- 3. Five-minute Tumbling Window Aggregation
-- ============================================================

INSERT INTO brand_window_sales

SELECT
    window_start,
    window_end,
    brand,

    COUNT(*) AS total_orders,

    CAST(
        ROUND(SUM(price), 2)
        AS DECIMAL(20, 2)
    ) AS gross_revenue,

    CAST(
        AVG(price)
        AS DOUBLE
    ) AS avg_order_value,

    COUNT(DISTINCT user_id) AS unique_buyers

FROM TABLE(
    TUMBLE(
        TABLE ecommerce_events,
        DESCRIPTOR(event_ts),
        INTERVAL '5' MINUTE
    )
)

WHERE event_type = 'purchase'

GROUP BY
    window_start,
    window_end,
    brand;