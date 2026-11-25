CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                     'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS analytics.daily_metrics (
    date Date,
    metric_name LowCardinality(String),
    dimension String,
    value Float64,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, metric_name, dimension);

CREATE TABLE IF NOT EXISTS analytics.cohort_retention (
    cohort_date Date,
    day_number UInt8,
    cohort_size UInt64,
    returned_users UInt64,
    retention_percent Float64,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number);

CREATE TABLE IF NOT EXISTS analytics.kafka_events (
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker-1:9092,broker-2:9092,broker-3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'clickhouse-movie-events-v1',
         kafka_format = 'AvroConfluent',
         kafka_num_consumers = 3,
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.events_ingest_mv TO analytics.raw_events AS
SELECT toUUID(event_id) AS event_id,
       user_id,
       movie_id,
       event_type,
       timestamp,
       device_type,
       session_id,
       progress_seconds
FROM analytics.kafka_events;
