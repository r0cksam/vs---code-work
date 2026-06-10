CREATE DATABASE IF NOT EXISTS veto_live;

CREATE TABLE IF NOT EXISTS veto_live.live_ts_detail
(
    log_date Date,
    source LowCardinality(String),
    minute_utc DateTime,
    minute_ist DateTime,
    reqHost LowCardinality(String),
    platform_key LowCardinality(String),
    platform_name LowCardinality(String),
    candidate_id String,
    channel_name LowCardinality(String),
    status_code LowCardinality(String),
    cliIP String,
    UA String,
    source_file_hash String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(log_date)
ORDER BY
(
    log_date,
    source,
    minute_ist,
    platform_name,
    channel_name,
    status_code
)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS veto_live.live_ingested_files
(
    source LowCardinality(String),
    source_file_path String,
    source_file_hash String,
    file_signature String,
    processed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (source, source_file_path);

CREATE VIEW IF NOT EXISTS veto_live.live_minute_view AS
SELECT
    log_date,
    source,
    minute_ist,
    platform_name,
    channel_name,
    count() AS raw_ts_rows,
    countIf(status_code = '200') AS status_200_ts_rows,
    round(count() / 10, 3) AS estimated_viewers_all_status,
    round(countIf(status_code = '200') / 10, 3) AS estimated_viewers_http_200,
    uniqExact(nullIf(cliIP, '')) AS approx_ips,
    uniqExact(nullIf(UA, '')) AS unique_user_agents
FROM veto_live.live_ts_detail
GROUP BY
    log_date,
    source,
    minute_ist,
    platform_name,
    channel_name;

CREATE VIEW IF NOT EXISTS veto_live.live_status_minute_view AS
SELECT
    log_date,
    source,
    minute_ist,
    platform_name,
    channel_name,
    status_code,
    count() AS status_ts_rows,
    round(count() / 10, 3) AS estimated_viewers
FROM veto_live.live_ts_detail
GROUP BY
    log_date,
    source,
    minute_ist,
    platform_name,
    channel_name,
    status_code;
