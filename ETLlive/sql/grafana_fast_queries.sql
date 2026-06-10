-- FAST near-live viewer trend by channel/platform.
-- Grafana time field: minute_ist
SELECT
    minute_ist AS time,
    concat(platform_name, ' / ', channel_name) AS series,
    estimated_viewers_all_status AS viewers_all_status,
    estimated_viewers_http_200 AS viewers_http_200
FROM veto_live.live_minute_view
WHERE source = 'fast'
  AND $__timeFilter(minute_ist)
ORDER BY time, series;

-- FAST status-code trend.
SELECT
    minute_ist AS time,
    status_code,
    sum(status_ts_rows) AS ts_rows,
    sum(estimated_viewers) AS estimated_viewers
FROM veto_live.live_status_minute_view
WHERE source = 'fast'
  AND $__timeFilter(minute_ist)
GROUP BY time, status_code
ORDER BY time, status_code;

-- FAST latest freshness.
SELECT
    max(minute_ist) AS latest_processed_minute_ist,
    dateDiff('minute', max(minute_ist), now('Asia/Kolkata')) AS delay_minutes
FROM veto_live.live_ts_detail
WHERE source = 'fast';

-- FAST top channels in selected range.
SELECT
    channel_name,
    platform_name,
    round(count() * 6 / 3600, 3) AS raw_watch_hours,
    round(countIf(status_code = '200') * 6 / 3600, 3) AS status_200_watch_hours,
    uniqExact(nullIf(cliIP, '')) AS unique_ips,
    uniqExact(nullIf(UA, '')) AS unique_user_agents
FROM veto_live.live_ts_detail
WHERE source = 'fast'
  AND $__timeFilter(minute_ist)
GROUP BY channel_name, platform_name
ORDER BY raw_watch_hours DESC;
