# ETL Dashboard Data Catalog

Generated from a deep scan of `ETL/src`, `ETL/data`, and `ETL/output` on 2026-06-19.

This file is the first place to check before building a new dashboard. It lists the reusable parquet, CSV, Excel, JSON, and HTML artifacts already produced by the ETL, what each artifact means, and which dashboard currently uses it.

## Rules Of Use

- Prefer the reusable parquet marts under `output/*` before scanning `data/lake`.
- Do not use `output/temp/*` for production dashboards unless a new mart is intentionally promoted.
- Do not use `output/*/parts/*` directly in dashboards. Those are incremental build parts; use the merged table in the parent folder.
- If a metric needs exact platform + channel + region/device splits, verify the mart grain first. Host-level FAST data is often platform-wide and can overstate channel-specific values.
- `raw_watch_hours` and request watch-hour metrics are based on `.ts` segment rows multiplied by 6 seconds unless a dashboard explicitly says otherwise.
- Approximate IP/session/device counts are usually daily distinct sums and can overlap across days or channels.

## Core Data Flow

| Layer | Location | What It Contains | Notes |
|---|---|---|---|
| Raw download | `data/raw/Veto Logs Backup/Veto Stream Backup` and `data/raw/Veto Logs Backup/Veto fast Backup` | Daily `.gz` CDN logs copied from Linode/Object Storage | Used by early ETL steps, not dashboards |
| Stage parquet | `data/stage/parquet/source=.../year=.../month=.../day=...` | Converted parquet from raw logs | Intermediate |
| Final clean stage | `data/stage/final_clean/source=...` | Cleaned daily parquet before lake append | Intermediate |
| Lake | `data/lake/source=fast|stream/year=YYYY/month=MM/day=DD/*.parquet` | Production partitioned parquet lake, IST partitioned | Main raw analytical base |
| Output marts | `output/*` | Reusable dashboard-ready marts | Prefer these for dashboards |
| Dashboard HTML/XLSX | `output/*/*.html`, `output/*/*.xlsx` | Stakeholder artifacts | Regenerated from output marts |

## Dashboard Outputs

| Dashboard | Output | Main Inputs |
|---|---|---|
| Overview Dashboard | `output/overview/overview_dashboard.html` | `output/overview/*.csv`, `output/overview/overview_report.xlsx` |
| Veto Watch Hours | `output/watch_hours/veto_watch_hours.html` | `output/watch_hours/profile/*.parquet`, `output/watch_hours/daily_tables/*.parquet`, concurrency, device decode |
| Veto Concurrency | `output/watch_hours/concurrency/veto_concurrency.html` | `output/watch_hours/concurrency/*.parquet` |
| Veto Latency | `output/latency/veto_latency.html` | `output/latency/profile/*.parquet` |
| Veto Audience Operations | `output/audience_ops/veto_audience_operations.html` | Watch-hour daily tables, latency profile, concurrency, identity, content, device decode, overview daily CSV |
| STREAM Showcase | `output/watch_hours/stream_showcase/stream_watch_hours_showcase.html` | `daily_volume.parquet`, `geo_daily.parquet` |
| STREAM Showcase Excel | `output/watch_hours/stream_showcase/stream_watch_hours_showcase.xlsx` | Same as STREAM Showcase |
| Viewer Journey Menu | `output/exports/viewer_journey/viewer_journey_menu.html` | `output/exports/viewer_journey/viewer_journey_index.parquet` |
| Single cliIP Journey | `output/exports/cliip_journey/<export_name>/...` | Raw cliIP parquet exports |
| UA Decode Preview | `output/device_decode/ua_decode_dashboard_preview.html` | UA decode lookup/cache files |

## Watch-Hour Daily Tables

Folder: `output/watch_hours/daily_tables`

These are the most reusable mart tables for new dashboards.

| File | Rows At Scan | Source/Range | Grain | What It Means | Current Users |
|---|---:|---|---|---|---|
| `daily_volume.parquet` | 125 | fast, stream / 2026-03-25 to 2026-06-19 | date + source | Total request rows, status 200/non-200, `.ts` rows, m3u8 rows, approximate unique IPs | Watch Hours, Audience Ops, Stream Showcase |
| `channel_audience_daily.parquet` | 2,306 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + channel | Channel watch hours, status 200 hours, approx IP/session/device | Watch Hours, Audience Ops |
| `geo_daily.parquet` | 16,412 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + country + state + city | Geography watch hours and approx IPs | Watch Hours, Audience Ops, Stream Showcase |
| `channel_geo_daily.parquet` | 110,056 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + country + state + city + channel | Channel watch hours by geography | Watch Hours, Audience Ops |
| `region_channel_audience_daily.parquet` | 91,789 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + country + state + channel | Channel watch hours and approx IP/session/device by region | Watch Hours, possible Audience Ops expansion |
| `device_type_by_channel_daily.parquet` | 8,984 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + channel + device_type | Channel `.ts` rows and approx IPs by coarse device type | Watch Hours, Audience Ops |
| `region_channel_device_daily.parquet` | 129,811 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + country + state + channel + device_type | Region + channel + device type watch hours | Watch Hours, Audience Ops |
| `status_codes_daily.parquet` | 959 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + statusCode | Status code rows, `.ts` rows, watch hours, sample path | Watch Hours, reliability sections |
| `hosts_daily.parquet` | 1,645 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + reqHost | Host traffic, `.ts` hours, status split, approx IPs | Watch Hours |
| `cache_daily.parquet` | 3,049 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + host + cacheStatus + cacheable | Cache status/cacheability traffic | Watch Hours |
| `errors_daily.parquet` | 9,875 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + host + status/error/startupError | Error/debug rows with sample path | Watch Hours |
| `extensions_daily.parquet` | 739 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + extension | Request volume by file extension (`ts`, `m3u8`, etc.) | Watch Hours |
| `asn_daily.parquet` | 95,442 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + ASN | ASN/network watch hours and approx IPs | Watch Hours |
| `user_agents_daily.parquet` | 138,173 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + userAgent | UA usage, `.ts` rows, approx IPs, distinct hosts | Watch Hours, Audience Ops device decode |
| `mapping_quality_daily.parquet` | 17,178 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + host + candidate_id + channel + quality | Mapping QA by channel candidate | Watch Hours |
| `unmapped_candidates_daily.parquet` | 2 | stream / 2026-06-17 to 2026-06-18 | date + source + host + candidate_id | Remaining unmapped channel candidates | Watch Hours mapping QA |
| `query_params_daily.parquet` | 125 | fast, stream / 2026-03-25 to 2026-06-19 | date + source | Presence counts for query string keys such as session/device/content/m | Watch Hours, Audience Ops sanity |
| `query_param_keys_daily.parquet` | 1,513 | fast, stream / 2026-03-25 to 2026-06-19 | date + source + param_key | Distinct query parameter keys with sample values | Watch Hours |
| `query_m_channel_daily.parquet` | 618 | fast, stream / 2026-03-26 to 2026-06-19 | date + source + m_value + channel | `m=` parameter to channel mapping evidence | Watch Hours |
| `cmcd_daily.parquet` | 41 | fast, stream / 2026-03-25 to 2026-06-19 | date + source | CMCD parameter presence counts | Watch Hours |

## Watch-Hour Profile Tables

Folder: `output/watch_hours/profile`

These are compact top-level/profile tables generated by the deep profile step. Watch Hours reads these first, preferring parquet over CSV where both exist.

| File | What It Means | Notes |
|---|---|---|
| `channel_summary.parquet` | Overall channel summary: raw/status 200 hours, approx IPs, first/last seen | Required by Watch Hours |
| `channel_daily.parquet` | Daily channel trend summary | Required by Watch Hours |
| `daily_volume.parquet` | Daily source/profile volume | Required by Watch Hours |
| `status_codes.parquet` | Overall status code mix with meanings in UI | Required by Watch Hours |
| `file_inventory.parquet` | Lake/profile file inventory by date/source | Required by Watch Hours |
| `hosts_overview.parquet` | Host volume/status/watch-hour overview | Host sections |
| `cache_by_host.parquet` | Cache split by host | Reliability |
| `errors_by_host.parquet` | Error/status/startup failures by host | Reliability/debug |
| `performance_by_host_extension.parquet` | TTFB/transfer/turnaround/throughput by host/extension | Older performance view |
| `geo_top.parquet` | Top geography watch-hour rows | Geography |
| `asn_top.parquet` | Top ASN/network rows | Network |
| `ua_top.parquet` | Top user agents | Device/UA review |
| `device_type_by_channel.parquet` | Overall channel device type table | Device sections |
| `path_candidate_quality.parquet` | Channel mapping candidates and quality buckets | Mapping QA |
| `unmapped_candidates.parquet` | Remaining unmapped candidates | Mapping QA |
| `querystr_param_presence.parquet` | Query parameter presence summary | QueryStr/identity checks |
| `querystr_channel_profile.parquet` | QueryStr channel evidence | Currently 0 rows at scan |
| `cmcd_presence.parquet` and `cmcd_top_values.parquet` | CMCD presence/top values | CMCD diagnostics |
| `extensions.parquet`, `host_extension.parquet` | Extension-level traffic | Request type diagnostics |
| `schema.parquet`, `column_fill_rate.parquet` | Lake schema and non-empty column analysis | Data quality |

## FAST Concurrency Tables

Folder: `output/watch_hours/concurrency`

| File | Rows At Scan | Grain | What It Means | Users |
|---|---:|---|---|---|
| `concurrency_minute.parquet` | 611,809 | minute + platform + channel + host | FAST minute-level active viewer estimates by cliIP and UA, plus `.ts` rows | Concurrency dashboard, Watch Hours, Audience Ops |
| `concurrency_status_minute.parquet` | 743,407 | minute + platform + channel + host + status_code | Status-code split for concurrency minute rows | Concurrency dashboard |
| `concurrency_summary.parquet` | 608 | date + platform + channel + host | Daily peak/average/p95 active viewers and row totals | Concurrency dashboard, Audience Ops |
| `fast_platform_channel_identity_daily.parquet` | 608 | date + platform + channel + host | Daily distinct cliIP, UA, cliIP+UA at platform+channel grain | Audience Ops |
| `fast_platform_channel_geo_daily.parquet` | 21,799 | date + platform + channel + host + country + state + city | FAST platform/channel geography watch hours and approx IPs from `.ts` rows | Audience Ops |
| `fast_platform_channel_ua_device_daily.parquet` | 69,819 | date + platform + channel + host + decoded UA/device labels | FAST platform/channel device type, OS, brand, model, form factor, decode quality, watch hours, and approx IPs | Audience Ops |
| `fast_platform_channel_manifest_daily.parquet` | 647 | date + platform + channel + host + manifest kind + channel evidence | FAST platform/channel `.m3u8` manifest request views from row-level path evidence | Audience Ops |
| `fast_platform_channel_bandwidth_daily.parquet` | 608 | date + platform + channel + host + channel evidence | FAST platform/channel exact `.ts` `totalBytes`, body bytes, response length, and byte coverage | Audience Ops |
| `fast_platform_channel_cmcd_daily.parquet` | 608 | date + platform + channel + host + channel evidence | FAST CMCD sid/cid playback-session coverage, daily distinct sid/cid, and CMCD duration | Audience Ops |
| `concurrency_cliip_viewers.parquet` | 611,809 | timestamp + channel + platform | CSV-style export: number of viewers by cliIP | Concurrency dashboard/export |
| `concurrency_ua_viewers.parquet` | 611,809 | timestamp + channel + platform | CSV-style export: number of viewers by UA | Concurrency dashboard/export |
| `concurrency_cliip_viewers.csv` | large | same as parquet | Stakeholder-style CSV export | Export only |
| `concurrency_ua_viewers.csv` and `.xlsx` | large | same as parquet | Stakeholder-style UA export | Export only |
| `concurrency_manifest.json` | n/a | run metadata | Date range, source, metric definitions | Pipeline/debug |

Important: FAST platform + channel exact IP metrics should use `fast_platform_channel_identity_daily.parquet`, not host-level latency IPs. FAST platform + channel geography should use `fast_platform_channel_geo_daily.parquet`. FAST platform + channel decoded device and OS views should use `fast_platform_channel_ua_device_daily.parquet`. FAST platform + channel manifest views should use `fast_platform_channel_manifest_daily.parquet`, not host-level `.m3u8` latency rows. FAST platform + channel video bandwidth should use `fast_platform_channel_bandwidth_daily.parquet`, not `rows * avg_total_bytes` host estimates. FAST CMCD playback sessions should use `fast_platform_channel_cmcd_daily.parquet` and must not be relabeled as app `session_id`.

## Latency Profile Tables

Folder: `output/latency/profile`

| File | Rows At Scan | Grain | What It Means | Users |
|---|---:|---|---|---|
| `daily.parquet` | 586 | date + source + extension | CDN latency/reliability by extension | Latency, Audience Ops |
| `hourly.parquet` | 6,616 | date + hour + source + extension | Hourly TTFB/cache/status trend | Latency, Audience Ops |
| `channel_daily.parquet` | 5,320 | date + source + extension + channel | Channel latency and error rates | Latency, Audience Ops |
| `host_daily.parquet` | 3,567 | date + source + extension + platform + host | Host/platform latency, cache, status, throughput | Latency, Audience Ops |
| `geo_daily.parquet` | 31,461 | date + source + extension + geo | Geography latency and status | Latency |
| `status_daily.parquet` | 1,806 | date + source + extension + status_code | Status-code latency/status mix | Latency, Audience Ops |
| `cache_daily.parquet` | 914 | date + source + extension + cacheStatus + cacheable | Cache hit/miss profile | Latency |
| `summary.parquet` | 1 | profile-wide | First/last dates and high-level latency totals | Latency |
| `latency_manifest.json` | n/a | run metadata | Incremental state and generated table details | Pipeline/debug |

Metric columns include `ttfb_p50_ms`, `ttfb_p95_ms`, `turnaround_p95_ms`, `transfer_p95_ms`, `throughput_p05`, `cache_hit_rows`, `status_5xx_rows`, and `non_200_rows`.

## STREAM Identity Tables

Folder: `output/identity`

These are STREAM-only at scan time. FAST query strings currently do not expose reliable `device_id` or `session_id`.

| File | Rows At Scan | Grain | What It Means | Users |
|---|---:|---|---|---|
| `identity_daily.parquet` | 86 | date + source | STREAM total devices/sessions/IP+UA, new/returning | Audience Ops |
| `identity_channel_daily.parquet` | 2,126 | date + source + channel | STREAM identity metrics by channel | Audience Ops |
| `identity_platform_daily.parquet` | 695 | date + source + platform | STREAM identity metrics by platform | Audience Ops |
| `identity_platform_channel_daily.parquet` | 12,949 | date + source + platform + channel | STREAM identity metrics by platform/channel | Audience Ops |
| `identity_device_daily.parquet` | 1,165,736 | date + channel + platform + device_id | Device-level STREAM identity rows | Future drilldowns |
| `identity_session_daily.parquet` | 1,534,703 | date + channel + platform + session_id | Session-level STREAM identity rows | Future drilldowns |
| `identity_ipua_daily.parquet` | 1,368,907 | date + channel + platform + cliIP+UA key | IP+UA identity rows | Future drilldowns |
| `identity_mart_state.json` | n/a | state | Incremental state | Pipeline |
| `identity_mart_manifest.json` | n/a | manifest | Last build metadata | Pipeline |

## Content Title Tables

Folder: `output/content`

| File | Rows At Scan | Grain | What It Means | Users |
|---|---:|---|---|---|
| `content_daily.parquet` | 32,167 | date + source + channel + platform + content_title + category_name | STREAM content title/category manifest and request-hour metrics | Audience Ops |
| `content_mart_state.json` | n/a | state | Incremental state | Pipeline |
| `content_mart_manifest.json` | n/a | manifest | Last build metadata | Pipeline |
| `parts/content_daily/source=.../year=.../month=.../day=.../*.parquet` | n/a | daily part | Incremental content parts | Do not use directly |

FAST content title is not available from current query string evidence.

## Device And UA Decode

Folder: `output/device_decode`

| File Pattern | What It Means | Users |
|---|---|---|
| `ua_decode_lookup_both_all.parquet` | Current distinct-UA decode lookup, local/API enriched where possible | Audience Ops, Watch Hours |
| `ua_decode_lookup_both_all.csv` | CSV copy of current UA lookup | Manual review |
| `ua_decode_lookup_both_all_summary.csv` | Decode status/quality summary | Manual QA |
| `ua_decode_lookup_both_all_unknown_review.csv` | Remaining unknown UA values for review | Manual QA |
| `ua_decode_dashboard_preview.html` | Standalone UA decode preview dashboard | Manual QA |
| `ua_distinct_profile_all_sources_*.parquet` | Distinct UA profile from lake/profile | UA decode pipeline |
| `ua_decode_enriched_all_sources_*.parquet` | Older enriched UA decode output | Watch Hours can read latest |
| `device_decode_summary_all_sources_*.parquet` | Legacy device summary by decoded device | Watch Hours/Audience Ops optional |
| `top_user_agents_all_sources_*.parquet` | Top UA rows | Watch Hours optional |
| `unknown_device_codes_all_sources_*.csv` | Unknown device code samples | Manual QA |
| `device_decode_manifest_*.json`, `ua_decode_manifest_*.json` | Run metadata | Pipeline/debug |

## Lookup And Config Files

These files are not dashboard outputs, but dashboards and decode/profile steps use them.

| File | What It Means | Users |
|---|---|---|
| `data/asn/asnDecoded.csv` | ASN number to network/provider display mapping | Watch Hours ASN/network display |
| `data/asn/unique_asn.csv` | Unique ASN list extracted from data | ASN lookup workflow |
| `data/asn/ip2location_asn_cache.json` | ASN lookup cache | ASN lookup workflow |
| `config/device_decode/amazon_fire_tv_models.csv` | Fire TV model-code to device-name mapping | UA/device decode |
| `data/cache/device_decode/ua_decode_cache.parquet` | Older UA decode cache | UA decode workflow |
| `data/cache/device_decode/whatmyuseragent_distinct_ua_cache.parquet` | WhatMyUserAgent API response cache | UA decode workflow |

## Overview CSV/XLSX Outputs

Folder: `output/overview`

| File | What It Means | Users |
|---|---|---|
| `overview_dashboard.html` | Stakeholder Overview dashboard | Output |
| `overview_report.xlsx` | Excel overview report | Output/manual share |
| `overview_source_daily.csv` | Daily source-level overview values including DAU/WAU/MAU style fields | Overview, Audience Ops |
| `device_daily.csv` | Daily device activity detail, large CSV | Overview |
| `device_snapshot.csv` | Latest device snapshot summary | Overview |
| `device_daily.manifest.json` | Device CSV generation metadata | Pipeline/debug |

## Exports And Ad Hoc Files

Folder: `output/exports`

| Location | What It Means | Notes |
|---|---|---|
| `raw_parquet/*.parquet` | User-requested raw exports by cliIP, channel, state, date range, etc. | Not a reusable mart unless promoted |
| `summary_parquet/*.parquet` | User-requested summary exports, for example Maharashtra/channel/status watch hours | Good reference, not a production mart |
| `cliip_journey/<export_name>/*.parquet` | Per-cliIP journey analysis: daily/channel/hourly/time-bucket/segments/summary | Input to single cliIP journey dashboard |
| `viewer_journey/viewer_journey_index.parquet` | Reusable viewer journey index | Viewer Journey Menu |
| `viewer_journey/parts/*.parquet` | Incremental daily viewer journey parts | Do not use directly |
| `watch_detail_*.xlsx` and `watch_detail_*.raw_rows.*` | Manual export generated for specific region/channel requests | Not reusable |

## Stream Showcase Outputs

Folder: `output/watch_hours/stream_showcase`

| File | What It Means | Inputs |
|---|---|---|
| `stream_watch_hours_showcase.html` | PR/showcase HTML for STREAM watch hours | `daily_volume.parquet`, `geo_daily.parquet` |
| `stream_watch_hours_showcase.xlsx` | Excel version of showcase with date controls | Same as HTML |

## State And Logs

| Location | What It Means |
|---|---|
| `output/state/pipeline_last_run.json` | Latest pipeline run summary |
| `output/state/pipeline_last_run_steps.csv` | Latest pipeline step status table |
| `output/state/pipeline_run_*.json` | Historical pipeline run summaries |
| `output/state/pipeline_health_*.json` | Health-check summaries |
| `output/logs/*` | Step logs and backfill logs |
| `output/cache/chartjs/chart.umd.min.js` | Cached Chart.js for static HTML dashboards |

## Known Gaps Before Building New Dashboards

| Needed Metric | Current Status | What To Build If Required |
|---|---|---|
| FAST true device_id/session_id users | Not available in current FAST query strings | Needs app telemetry or a new source with device/session identifiers |
| FAST CMCD playback sessions | Partially available | Use `output/watch_hours/concurrency/fast_platform_channel_cmcd_daily.parquet`; current coverage depends on client/platform |
| FAST platform + channel + geo exact split | Available | Use `output/watch_hours/concurrency/fast_platform_channel_geo_daily.parquet` |
| FAST platform + channel + decoded UA/device/OS split | Available | Use `output/watch_hours/concurrency/fast_platform_channel_ua_device_daily.parquet` |
| FAST platform + channel exact manifest views | Available | Use `output/watch_hours/concurrency/fast_platform_channel_manifest_daily.parquet` |
| FAST platform + channel exact bandwidth | Available | Use `output/watch_hours/concurrency/fast_platform_channel_bandwidth_daily.parquet` |
| True range-distinct users across many days | Static dashboards mostly use daily distinct sums | Needs backend query or a compact identity sketch/mart |
| Deduped playback/watch sessions | Marked WIP in dashboards | Needs segment/session dedupe logic beyond request-hour `.ts * 6 sec` |

## Dashboard Input Map

| Dashboard | Reads |
|---|---|
| Overview | `output/overview/device_snapshot.csv`, `device_daily.csv`, `overview_source_daily.csv`, writes `overview_dashboard.html` and `overview_report.xlsx` |
| Watch Hours | `output/watch_hours/profile/*.parquet`, `output/watch_hours/daily_tables/*.parquet`, `output/watch_hours/concurrency/concurrency_minute.parquet`, `concurrency_summary.parquet`, `output/device_decode/*latest*`, `data/asn/asnDecoded.csv` |
| Concurrency | `output/watch_hours/concurrency/concurrency_minute.parquet`, `concurrency_status_minute.parquet`, `concurrency_summary.parquet`, writes CSV/parquet viewer exports and `veto_concurrency.html` |
| Latency | `output/latency/profile/daily.parquet`, `hourly.parquet`, `channel_daily.parquet`, `host_daily.parquet`, `geo_daily.parquet`, `status_daily.parquet`, `cache_daily.parquet`, `summary.parquet` |
| Audience Ops | `daily_volume`, `overview_source_daily.csv`, `channel_audience_daily`, `geo_daily`, `channel_geo_daily`, `device_type_by_channel_daily`, `region_channel_device_daily`, `fast_platform_channel_geo_daily.parquet`, `fast_platform_channel_ua_device_daily.parquet`, `fast_platform_channel_manifest_daily.parquet`, `fast_platform_channel_bandwidth_daily.parquet`, `fast_platform_channel_cmcd_daily.parquet`, `user_agents_daily`, `ua_decode_lookup_both_all.parquet`, latency profile, concurrency, identity, content |
| Stream Showcase | `output/watch_hours/daily_tables/daily_volume.parquet`, `geo_daily.parquet`, writes HTML and XLSX |
| Viewer Journey Menu | `output/exports/viewer_journey/viewer_journey_index.parquet`, writes `viewer_journey_menu.html` |
| cliIP Journey | Per-export raw cliIP parquet plus derived journey parquet files under `output/exports/cliip_journey/<name>` |

## How To Add A New Dashboard Without Repeating Work

1. Decide the desired grain first: source, date, channel, platform, geo, device, session, or user.
2. Search this catalog for an existing mart at that grain.
3. If the exact grain exists, use it.
4. If only a wider or different grain exists, do not silently reuse it. Add a clear UI note or build a new mart.
5. If a new mart is needed, put it under a stable folder like `output/<domain>/...`, add an incremental state/manifest file, and update this catalog.
6. Keep generated HTML/large exports out of git unless explicitly needed.
