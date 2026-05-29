# VgLive Lake Deep Profile Findings

Profile generated from `D:\Veto Logs Backup\lake` using `vglive_deep_profile.py`.

## Scope

- Rows scanned: 1,360,187,031
- Status 200 rows: 1,339,089,636
- Non-200 rows: 21,097,395
- `.ts` media segment rows: 513,650,439
- `.m3u8` playlist rows: 825,438,559
- Raw request-based watch hours: 856,084.1

## Channel Identity

- Full `.ts` unmapped candidate output is empty, so current channel mapping covers the observed media segment paths.
- Query string validation found 481,057,439 requests aligned with current mapping.
- Query string mismatches are small: 60,269 requests across 2,742 sessions.
- Main review candidates:
  - `indiatv` query evidence on `vetocricket.akamaized.net / vetocricketlive`
  - `newsnation_pbhr` query evidence on `nn-veto.akamaized.net / nnpunj`

## Top Raw Watch-Hour Channels

- India TV: 606,442.5 hours
- India TV SpeedNews: 104,035.4 hours
- Ndtv India: 98,888.7 hours
- NewsNation: 8,865.4 hours
- India TV Yoga: 5,738.8 hours
- 9XM: 4,986.0 hours
- Sanskaar TV: 4,835.8 hours
- B4U Movies: 4,346.7 hours

These figures are raw request-based hours: `raw .ts chunks * 6 / 3600`. The dashboard should also expose deduped estimated hours where needed.

## What The Dashboard Can Add

- Executive view: total watch hours, unique IPs/viewers, active channels, non-200 rate, top channels, trend by day.
- Channel view: watch hours, viewer count, share of total, daily trend, host/candidate mapping evidence, quality mix.
- Mapping audit: unmapped candidates, queryStr mismatch review, approved ID map, host/path rule source.
- Audience view: state/city, ASN/network, device type, platform/queryStr device, top user agents.
- Reliability view: status code mix, errorCode/startupError, 403/404/503/504 hotspots, host health.
- CDN performance view: cache hit/miss, TTFB p50/p95, transfer p50/p95, throughput p50/p05 by host and extension.
- Data quality view: column fill rates, queryStr availability, CMCD availability, `.ts` vs `.m3u8` split.

## Dashboard Rules

- Count watch hours from `.ts` only.
- Use `.m3u8` for metadata/evidence, not watch time.
- Keep mapping conservative: approved `vglive-sk`, host rules, path rules, queryStr audit as validation.
- Show `Other` only when mapping is genuinely unknown; current full profile has no `.ts` unmapped candidates.
- Label raw vs deduped watch hours clearly.
