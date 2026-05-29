from __future__ import annotations

import polars as pl
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class FilterConfig:
    start_dt: object | None = None
    end_dt: object | None = None
    states: tuple[str, ...] = ()
    hosts: tuple[str, ...] = ()
    status_codes: tuple[int, ...] = ()
    path_search: str = ""
    country: tuple[str, ...] = ()
    cache_status: tuple[str, ...] = ()


class CDNAnalytics:
    def __init__(self, parquet_glob: str) -> None:
        self.parquet_glob = parquet_glob
        self.lf = pl.scan_parquet(parquet_glob)
        self.schema = self.lf.collect_schema().names()
        self.base = self._prepare_base(self.lf)

    def _has(self, col: str) -> bool:
        return col in self.schema

    def _prepare_base(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        exprs: list[pl.Expr] = []

        cast_map = {
            "datetime": pl.Datetime,
            "statusCode": pl.Int64,
            "bytes": pl.Float64,
            "totalBytes": pl.Float64,
            "throughput": pl.Float64,
            "timeToFirstByte": pl.Float64,
            "downloadTime": pl.Float64,
            "transferTimeMSec": pl.Float64,
            "turnAroundTimeMSec": pl.Float64,
            "asn": pl.Int64,
            "cliIP": pl.Utf8,
            "UA": pl.Utf8,
            "reqPath": pl.Utf8,
            "reqHost": pl.Utf8,
            "state": pl.Utf8,
            "country": pl.Utf8,
            "city": pl.Utf8,
            "cacheStatus": pl.Utf8,
            "errorCode": pl.Utf8,
            "cookie": pl.Utf8,
            "streamId": pl.Utf8,
            "xForwardedFor": pl.Utf8,
            "queryStr": pl.Utf8,
        }

        for col, dtype in cast_map.items():
            if self._has(col):
                exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))

        lf = lf.with_columns(exprs) if exprs else lf

        if self._has("cliIP") and self._has("UA"):
            lf = lf.with_columns([
                pl.concat_str(
                    [
                        pl.col("cliIP").fill_null("NA"),
                        pl.lit("|"),
                        pl.col("UA").fill_null("NA"),
                    ]
                ).alias("pseudo_user")
            ])
        elif self._has("cliIP"):
            lf = lf.with_columns(pl.col("cliIP").fill_null("NA").alias("pseudo_user"))
        else:
            lf = lf.with_columns(pl.lit("UNKNOWN").alias("pseudo_user"))

        if self._has("statusCode"):
            lf = lf.with_columns([
                pl.when(pl.col("statusCode") < 400)
                .then(pl.lit("success"))
                .when(pl.col("statusCode") < 500)
                .then(pl.lit("client_error"))
                .otherwise(pl.lit("server_error"))
                .alias("status_class")
            ])

        if self._has("cacheStatus"):
            lf = lf.with_columns([
                pl.when(pl.col("cacheStatus").str.to_uppercase().str.contains("HIT", literal=True))
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .alias("is_cache_hit")
            ])

        return lf

    def apply_filters(self, filters: FilterConfig) -> pl.LazyFrame:
        q = self.base

        if filters.start_dt is not None and self._has("datetime"):
            q = q.filter(pl.col("datetime") >= pl.lit(filters.start_dt))
        if filters.end_dt is not None and self._has("datetime"):
            q = q.filter(pl.col("datetime") <= pl.lit(filters.end_dt))
        if filters.states and self._has("state"):
            q = q.filter(pl.col("state").is_in(filters.states))
        if filters.hosts and self._has("reqHost"):
            q = q.filter(pl.col("reqHost").is_in(filters.hosts))
        if filters.status_codes and self._has("statusCode"):
            q = q.filter(pl.col("statusCode").is_in(filters.status_codes))
        if filters.country and self._has("country"):
            q = q.filter(pl.col("country").is_in(filters.country))
        if filters.cache_status and self._has("cacheStatus"):
            q = q.filter(pl.col("cacheStatus").is_in(filters.cache_status))
        if filters.path_search and self._has("reqPath"):
            q = q.filter(pl.col("reqPath").str.contains(filters.path_search, literal=False))

        return q

    def global_summary(self, filters: FilterConfig) -> pl.DataFrame:
        q = self.apply_filters(filters)

        exprs: list[pl.Expr] = [
            pl.len().alias("total_requests"),
            pl.col("pseudo_user").n_unique().alias("pseudo_users"),
        ]

        if self._has("reqPath"):
            exprs.append(pl.col("reqPath").n_unique().alias("unique_paths"))
        if self._has("totalBytes"):
            exprs += [
                pl.col("totalBytes").sum().alias("total_bytes"),
                pl.col("totalBytes").mean().alias("avg_bytes"),
            ]
        if self._has("throughput"):
            exprs += [
                pl.col("throughput").mean().alias("avg_throughput"),
                pl.col("throughput").median().alias("median_throughput"),
            ]
        if self._has("statusCode"):
            exprs += [
                (pl.col("statusCode") >= 400).sum().alias("error_requests"),
                ((pl.col("statusCode") >= 400).sum() / pl.len() * 100).alias("error_rate_pct"),
            ]
        if "is_cache_hit" in self.base.collect_schema().names():
            exprs.append(pl.col("is_cache_hit").mean().mul(100).alias("cache_hit_pct"))
        if self._has("timeToFirstByte"):
            exprs.append(pl.col("timeToFirstByte").mean().alias("avg_ttfb"))
        if self._has("downloadTime"):
            exprs.append(pl.col("downloadTime").mean().alias("avg_download_time"))

        return q.select(exprs).collect()

    def hourly_trend(self, filters: FilterConfig) -> pl.DataFrame:
        q = self.apply_filters(filters)
        if not self._has("datetime"):
            return pl.DataFrame()

        aggs: list[pl.Expr] = [pl.len().alias("requests")]

        if self._has("totalBytes"):
            aggs.append(pl.col("totalBytes").sum().alias("bytes"))
        if self._has("throughput"):
            aggs.append(pl.col("throughput").mean().alias("avg_throughput"))
        if self._has("statusCode"):
            aggs.append((pl.col("statusCode") >= 400).sum().alias("errors"))

        return (
            q.with_columns(pl.col("datetime").dt.truncate("1h").alias("hour"))
            .group_by("hour")
            .agg(aggs)
            .sort("hour")
            .collect()
        )

    def top_states(self, filters: FilterConfig, limit: int = 20) -> pl.DataFrame:
        if not self._has("state"):
            return pl.DataFrame()
        return (
            self.apply_filters(filters)
            .group_by("state")
            .agg(pl.len().alias("requests"))
            .sort("requests", descending=True)
            .limit(limit)
            .collect()
        )

    def top_paths(self, filters: FilterConfig, limit: int = 50) -> pl.DataFrame:
        if not self._has("reqPath"):
            return pl.DataFrame()

        aggs = [pl.len().alias("hits")]
        if self._has("totalBytes"):
            aggs.append(pl.col("totalBytes").sum().alias("total_bytes"))
        if self._has("throughput"):
            aggs.append(pl.col("throughput").mean().alias("avg_throughput"))
        if self._has("statusCode"):
            aggs.append((pl.col("statusCode") >= 400).sum().alias("errors"))

        return (
            self.apply_filters(filters)
            .group_by("reqPath")
            .agg(aggs)
            .sort("hits", descending=True)
            .limit(limit)
            .collect()
        )

    def status_breakdown(self, filters: FilterConfig) -> pl.DataFrame:
        if not self._has("statusCode"):
            return pl.DataFrame()
        return (
            self.apply_filters(filters)
            .group_by(["statusCode", "status_class"])
            .agg(pl.len().alias("count"))
            .sort(["count", "statusCode"], descending=[True, False])
            .collect()
        )

    def cache_breakdown(self, filters: FilterConfig) -> pl.DataFrame:
        if not self._has("cacheStatus"):
            return pl.DataFrame()
        return (
            self.apply_filters(filters)
            .group_by("cacheStatus")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect()
        )

    def top_error_paths(self, filters: FilterConfig, limit: int = 50) -> pl.DataFrame:
        if not (self._has("statusCode") and self._has("reqPath")):
            return pl.DataFrame()
        return (
            self.apply_filters(filters)
            .filter(pl.col("statusCode") >= 400)
            .group_by(["statusCode", "reqPath"])
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .limit(limit)
            .collect()
        )

    def top_pseudo_users(self, filters: FilterConfig, limit: int = 50) -> pl.DataFrame:
        q = self.apply_filters(filters)
        aggs = [
            pl.len().alias("requests"),
            pl.col("reqPath").n_unique().alias("unique_paths") if self._has("reqPath") else pl.lit(None).alias("unique_paths"),
        ]
        if self._has("totalBytes"):
            aggs.append(pl.col("totalBytes").sum().alias("total_bytes"))
        if self._has("statusCode"):
            aggs.append((pl.col("statusCode") >= 400).sum().alias("errors"))

        return (
            q.group_by("pseudo_user")
            .agg(aggs)
            .sort("requests", descending=True)
            .limit(limit)
            .collect()
        )

    def bot_like_users(self, filters: FilterConfig, min_requests: int = 500, min_ratio: float = 50.0) -> pl.DataFrame:
        if not self._has("reqPath"):
            return pl.DataFrame()

        return (
            self.apply_filters(filters)
            .group_by("pseudo_user")
            .agg([
                pl.len().alias("requests"),
                pl.col("reqPath").n_unique().alias("unique_paths"),
                pl.col("reqHost").n_unique().alias("unique_hosts") if self._has("reqHost") else pl.lit(None).alias("unique_hosts"),
            ])
            .with_columns([
                pl.when(pl.col("unique_paths") > 0)
                .then(pl.col("requests") / pl.col("unique_paths"))
                .otherwise(None)
                .alias("request_path_ratio")
            ])
            .filter(pl.col("requests") >= min_requests)
            .filter(pl.col("request_path_ratio") >= min_ratio)
            .sort(["request_path_ratio", "requests"], descending=True)
            .collect()
        )

    def duplicate_groups(
        self,
        filters: FilterConfig,
        keys: Sequence[str] | None = None,
        rounded_seconds: int | None = None,
        limit: int = 200,
    ) -> pl.DataFrame:
        q = self.apply_filters(filters)

        if keys is None:
            keys = [c for c in ["pseudo_user", "reqPath", "queryStr", "statusCode"] if self._has(c)]

        group_cols = list(keys)

        if rounded_seconds is not None and self._has("datetime"):
            q = q.with_columns(
                pl.col("datetime").dt.truncate(f"{rounded_seconds}s").alias("time_bucket")
            )
            group_cols.append("time_bucket")

        if not group_cols:
            return pl.DataFrame()

        return (
            q.group_by(group_cols)
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
            .sort("count", descending=True)
            .limit(limit)
            .collect()
        )

    def pseudo_user_timeline(self, pseudo_user: str, filters: FilterConfig, limit: int = 5000) -> pl.DataFrame:
        q = self.apply_filters(filters).filter(pl.col("pseudo_user") == pseudo_user)

        cols = [c for c in [
            "datetime", "reqHost", "reqPath", "queryStr", "statusCode",
            "cacheStatus", "totalBytes", "throughput", "timeToFirstByte",
            "downloadTime", "transferTimeMSec", "city", "state", "country",
            "asn", "errorCode"
        ] if self._has(c)]

        return q.select(cols).sort("datetime").limit(limit).collect()

    def pseudo_user_summary(self, pseudo_user: str, filters: FilterConfig) -> pl.DataFrame:
        q = self.apply_filters(filters).filter(pl.col("pseudo_user") == pseudo_user)

        exprs: list[pl.Expr] = [
            pl.len().alias("requests"),
            pl.col("datetime").min().alias("first_seen") if self._has("datetime") else pl.lit(None).alias("first_seen"),
            pl.col("datetime").max().alias("last_seen") if self._has("datetime") else pl.lit(None).alias("last_seen"),
        ]

        if self._has("reqPath"):
            exprs.append(pl.col("reqPath").n_unique().alias("unique_paths"))
        if self._has("statusCode"):
            exprs.append((pl.col("statusCode") >= 400).sum().alias("errors"))
        if self._has("totalBytes"):
            exprs.append(pl.col("totalBytes").sum().alias("total_bytes"))
        if self._has("throughput"):
            exprs.append(pl.col("throughput").mean().alias("avg_throughput"))
        if self._has("timeToFirstByte"):
            exprs.append(pl.col("timeToFirstByte").mean().alias("avg_ttfb"))
        if self._has("downloadTime"):
            exprs.append(pl.col("downloadTime").mean().alias("avg_download_time"))
        if self._has("cacheStatus"):
            exprs.append(pl.col("cacheStatus").drop_nulls().mode().first().alias("top_cache_status"))
        if self._has("state"):
            exprs.append(pl.col("state").drop_nulls().mode().first().alias("top_state"))
        if self._has("country"):
            exprs.append(pl.col("country").drop_nulls().mode().first().alias("top_country"))

        return q.select(exprs).collect()

    def pseudo_user_sessions(
        self,
        pseudo_user: str,
        filters: FilterConfig,
        inactivity_minutes: int = 30,
    ) -> pl.DataFrame:
        if not self._has("datetime"):
            return pl.DataFrame()

        user_lf = (
            self.apply_filters(filters)
            .filter(pl.col("pseudo_user") == pseudo_user)
            .sort("datetime")
            .with_columns([
                pl.col("datetime").diff().alias("gap"),
            ])
            .with_columns([
                pl.when(
                    pl.col("gap").is_null() | (pl.col("gap") > pl.duration(minutes=inactivity_minutes))
                )
                .then(1)
                .otherwise(0)
                .alias("new_session")
            ])
            .with_columns([
                pl.col("new_session").cum_sum().alias("session_id")
            ])
        )

        aggs: list[pl.Expr] = [
            pl.col("datetime").min().alias("session_start"),
            pl.col("datetime").max().alias("session_end"),
            pl.len().alias("requests"),
        ]

        if self._has("reqPath"):
            aggs.append(pl.col("reqPath").n_unique().alias("unique_paths"))
        if self._has("totalBytes"):
            aggs.append(pl.col("totalBytes").sum().alias("total_bytes"))
        if self._has("statusCode"):
            aggs.append((pl.col("statusCode") >= 400).sum().alias("errors"))

        return (
            user_lf.group_by("session_id")
            .agg(aggs)
            .with_columns([
                (pl.col("session_end") - pl.col("session_start")).alias("duration")
            ])
            .sort("session_start")
            .collect()
        )

    def pseudo_user_retries(
        self,
        pseudo_user: str,
        filters: FilterConfig,
        threshold_seconds: int = 2,
        limit: int = 1000,
    ) -> pl.DataFrame:
        if not (self._has("datetime") and self._has("reqPath")):
            return pl.DataFrame()

        cols = [c for c in ["datetime", "reqPath", "statusCode", "throughput", "cacheStatus", "errorCode"] if self._has(c)]

        return (
            self.apply_filters(filters)
            .filter(pl.col("pseudo_user") == pseudo_user)
            .sort("datetime")
            .with_columns([
                pl.col("datetime").diff().over("reqPath").alias("path_gap")
            ])
            .filter(pl.col("path_gap").is_not_null())
            .filter(pl.col("path_gap") <= pl.duration(seconds=threshold_seconds))
            .select(cols + ["path_gap"])
            .limit(limit)
            .collect()
        )

    def qoe_segments(self, filters: FilterConfig) -> pl.DataFrame:
        q = self.apply_filters(filters)
        if not self._has("throughput"):
            return pl.DataFrame()

        return (
            q.with_columns([
                pl.when(pl.col("throughput") < 1_000_000)
                .then(pl.lit("very_low"))
                .when(pl.col("throughput") < 3_000_000)
                .then(pl.lit("low"))
                .when(pl.col("throughput") < 8_000_000)
                .then(pl.lit("medium"))
                .otherwise(pl.lit("high"))
                .alias("throughput_band")
            ])
            .group_by("throughput_band")
            .agg([
                pl.len().alias("requests"),
                pl.col("timeToFirstByte").mean().alias("avg_ttfb") if self._has("timeToFirstByte") else pl.lit(None).alias("avg_ttfb"),
                pl.col("downloadTime").mean().alias("avg_download_time") if self._has("downloadTime") else pl.lit(None).alias("avg_download_time"),
            ])
            .sort("requests", descending=True)
            .collect()
        )

    def state_qoe(self, filters: FilterConfig, limit: int = 30) -> pl.DataFrame:
        if not self._has("state"):
            return pl.DataFrame()

        aggs: list[pl.Expr] = [pl.len().alias("requests")]
        if self._has("throughput"):
            aggs.append(pl.col("throughput").mean().alias("avg_throughput"))
        if self._has("timeToFirstByte"):
            aggs.append(pl.col("timeToFirstByte").mean().alias("avg_ttfb"))
        if self._has("downloadTime"):
            aggs.append(pl.col("downloadTime").mean().alias("avg_download_time"))
        if self._has("statusCode"):
            aggs.append((pl.col("statusCode") >= 400).sum().alias("errors"))

        return (
            self.apply_filters(filters)
            .group_by("state")
            .agg(aggs)
            .sort("requests", descending=True)
            .limit(limit)
            .collect()
        )

    def sample_rows(self, filters: FilterConfig, limit: int = 500) -> pl.DataFrame:
        return self.apply_filters(filters).limit(limit).collect()