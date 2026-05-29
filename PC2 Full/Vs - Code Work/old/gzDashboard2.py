import polars as pl

lf = pl.scan_parquet(r"D:\VETO Logs\parquet_output\*.parquet")

out = lf.select([
    pl.len().alias("rows"),

    pl.col("cookie").drop_nulls().n_unique().alias("cookie_unique"),
    pl.col("streamId").drop_nulls().n_unique().alias("streamId_unique"),
    pl.col("arlid").drop_nulls().n_unique().alias("arlid_unique"),
    pl.col("cliIP").drop_nulls().n_unique().alias("cliIP_unique"),
    pl.col("xForwardedFor").drop_nulls().n_unique().alias("xff_unique"),
    pl.col("UA").drop_nulls().n_unique().alias("ua_unique"),
]).collect()

print(out)