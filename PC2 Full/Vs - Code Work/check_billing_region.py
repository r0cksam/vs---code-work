import duckdb

files = r"D:\Vs - Code Work\cleaned_output\*.parquet"

con = duckdb.connect()

df = con.execute("""
WITH x AS (
    SELECT
        billingRegion,
        country,
        serverCountry,
        COUNT(*) AS requests
    FROM read_parquet(?, union_by_name=true)
    GROUP BY 1,2,3
),
r AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY billingRegion
            ORDER BY requests DESC
        ) AS rn
    FROM x
)
SELECT
    billingRegion,
    country,
    serverCountry,
    requests
FROM r
WHERE rn <= 50
ORDER BY TRY_CAST(billingRegion AS INT), rn
""", [files]).df()

print(df.to_string(index=False))