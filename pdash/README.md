# pDash Modular

Run:

```bash
pip install -r requirements.txt
streamlit run app.py
```

Structure:

```text
app.py                         # Streamlit UI entrypoint
pdash/common.py                # shared imports, constants, defaults, logging/progress helpers
pdash/core/data.py             # parquet scanning, schema, filtering, exports, charts aggregation
pdash/core/query_string.py     # query-string parsing/loading helpers
pdash/analytics/behavior.py    # user/global behavior analytics, sessions, reports
```
