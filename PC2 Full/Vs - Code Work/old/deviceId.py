import pandas as pd
import re

file_path = r"C:\Users\Intern\Downloads\unique_queryStr (1).csv"

df = pd.read_csv(file_path, low_memory=False)

print("Columns:", df.columns.tolist())

col_name = "value"

def extract_device_id(qs):
    if pd.isna(qs):
        return None

    qs = str(qs)

    patterns = [
        r"device_id=([^&]+)",
        r"did=([^&]+)",
        r"deviceId=([^&]+)",
        r"uid=([^&]+)",
        r"subscriber_id=([^&]+)",
        r"deviceid=([^&]+)",
    ]

    for p in patterns:
        m = re.search(p, qs, flags=re.IGNORECASE)
        if m:
            return m.group(1)

    return None

df["device_id"] = df[col_name].apply(extract_device_id)

df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

grouped = (
    df.dropna(subset=["device_id"])
      .groupby("device_id")["count"]
      .sum()
      .sort_values(ascending=False)
)

print("\nUnique device IDs:", grouped.index.nunique())

same_devices = grouped[grouped > 1]

print("\nRepeated device IDs:", len(same_devices))
print("\nTop repeated device IDs:")
print(same_devices.head(20))

grouped.to_csv(r"C:\Users\Intern\Downloads\all_device_ids_with_counts.csv")
same_devices.to_csv(r"C:\Users\Intern\Downloads\repeated_device_ids.csv")

print("\nSaved:")
print(r"C:\Users\Intern\Downloads\all_device_ids_with_counts.csv")
print(r"C:\Users\Intern\Downloads\repeated_device_ids.csv")