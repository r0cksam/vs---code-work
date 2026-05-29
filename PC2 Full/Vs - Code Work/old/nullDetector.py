import gzip
import json

filename = 'ak-199952-1775615225-577717-ds.gz'

# Track which columns have "Real Data"
real_data_found = {}
total_cols_seen = set()

with gzip.open(filename, 'rt') as f:
    for line in f:
        try:
            data = json.loads(line)
            for key, value in data.items():
                total_cols_seen.add(key)
                if key not in real_data_found:
                    real_data_found[key] = False
                
                # If we see anything OTHER than - or ^, it's real data
                if str(value) not in ["-", "^"]:
                    real_data_found[key] = True
        except json.JSONDecodeError:
            continue

# Filter for the completely blank ones
completely_blank = [k for k, found in real_data_found.items() if not found]

print(f"Total columns found in file: {len(total_cols_seen)}")
print(f"Number of COMPLETELY blank columns: {len(completely_blank)}")
print("-" * 40)
print("List of blank columns:")
for col in sorted(completely_blank):
    print(f" - {col}")