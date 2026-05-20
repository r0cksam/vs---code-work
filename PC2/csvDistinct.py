from tqdm import tqdm
import re

input_file = "unique_reqPath.csv"
output_file = "mapped_distinct.csv"

# Known mappings
mapping = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
}

# Regex
pattern = re.compile(r"vglive-sk-\d+")

unique_values = set()

# Count lines for progress bar
with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
    total_lines = sum(1 for _ in f)

# Extract distinct values
with open(input_file, "r", encoding="utf-8", errors="ignore") as infile:

    for line in tqdm(infile, total=total_lines, desc="Extracting"):

        matches = pattern.findall(line)

        if matches:
            unique_values.update(matches)

# Save mapped output
with open(output_file, "w", encoding="utf-8") as outfile:

    for value in sorted(unique_values):

        channel_name = mapping.get(value, "Unknown")

        outfile.write(f"{value},{channel_name}\n")

print(f"\nDone. Found {len(unique_values)} unique values.")
print(f"Saved to: {output_file}")