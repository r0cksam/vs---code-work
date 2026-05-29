from tqdm import tqdm

input_file = "unique_reqPath.csv"
output_file = "big_unique_reqPath.csv"

replacements = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
}

# Count total lines for progress bar
with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
    total_lines = sum(1 for _ in f)

with open(input_file, "r", encoding="utf-8", errors="ignore") as infile, \
     open(output_file, "w", encoding="utf-8") as outfile:

    for line in tqdm(infile, total=total_lines, desc="Processing"):

        for old, new in replacements.items():
            line = line.replace(old, new)

        outfile.write(line)

print("Replacement complete.")