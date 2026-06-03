from tqdm import tqdm

input_file = "big_unique_reqPath.csv"
output_file = "vglive_big_unique_reqPath.csv"

keyword = "vglive-sk"

# Count total lines for progress bar
with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
    total_lines = sum(1 for _ in f)

with open(input_file, "r", encoding="utf-8", errors="ignore") as infile, \
     open(output_file, "w", encoding="utf-8") as outfile:

    for line in tqdm(infile, total=total_lines, desc="Extracting"):

        if keyword in line:
            outfile.write(line)

print("Done. Matching rows saved to:", output_file)