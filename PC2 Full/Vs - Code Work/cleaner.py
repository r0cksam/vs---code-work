import urllib.parse
import re

# CONFIGURATION
input_file = "unique_queryStr (3).csv"
output_file = "digital_commentary.txt"
search_keyword = "digital_commentary"

def fix_and_search():
    count = 0
    print(f"Searching for '{search_keyword}'...")
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f_in:
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                if search_keyword in line:
                    # 1. Find the URL part starting with 'session_id='
                    match = re.search(r'session_id=[^,]+', line)
                    if match:
                        raw_query = match.group(0)
                        
                        # 2. FIX: Decode %20 to spaces and %E... to Hindi text
                        clean_line = urllib.parse.unquote(raw_query)
                        
                        # 3. Write to the new file
                        f_out.write(clean_line + "\n")
                        count += 1
                        
                        # Progress update every 1000 matches
                        if count % 1000 == 0:
                            print(f"Found {count} matches so far...")

    print(f"Done! Found {count} total rows. Results saved to '{output_file}'.")

if __name__ == "__main__":
    fix_and_search()
