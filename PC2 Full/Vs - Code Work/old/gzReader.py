import gzip
import json
import os

file_path = r'Z:\veto fast logs\14\110280\ak-000000-1778756062-867255-ds.gz'
if os.path.exists(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            clean_line = line.strip()
            if clean_line:  
                print(clean_line)
    print(f"Error: The file at {file_path} was not found.")
    
    
    
    

# import gzip

# # This opens the file and decompresses it into text automatically
# with gzip.open('ak-199952-1775615225-577717-ds.gz', 'rt') as f:
#     content = f.read()
#     print(content)






# import gzip
# import json

# # Use a loop to handle every single row without missing any data
# with gzip.open('ak-199952-1775615225-577717-ds.gz', 'rt') as f:
#     for line in f:
#         # data = json.loads(line)  # Use this to turn it into a Python dictionary
#         print(line.strip())        # This shows you the raw text of every record