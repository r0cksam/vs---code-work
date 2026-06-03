import gzip
import json
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

FOLDER_PATH = r'Z:\Veto Logs\23'

def extract_sparse_data(file_path):
    """Worker function to find unique non-placeholder values in specific columns."""
    found_cmcd = set()
    found_custom = set()
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                
                # Check cmcd
                val_cmcd = str(data.get('cmcd', '-'))
                if val_cmcd not in ["-", "^"]:
                    found_cmcd.add(val_cmcd)
                
                # Check customField
                val_custom = str(data.get('customField', '-'))
                if val_custom not in ["-", "^"]:
                    found_custom.add(val_custom)
                    
    except Exception:
        pass 
    return list(found_cmcd), list(found_custom)

if __name__ == '__main__':
    files = list(Path(FOLDER_PATH).glob('*.gz'))
    
    all_unique_cmcd = set()
    all_unique_custom = set()

    with Pool(cpu_count()) as pool:
        # Using chunksize for speed with 5L files
        results = pool.imap_unordered(extract_sparse_data, files, chunksize=100)
        
        for cmcd_list, custom_list in tqdm(results, total=len(files), desc="Extracting Data"):
            all_unique_cmcd.update(cmcd_list)
            all_unique_custom.update(custom_list)

    print("\n" + "="*60)
    print(f"UNIQUE DATA FOUND IN 'cmcd' ({len(all_unique_cmcd)} unique values):")
    for val in sorted(list(all_unique_cmcd))[:20]: # Show first 20
        print(f" -> {val}")

    print("\n" + "="*60)
    print(f"UNIQUE DATA FOUND IN 'customField' ({len(all_unique_custom)} unique values):")
    for val in sorted(list(all_unique_custom))[:20]: # Show first 20
        print(f" -> {val}")