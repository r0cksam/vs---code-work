#!/usr/bin/env python3
"""
Improved network/local drive type checker.
- accurately measures physical disk speed by bypassing OS RAM cache.
- Dependency-free (Uses native OS tools like PowerShell/lsblk instead of pip packages).
"""

import os
import sys
import time
import platform
import subprocess
import json

def measure_write_speed(path, test_file_name="speed_test.tmp", size_mb=100):
    """
    Measures sequential write speed (MB/s). 
    Uses os.fsync to ensure data is actually written to the disk, not just RAM.
    """
    test_file_path = os.path.join(path, test_file_name)
    data = os.urandom(1024 * 1024)
    total_written = 0
    start_time = time.time()

    try:
        with open(test_file_path, 'wb') as f:
            for _ in range(size_mb):
                f.write(data)
                total_written += 1
            
            # CRITICAL: Force the OS to write the RAM buffer to the physical disk
            f.flush()
            os.fsync(f.fileno())
            
        end_time = time.time()
    except (OSError, IOError) as e:
        print(f"  [Speed test] Error writing to {path}: {e}")
        return None
    finally:
        if os.path.exists(test_file_path):
            try:
                os.remove(test_file_path)
            except OSError:
                pass

    duration = end_time - start_time
    if duration > 0:
        return total_written / duration
    return None

def classify_by_speed(speed_mb_s):
    if speed_mb_s is None:
        return "Unknown (speed test failed)"
    if speed_mb_s > 800:
        return "NVMe SSD"
    if 200 < speed_mb_s <= 800:
        return "SATA SSD"
    if 50 <= speed_mb_s <= 200:
        return "HDD (7200 RPM or slower)"
    if 0 < speed_mb_s < 50:
        return "Very slow (HDD, USB 2.0, or congested network)"
    return "Other"

def detect_windows(drive_root):
    """Uses PowerShell to get the physical disk type without needing 'wmi' pip package."""
    drive_letter = drive_root.replace(":\\", "").replace(":/", "").strip()
    ps_cmd = (
        f"Get-Partition -DriveLetter {drive_letter} | "
        "Get-Disk | Select-Object BusType, MediaType | ConvertTo-Json"
    )
    
    try:
        result = subprocess.run(["powershell", "-Command", ps_cmd], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            disk_info = json.loads(result.stdout)
            bus = disk_info.get("BusType", "")
            media = disk_info.get("MediaType", "")
            
            # Format output beautifully
            if str(media) == "4" or str(media).upper() == "SSD":
                return f"{bus} SSD (detected)"
            elif str(media) == "3" or str(media).upper() == "HDD":
                return f"{bus} HDD (detected)"
            elif str(media) == "Unspecified" and bus == "NVMe":
                return "NVMe SSD (detected)"
            return f"{bus} Drive (detected)"
    except Exception as e:
        pass
    return None

def detect_linux(drive_path):
    """Uses lsblk to accurately fetch disk rotation and transport type."""
    try:
        # Find mount point block device
        df_res = subprocess.run(['df', '--output=source', drive_path], capture_output=True, text=True)
        device = df_res.stdout.strip().split('\n')[1].strip()
        
        # Get JSON data about this specific device
        lsblk_cmd = ['lsblk', device, '-o', 'NAME,ROTA,TRAN', '-J']
        lsblk_res = subprocess.run(lsblk_cmd, capture_output=True, text=True)
        data = json.loads(lsblk_res.stdout)
        
        block_info = data.get('blockdevices', [{}])[0]
        is_hdd = block_info.get('rota') == 1
        bus = block_info.get('tran', '').upper()
        
        if bus == "NVME":
            return "NVMe SSD (detected)"
        elif not is_hdd:
            return f"{bus if bus else 'SATA/SAS'} SSD (detected)"
        else:
            return f"{bus if bus else 'SATA/SAS'} HDD (detected)"
    except Exception:
        return None

def detect_mac(drive_path):
    """Basic detection for macOS using diskutil."""
    try:
        df_res = subprocess.run(['df', drive_path], capture_output=True, text=True)
        device = df_res.stdout.strip().split('\n')[1].split()[0]
        
        info_res = subprocess.run(['diskutil', 'info', device], capture_output=True, text=True)
        if "Solid State: Yes" in info_res.stdout:
            if "NVMe" in info_res.stdout or "PCIe" in info_res.stdout:
                return "NVMe SSD (detected)"
            return "SSD (detected)"
    except Exception:
        return None

def detect_local_drive_type(path):
    sys_os = platform.system()
    if sys_os == 'Windows':
        drive_root = os.path.splitdrive(os.path.abspath(path))[0] + '\\'
        return detect_windows(drive_root)
    elif sys_os == 'Linux':
        return detect_linux(path)
    elif sys_os == 'Darwin':
        return detect_mac(path)
    return None

def main():
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = input("Enter path to test (e.g., Z:/, /mnt/share, C:\\): ").strip()
        if not target:
            sys.exit(1)

    target = os.path.abspath(target)

    if not os.path.exists(target):
        print(f"Error: Path '{target}' does not exist.")
        sys.exit(1)
    if not os.access(target, os.W_OK):
        print(f"Error: Path '{target}' requires write permissions for the speed test.")
        sys.exit(1)

    print(f"\n--- Checking: {target} ---")
    print(f"Platform: {platform.system()}")

    detection_result = detect_local_drive_type(target)
    print(f"Hardware detection : {detection_result or 'Not available (Network drive, VM, or unknown)'}")

    print("Performing write speed test (100 MB file)...")
    speed = measure_write_speed(target, size_mb=100)
    
    if speed is not None:
        print(f"Measured write speed: {speed:.2f} MB/s")
        speed_based = classify_by_speed(speed)
        print(f"Performance based   : {speed_based}")
    else:
        print("Speed test failed.")
        speed_based = "Unknown"

    print("\n--- Final classification ---")
    if detection_result and "detected" in detection_result:
        print(f"Drive type: {detection_result}")
    else:
        print(f"Drive type: {speed_based}")

if __name__ == "__main__":
    main()