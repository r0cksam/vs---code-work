#!/usr/bin/env python3
"""
Combined Network & Local Drive Type Checker (Optimized)
- Automatically unmasks network paths and reveals mapped share paths on Windows.
- Bypasses operating system RAM caches to measure actual disk performance.
- Avoids Python CPU bottlenecks when testing fast NVMe drives.
- Requires NO external libraries (zero pip installs).
"""

import os
import sys
import time
import platform
import subprocess
import json

def measure_write_speed(path, test_file_name="speed_test.tmp", total_size_mb=1000):
    """
    Measures true sequential write speed (MB/s).
    Uses a pre-allocated 16MB buffer to prevent Python CPU bottlenecks,
    and forces hardware syncs to bypass system RAM caching.
    """
    test_file_path = os.path.join(path, test_file_name)
    
    # Large chunk sizes reduce loop overhead on high-speed NVMe drives
    chunk_size_mb = 16
    chunk_bytes = chunk_size_mb * 1024 * 1024
    
    print(f"  [Bench] Pre-allocating data buffer...")
    try:
        data_buffer = os.urandom(chunk_bytes)
    except Exception as e:
        print(f"  [Bench] Random data allocation failed, falling back to static bytes: {e}")
        data_buffer = b'\x00' * chunk_bytes

    iterations = max(1, total_size_mb // chunk_size_mb)
    total_written_bytes = iterations * chunk_bytes
    
    print(f"  [Bench] Writing {total_size_mb} MB test file using {chunk_size_mb} MB chunks...")
    
    start_time = time.time()
    try:
        with open(test_file_path, 'wb') as f:
            for _ in range(iterations):
                f.write(data_buffer)
            
            # CRITICAL: Force the operating system to flush RAM buffers to hardware
            f.flush()
            os.fsync(f.fileno())
            
        end_time = time.time()
    except (OSError, IOError) as e:
        print(f"  [Speed test] Write error on {path}: {e}")
        return None
    finally:
        # Cleanup benchmark file immediately
        if os.path.exists(test_file_path):
            try:
                os.remove(test_file_path)
            except OSError:
                pass

    duration = end_time - start_time
    if duration > 0:
        return (total_written_bytes / (1024 * 1024)) / duration
    return None

def classify_by_speed(speed_mb_s):
    """Provides a drive classification heuristic based on true hardware performance."""
    if speed_mb_s is None:
        return "Unknown (speed test failed)"
    if speed_mb_s > 800:
        return "High-Speed NVMe SSD"
    if 200 < speed_mb_s <= 800:
        return "SATA SSD or Fast Network Share"
    if 50 <= speed_mb_s <= 200:
        return "Mechanical HDD or Standard Network Share"
    if 0 < speed_mb_s < 50:
        return "Very Slow Storage (Bottlenecked Network, USB 2.0, or SMR HDD)"
    return "Other"

def detect_windows(drive_root):
    """Uses native PowerShell via subprocess to trace network origins or look up local bus hardware."""
    drive_letter = drive_root.replace(":\\", "").replace(":/", "").strip()
    
    # Step 1: Trace whether this drive letter is a network share/mapped drive
    logic_disk_cmd = (
        f"Get-CimInstance Win32_LogicalDisk -Filter \"DeviceID='{drive_letter}:'\" | "
        "Select-Object DriveType, ProviderName | ConvertTo-Json"
    )
    try:
        net_result = subprocess.run(["powershell", "-Command", logic_disk_cmd], capture_output=True, text=True)
        if net_result.returncode == 0 and net_result.stdout.strip():
            net_info = json.loads(net_result.stdout)
            # DriveType 4 corresponds directly to a network mapped storage volume
            if net_info.get("DriveType") == 4:
                remote_path = net_info.get("ProviderName", "Unknown network target")
                return f"Network Share (Mapped to: {remote_path})"
    except Exception:
        pass

    # Step 2: Fall back to direct local physical block storage bus queries if it's local hardware
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
            
            # Format cleanly based on storage specification numbers
            if str(media) == "4" or str(media).upper() == "SSD":
                return f"{bus} SSD (detected)"
            elif str(media) == "3" or str(media).upper() == "HDD":
                return f"{bus} HDD (detected)"
            elif str(media) == "Unspecified" and str(bus).upper() == "NVME":
                return "NVMe SSD (detected)"
            return f"{bus} Storage Drive (detected)"
    except Exception:
        pass
    return None

def detect_linux(drive_path):
    """Uses lsblk block devices architecture map to determine bus attributes."""
    try:
        df_res = subprocess.run(['df', '--output=source', drive_path], capture_output=True, text=True)
        device = df_res.stdout.strip().split('\n')[1].strip()
        
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
    """Evaluates apple diskutil descriptions for solid-state markers."""
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
    """Wrapper mapping system profiles to corresponding OS-native queries."""
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
            print("No path provided. Exiting.")
            sys.exit(1)

    target = os.path.abspath(target)

    if not os.path.exists(target):
        print(f"Error: Path '{target}' does not exist.")
        sys.exit(1)
    if not os.access(target, os.W_OK):
        print(f"Error: Path '{target}' requires write permissions for evaluation benchmarks.")
        sys.exit(1)

    print(f"\n--- Evaluation target: {target} ---")
    print(f"Platform Architecture: {platform.system()}")

    # Step 1: Trace device paths and look up physical or network definitions
    detection_result = detect_local_drive_type(target)
    print(f"Hardware Layer Info  : {detection_result or 'Not available (Virtual volume or unsupported target)'}")

    # Step 2: Perform the write test with large chunks (Default: 1GB file)
    print("Executing sequential physical speed test...")
    speed = measure_write_speed(target, total_size_mb=1000)
    
    if speed is not None:
        print(f"Measured Write Speed : {speed:.2f} MB/s")
        speed_based = classify_by_speed(speed)
        print(f"Performance Heuristic: {speed_based}")
    else:
        print("Speed test execution failed.")
        speed_based = "Unknown Status"

    # Step 3: Final Analysis Synthesis
    print("\n--- Summary Diagnostic Analysis ---")
    if detection_result:
        print(f"Drive Classification: {detection_result}")
    else:
        print(f"Drive Classification: {speed_based} (Based purely on speed data)")

if __name__ == "__main__":
    main()