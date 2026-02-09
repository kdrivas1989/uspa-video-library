#!/usr/bin/env python3
"""
Auto-convert watcher: Monitors Google Drive folders for non-MP4 video files
and automatically converts them to MP4.

Runs continuously, checking every few minutes for new files.
"""

import os
import sys
import subprocess
import time
import json
from datetime import datetime

# Configuration
RCLONE_REMOTE = "gdrive:"
TEMP_DIR = "/tmp/video_convert"
CHECK_INTERVAL = 300  # Check every 5 minutes
PROCESSED_FILE = "/tmp/auto_convert_processed.json"

# Video extensions to convert
VIDEO_EXTENSIONS = ('.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mts', '.m2ts', '.ts')

# Folders to watch (add more as needed)
WATCH_FOLDERS = [
    "1 - Skydiving Competitions",
    "Videos",
]

def log(msg):
    """Log with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def run_cmd(cmd, check=True):
    """Run a command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        return None
    return result.stdout.strip()

def load_processed():
    """Load list of already processed files."""
    try:
        if os.path.exists(PROCESSED_FILE):
            with open(PROCESSED_FILE, 'r') as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_processed(processed):
    """Save list of processed files."""
    try:
        with open(PROCESSED_FILE, 'w') as f:
            json.dump(list(processed), f)
    except Exception as e:
        log(f"Warning: Could not save processed list: {e}")

def get_video_files(folder):
    """Get list of non-MP4 video files in a folder recursively."""
    cmd = f'rclone lsf "{RCLONE_REMOTE}{folder}" -R 2>/dev/null'
    output = run_cmd(cmd, check=False)

    if not output:
        return []

    files = []
    for line in output.split('\n'):
        line = line.strip()
        if not line or line.endswith('/'):
            continue

        ext = os.path.splitext(line)[1].lower()
        if ext in VIDEO_EXTENSIONS:
            files.append(line)

    return files

def convert_file(folder, relative_path):
    """Download, convert to MP4, upload, and delete original."""

    filename = os.path.basename(relative_path)
    name_without_ext = os.path.splitext(filename)[0]
    mp4_filename = f"{name_without_ext}.mp4"

    subfolder = os.path.dirname(relative_path)

    local_input = os.path.join(TEMP_DIR, filename)
    local_output = os.path.join(TEMP_DIR, mp4_filename)

    gdrive_input_path = f"{folder}/{relative_path}"
    if subfolder:
        gdrive_output_folder = f"{folder}/{subfolder}"
    else:
        gdrive_output_folder = folder

    try:
        # 1. Download
        log(f"  Downloading {filename}...")
        cmd = f'rclone copy "{RCLONE_REMOTE}{gdrive_input_path}" "{TEMP_DIR}" 2>/dev/null'
        if run_cmd(cmd) is None:
            log(f"  Failed to download")
            return False

        if not os.path.exists(local_input):
            log(f"  Download failed: file not found")
            return False

        # 2. Convert
        log(f"  Converting to MP4...")
        cmd = f'ffmpeg -i "{local_input}" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k -y "{local_output}" 2>/dev/null'
        result = subprocess.run(cmd, shell=True, capture_output=True)

        if result.returncode != 0 or not os.path.exists(local_output):
            log(f"  Conversion failed")
            if os.path.exists(local_input):
                os.remove(local_input)
            return False

        # 3. Upload MP4
        log(f"  Uploading {mp4_filename}...")
        cmd = f'rclone copy "{local_output}" "{RCLONE_REMOTE}{gdrive_output_folder}" 2>/dev/null'
        if run_cmd(cmd) is None:
            log(f"  Upload failed")
            os.remove(local_input)
            os.remove(local_output)
            return False

        # 4. Delete original from Google Drive
        log(f"  Deleting original...")
        cmd = f'rclone delete "{RCLONE_REMOTE}{gdrive_input_path}" 2>/dev/null'
        run_cmd(cmd, check=False)

        # 5. Cleanup local files
        os.remove(local_input)
        os.remove(local_output)

        log(f"  Done: {mp4_filename}")
        return True

    except Exception as e:
        log(f"  Error: {e}")
        if os.path.exists(local_input):
            os.remove(local_input)
        if os.path.exists(local_output):
            os.remove(local_output)
        return False

def scan_and_convert():
    """Scan all watch folders and convert any non-MP4 videos."""
    processed = load_processed()
    new_processed = set()
    total_converted = 0

    for folder in WATCH_FOLDERS:
        log(f"Scanning: {folder}")

        video_files = get_video_files(folder)
        if not video_files:
            log(f"  No convertible videos found")
            continue

        log(f"  Found {len(video_files)} video files to check")

        for video_file in video_files:
            full_path = f"{folder}/{video_file}"

            # Skip if already processed
            if full_path in processed:
                continue

            log(f"\nConverting: {video_file}")

            if convert_file(folder, video_file):
                total_converted += 1
                new_processed.add(full_path)
            else:
                # Mark as processed even if failed to avoid retry loop
                new_processed.add(full_path)

    # Save updated processed list
    processed.update(new_processed)
    save_processed(processed)

    return total_converted

def main():
    log("=" * 60)
    log("Auto-Convert Watcher Started")
    log(f"Watching folders: {WATCH_FOLDERS}")
    log(f"Check interval: {CHECK_INTERVAL} seconds")
    log("=" * 60)

    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)

    while True:
        try:
            converted = scan_and_convert()
            if converted > 0:
                log(f"\nConverted {converted} files this cycle")
            else:
                log("No new files to convert")

        except Exception as e:
            log(f"Error during scan: {e}")

        log(f"\nSleeping for {CHECK_INTERVAL} seconds...")
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    # Allow running once with --once flag
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        os.makedirs(TEMP_DIR, exist_ok=True)
        converted = scan_and_convert()
        print(f"Converted {converted} files")
    else:
        main()
