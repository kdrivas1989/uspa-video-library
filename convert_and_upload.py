#!/usr/bin/env python3
"""
Convert MKV files to MP4 and upload to Google Drive.
Downloads one file at a time to save disk space.
"""

import os
import subprocess
import sys
import tempfile
import shutil

# Configuration
RCLONE_REMOTE = "gdrive:"
TEMP_DIR = "/tmp/video_convert"

def run_cmd(cmd, check=True):
    """Run a command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Error: {result.stderr}")
        return None
    return result.stdout.strip()

def get_mkv_files(folder):
    """Get list of MKV files in a folder."""
    cmd = f'rclone lsf "{RCLONE_REMOTE}{folder}" -R 2>/dev/null | grep -i "\\.mkv$"'
    output = run_cmd(cmd, check=False)
    if output:
        return [f.strip() for f in output.split('\n') if f.strip()]
    return []

def convert_file(gdrive_folder, relative_path):
    """Download, convert, upload, and delete original."""

    # Setup paths
    filename = os.path.basename(relative_path)
    name_without_ext = os.path.splitext(filename)[0]
    mp4_filename = f"{name_without_ext}.mp4"

    # Determine the subfolder path
    subfolder = os.path.dirname(relative_path)

    local_mkv = os.path.join(TEMP_DIR, filename)
    local_mp4 = os.path.join(TEMP_DIR, mp4_filename)

    gdrive_mkv_path = f"{gdrive_folder}/{relative_path}"
    if subfolder:
        gdrive_mp4_folder = f"{gdrive_folder}/{subfolder}"
    else:
        gdrive_mp4_folder = gdrive_folder

    try:
        # 1. Download MKV
        print(f"  ⬇️  Downloading {filename}...")
        cmd = f'rclone copy "{RCLONE_REMOTE}{gdrive_mkv_path}" "{TEMP_DIR}" 2>/dev/null'
        if run_cmd(cmd) is None:
            return False

        if not os.path.exists(local_mkv):
            print(f"  ❌ Download failed: {filename}")
            return False

        # 2. Convert to MP4
        print(f"  🔄 Converting to MP4...")
        cmd = f'ffmpeg -i "{local_mkv}" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k -y "{local_mp4}" 2>/dev/null'
        result = subprocess.run(cmd, shell=True, capture_output=True)

        if result.returncode != 0 or not os.path.exists(local_mp4):
            print(f"  ❌ Conversion failed: {filename}")
            # Cleanup
            if os.path.exists(local_mkv):
                os.remove(local_mkv)
            return False

        # 3. Upload MP4
        print(f"  ⬆️  Uploading {mp4_filename}...")
        cmd = f'rclone copy "{local_mp4}" "{RCLONE_REMOTE}{gdrive_mp4_folder}" 2>/dev/null'
        if run_cmd(cmd) is None:
            # Cleanup
            os.remove(local_mkv)
            os.remove(local_mp4)
            return False

        # 4. Delete original MKV from Google Drive
        print(f"  🗑️  Deleting original MKV...")
        cmd = f'rclone delete "{RCLONE_REMOTE}{gdrive_mkv_path}" 2>/dev/null'
        run_cmd(cmd, check=False)

        # 5. Cleanup local files
        os.remove(local_mkv)
        os.remove(local_mp4)

        print(f"  ✅ Done: {mp4_filename}")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        # Cleanup
        if os.path.exists(local_mkv):
            os.remove(local_mkv)
        if os.path.exists(local_mp4):
            os.remove(local_mp4)
        return False

def process_folder(gdrive_folder):
    """Process all MKV files in a folder."""
    print(f"\n{'='*60}")
    print(f"📁 Processing: {gdrive_folder}")
    print(f"{'='*60}")

    # Get MKV files
    mkv_files = get_mkv_files(gdrive_folder)

    if not mkv_files:
        print("No MKV files found.")
        return

    print(f"Found {len(mkv_files)} MKV files to convert.\n")

    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)

    success = 0
    failed = 0

    for i, mkv_file in enumerate(mkv_files, 1):
        print(f"\n[{i}/{len(mkv_files)}] {mkv_file}")

        if convert_file(gdrive_folder, mkv_file):
            success += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"✅ Completed: {success} converted, {failed} failed")
    print(f"{'='*60}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_and_upload.py <gdrive_folder>")
        print("Example: python convert_and_upload.py '1 - Skydiving Competitions/2016 Mondial - Organized'")
        sys.exit(1)

    folder = sys.argv[1]
    process_folder(folder)

if __name__ == '__main__':
    main()
