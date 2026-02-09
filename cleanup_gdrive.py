#!/usr/bin/env python3
"""
Check video library database and remove already-uploaded files from Google Drive.
"""

import os
import sys
import subprocess

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(script_dir, '.env'))

from app import supabase, USE_SUPABASE, get_all_videos

RCLONE_REMOTE = "gdrive:"

def run_cmd(cmd, check=True):
    """Run a command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        return None
    return result.stdout.strip()

def get_gdrive_files(folder):
    """Get all files in a Google Drive folder."""
    cmd = f'rclone lsf "{RCLONE_REMOTE}{folder}" -R 2>/dev/null'
    output = run_cmd(cmd, check=False)
    if output:
        return [f.strip() for f in output.split('\n') if f.strip() and not f.endswith('/')]
    return []

def delete_from_gdrive(folder, filename):
    """Delete a file from Google Drive."""
    path = f"{folder}/{filename}"
    cmd = f'rclone delete "{RCLONE_REMOTE}{path}" 2>/dev/null'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0

def normalize_title(title):
    """Normalize a title for comparison."""
    # Convert to lowercase, replace underscores with spaces, strip extension
    t = title.lower()
    t = os.path.splitext(t)[0]  # Remove extension
    t = t.replace('_', ' ').replace('-', ' ')
    t = ' '.join(t.split())  # Normalize whitespace
    return t

def main():
    print("=" * 60)
    print("Checking video library for already-uploaded files...")
    print("=" * 60)

    # Get all videos from database
    videos = get_all_videos()
    print(f"Found {len(videos)} videos in database")

    # Build a set of normalized titles from database
    db_titles = set()
    for v in videos:
        title = v.get('title', '')
        normalized = normalize_title(title)
        db_titles.add(normalized)

    print(f"Unique normalized titles in DB: {len(db_titles)}")

    # Check Google Drive folders
    folders_to_check = [
        "1 - Skydiving Competitions/2016 Mondial - Organized",
        "1 - Skydiving Competitions/2018WPC - Organized",
    ]

    total_deleted = 0
    total_found = 0

    for gdrive_folder in folders_to_check:
        print(f"\n{'='*60}")
        print(f"Checking: {gdrive_folder}")
        print(f"{'='*60}")

        # Get files on Google Drive
        gdrive_files = get_gdrive_files(gdrive_folder)
        mp4_files = [f for f in gdrive_files if f.lower().endswith('.mp4')]

        print(f"Found {len(mp4_files)} MP4 files on Google Drive")

        if not mp4_files:
            continue

        # Check each file against database
        deleted = 0
        for filepath in mp4_files:
            filename = os.path.basename(filepath)
            normalized_filename = normalize_title(filename)

            # Check if this video is in the database
            if normalized_filename in db_titles:
                print(f"  Deleting (already in DB): {filepath}")
                if delete_from_gdrive(gdrive_folder, filepath):
                    deleted += 1
                else:
                    print(f"    Failed to delete")
            else:
                total_found += 1

        total_deleted += deleted
        print(f"\nDeleted {deleted} files from {gdrive_folder}")

    print(f"\n{'='*60}")
    print(f"Total deleted: {total_deleted} files")
    print(f"Files still on GDrive (not in DB): {total_found}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
