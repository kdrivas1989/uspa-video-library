#!/usr/bin/env python3
"""
Batch upload MP4 files from Google Drive to the Video Library via Backblaze B2.
Downloads, uploads to B2, saves to database, then deletes from Google Drive.
(Also supports AWS S3 as fallback if B2 not configured)
"""

import os
import sys
import subprocess
import uuid
from datetime import datetime

# Add parent directory to path to import app modules
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

# Load .env file before importing app
from dotenv import load_dotenv
load_dotenv(os.path.join(script_dir, '.env'))

from app import (
    USE_S3, upload_to_s3_from_path, save_video, generate_thumbnail, upload_to_s3
)

# Configuration
RCLONE_REMOTE = "gdrive:"
TEMP_DIR = "/tmp/video_upload"
EVENT_NAME = "2016 Mondial"  # Default event name

def run_cmd(cmd, check=True):
    """Run a command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Error: {result.stderr}")
        return None
    return result.stdout.strip()

def get_mp4_files(folder):
    """Get list of MP4 files in a folder."""
    cmd = f'rclone lsf "{RCLONE_REMOTE}{folder}" -R 2>/dev/null | grep -i "\\.mp4$"'
    output = run_cmd(cmd, check=False)
    if output:
        return [f.strip() for f in output.split('\n') if f.strip()]
    return []

def parse_video_metadata(relative_path, event_name):
    """Parse metadata from file path and name."""
    # Path structure: Category/filename.mp4
    parts = relative_path.split('/')
    category = parts[0] if len(parts) > 1 else "Uncategorized"
    filename = parts[-1]
    name_without_ext = os.path.splitext(filename)[0]

    # Map folder names to categories
    category_mapping = {
        '2 Way VFS': 'fs_4way_vfs',
        '2-Way VFS': 'fs_4way_vfs',
        '4 Way': 'fs_4way_fs',
        '4-Way': 'fs_4way_fs',
        '4 Way Open': 'fs_4way_fs',
        '4 Way Female': 'fs_4way_fs',
        '8 Way': 'fs_8way',
        '8-Way': 'fs_8way',
        'CF2': 'cf_2way_open',
        'CF4Rot': 'cf_4way_rot',
        'CF4Seq': 'cf_4way_seq',
        'AE': 'ae',
        'AEFreeFly': 'ae_freefly',
        'AEFreeStyle': 'ae_freestyle',
        'VFS': 'fs_4way_vfs',
        'Rots': 'cf_4way_rot',
    }

    # Try to match category
    db_category = 'uncategorized'
    for folder_name, cat_id in category_mapping.items():
        if folder_name.lower() in category.lower():
            db_category = cat_id
            break

    # Try to extract round and team from filename
    round_num = ''
    team = ''
    parts = name_without_ext.split('_')
    if len(parts) >= 3:
        try:
            round_num = parts[1]
            team = parts[2]
        except:
            pass

    return {
        'category': db_category,
        'subcategory': category,
        'event': event_name,
        'title': name_without_ext.replace('_', ' '),
        'round_num': round_num,
        'team': team,
    }

def upload_file(gdrive_folder, relative_path, event_name):
    """Download from GDrive, upload to S3, save to DB, delete from GDrive."""

    filename = os.path.basename(relative_path)
    local_path = os.path.join(TEMP_DIR, filename)
    gdrive_path = f"{gdrive_folder}/{relative_path}"

    try:
        # 1. Download from Google Drive
        print(f"  Downloading {filename}...")
        cmd = f'rclone copy "{RCLONE_REMOTE}{gdrive_path}" "{TEMP_DIR}" 2>/dev/null'
        if run_cmd(cmd) is None:
            return False

        if not os.path.exists(local_path):
            print(f"  Download failed: {filename}")
            return False

        # 2. Parse metadata
        metadata = parse_video_metadata(relative_path, event_name)
        video_id = str(uuid.uuid4())[:8]

        # 3. Upload to AWS S3
        print(f"  Uploading to S3...")
        if not USE_S3:
            print(f"  ERROR: S3 is not configured! Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_S3_BUCKET env vars.")
            os.remove(local_path)
            return False

        # Create S3 folder structure: category/subcategory/
        s3_folder = f"{metadata['category']}/{metadata['subcategory']}" if metadata['subcategory'] else metadata['category']
        s3_filename = f"{video_id}.mp4"

        # Read file and upload
        with open(local_path, 'rb') as f:
            file_data = f.read()

        video_url = upload_to_s3(file_data, s3_filename, 'video/mp4', s3_folder)

        if not video_url:
            print(f"  S3 upload failed: {filename}")
            os.remove(local_path)
            return False

        print(f"  Uploaded to: {video_url}")

        # 4. Generate and upload thumbnail
        thumbnail_url = ''
        try:
            import tempfile
            thumb_path = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False).name
            if generate_thumbnail(local_path, thumb_path):
                with open(thumb_path, 'rb') as f:
                    thumb_data = f.read()
                thumb_filename = f"{video_id}_thumb.jpg"
                thumbnail_url = upload_to_s3(thumb_data, thumb_filename, 'image/jpeg', 'thumbnails')
            try:
                os.unlink(thumb_path)
            except:
                pass
        except Exception as e:
            print(f"  Thumbnail error (non-fatal): {e}")

        # 5. Save video metadata to database
        video_data = {
            'id': video_id,
            'title': metadata['title'],
            'description': f"From {event_name} - {metadata['subcategory']}",
            'url': video_url,
            'thumbnail': thumbnail_url,
            'category': metadata['category'],
            'subcategory': metadata['subcategory'],
            'tags': f"{event_name},{metadata['subcategory']}",
            'duration': None,
            'created_at': datetime.now().isoformat(),
            'views': 0,
            'video_type': 's3',
            'local_file': '',
            'event': metadata['event'],
            'team': metadata['team'],
            'round_num': metadata['round_num'],
            'jump_num': '',
        }

        save_video(video_data)
        print(f"  Saved to database: {video_id}")

        # 6. Delete from Google Drive
        print(f"  Deleting from Google Drive...")
        cmd = f'rclone delete "{RCLONE_REMOTE}{gdrive_path}" 2>/dev/null'
        run_cmd(cmd, check=False)

        # 7. Cleanup local file
        os.remove(local_path)

        print(f"  Done: {filename}")
        return True

    except Exception as e:
        print(f"  Error: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False

def process_folder(gdrive_folder, event_name=None):
    """Process all MP4 files in a folder."""
    if event_name is None:
        event_name = EVENT_NAME

    print(f"\n{'='*60}")
    print(f"Uploading to Video Library (AWS S3): {gdrive_folder}")
    print(f"Event: {event_name}")
    print(f"{'='*60}")

    # Check S3 configuration
    if not USE_S3:
        print("\nERROR: AWS S3 is not configured!")
        print("Please set the following environment variables:")
        print("  - AWS_ACCESS_KEY_ID")
        print("  - AWS_SECRET_ACCESS_KEY")
        print("  - AWS_S3_BUCKET")
        print("  - AWS_REGION (optional, defaults to us-east-1)")
        return

    # Get MP4 files
    mp4_files = get_mp4_files(gdrive_folder)

    if not mp4_files:
        print("No MP4 files found.")
        return

    print(f"Found {len(mp4_files)} MP4 files to upload.\n")

    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)

    success = 0
    failed = 0

    for i, mp4_file in enumerate(mp4_files, 1):
        print(f"\n[{i}/{len(mp4_files)}] {mp4_file}")

        if upload_file(gdrive_folder, mp4_file, event_name):
            success += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"Completed: {success} uploaded to S3, {failed} failed")
    print(f"{'='*60}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python batch_upload_to_library.py <gdrive_folder> [event_name]")
        print("Example: python batch_upload_to_library.py '1 - Skydiving Competitions/2016 Mondial - Organized' '2016 Mondial'")
        print("\nRequired environment variables:")
        print("  - AWS_ACCESS_KEY_ID")
        print("  - AWS_SECRET_ACCESS_KEY")
        print("  - AWS_S3_BUCKET")
        sys.exit(1)

    folder = sys.argv[1]
    event_name = sys.argv[2] if len(sys.argv) > 2 else EVENT_NAME
    process_folder(folder, event_name)

if __name__ == '__main__':
    main()
