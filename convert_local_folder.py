#!/usr/bin/env python3
"""
Convert local video files to MP4, upload to S3, and delete originals.
"""

import os
import sys
import subprocess
import uuid
from datetime import datetime

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(script_dir, '.env'))

from app import USE_S3, upload_to_s3, save_video, generate_thumbnail

# Video extensions to convert
VIDEO_EXTENSIONS = ('.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mts', '.m2ts', '.ts', '.mp4')
TEMP_DIR = "/tmp/video_convert"

def convert_and_upload(input_path, event_name, category='uncategorized', delete_original=True):
    """Convert a video file to MP4, upload to S3, and optionally delete original."""

    filename = os.path.basename(input_path)
    name_without_ext = os.path.splitext(filename)[0]
    ext = os.path.splitext(filename)[1].lower()

    # Check if already MP4
    needs_conversion = ext != '.mp4'

    os.makedirs(TEMP_DIR, exist_ok=True)

    if needs_conversion:
        output_path = os.path.join(TEMP_DIR, f"{name_without_ext}.mp4")

        print(f"  Converting to MP4...")
        cmd = f'ffmpeg -i "{input_path}" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k -y "{output_path}" 2>/dev/null'
        result = subprocess.run(cmd, shell=True, capture_output=True)

        if result.returncode != 0 or not os.path.exists(output_path):
            print(f"  Conversion failed!")
            return False

        upload_file = output_path
    else:
        upload_file = input_path

    # Generate video ID
    video_id = str(uuid.uuid4())[:8]
    s3_filename = f"{video_id}.mp4"
    s3_folder = f"{category}"

    # Upload to S3
    print(f"  Uploading to S3...")
    if not USE_S3:
        print("  ERROR: S3 not configured!")
        if needs_conversion and os.path.exists(output_path):
            os.remove(output_path)
        return False

    with open(upload_file, 'rb') as f:
        file_data = f.read()

    video_url = upload_to_s3(file_data, s3_filename, 'video/mp4', s3_folder)

    if not video_url:
        print("  S3 upload failed!")
        if needs_conversion and os.path.exists(output_path):
            os.remove(output_path)
        return False

    print(f"  Uploaded: {video_url}")

    # Generate thumbnail
    thumbnail_url = ''
    try:
        import tempfile
        thumb_path = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False).name
        if generate_thumbnail(upload_file, thumb_path):
            with open(thumb_path, 'rb') as f:
                thumb_data = f.read()
            thumb_filename = f"{video_id}_thumb.jpg"
            thumbnail_url = upload_to_s3(thumb_data, thumb_filename, 'image/jpeg', 'thumbnails')
        os.unlink(thumb_path)
    except Exception as e:
        print(f"  Thumbnail error (non-fatal): {e}")

    # Save to database
    title = name_without_ext.replace('_', ' ').replace('-', ' ')

    video_data = {
        'id': video_id,
        'title': title,
        'description': f"From {event_name}",
        'url': video_url,
        'thumbnail': thumbnail_url,
        'category': category,
        'subcategory': '',
        'tags': event_name,
        'duration': None,
        'created_at': datetime.now().isoformat(),
        'views': 0,
        'video_type': 's3',
        'local_file': '',
        'event': event_name,
        'team': '',
        'round_num': '',
        'jump_num': '',
    }

    save_video(video_data)
    print(f"  Saved to database: {video_id}")

    # Cleanup converted file
    if needs_conversion and os.path.exists(output_path):
        os.remove(output_path)

    # Delete original if requested
    if delete_original:
        print(f"  Deleting original...")
        os.remove(input_path)

    print(f"  Done!")
    return True

def process_folder(folder_path, event_name, category='uncategorized', delete_originals=True):
    """Process all video files in a folder."""

    print(f"\n{'='*60}")
    print(f"Processing: {folder_path}")
    print(f"Event: {event_name}")
    print(f"{'='*60}\n")

    # Get video files
    files = []
    for f in os.listdir(folder_path):
        ext = os.path.splitext(f)[1].lower()
        if ext in VIDEO_EXTENSIONS:
            files.append(f)

    if not files:
        print("No video files found.")
        return

    print(f"Found {len(files)} video files\n")

    success = 0
    failed = 0

    for i, filename in enumerate(files, 1):
        filepath = os.path.join(folder_path, filename)
        print(f"[{i}/{len(files)}] {filename}")

        if convert_and_upload(filepath, event_name, category, delete_originals):
            success += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"Completed: {success} uploaded, {failed} failed")
    print(f"{'='*60}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python convert_local_folder.py <folder_path> <event_name> [category] [--keep]")
        print("Example: python convert_local_folder.py '/path/to/videos' 'Nationals 2025' 'cp'")
        print("  --keep: Don't delete original files after upload")
        sys.exit(1)

    folder_path = sys.argv[1]
    event_name = sys.argv[2]
    category = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else 'uncategorized'
    delete_originals = '--keep' not in sys.argv

    process_folder(folder_path, event_name, category, delete_originals)

if __name__ == '__main__':
    main()
