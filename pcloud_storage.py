"""
pCloud Storage Integration

Uses rclone for file operations and provides streaming through Flask.
"""

import os
import subprocess
import tempfile
from functools import lru_cache

# Configuration
PCLOUD_REMOTE = os.environ.get('PCLOUD_REMOTE', 'pcloud')
PCLOUD_BASE_FOLDER = os.environ.get('PCLOUD_BASE_FOLDER', 'video-library')
USE_PCLOUD = os.environ.get('USE_PCLOUD', 'false').lower() == 'true'

def check_pcloud_configured():
    """Check if pCloud is configured in rclone."""
    try:
        result = subprocess.run(
            ['rclone', 'listremotes'],
            capture_output=True, text=True, timeout=10
        )
        remotes = result.stdout.strip().split('\n')
        return f"{PCLOUD_REMOTE}:" in remotes
    except Exception as e:
        print(f"pCloud check error: {e}")
        return False

def upload_to_pcloud(file_path, remote_path, folder='videos'):
    """
    Upload a file to pCloud using rclone.

    Args:
        file_path: Local path to the file
        remote_path: Filename to use on pCloud
        folder: Subfolder within the base folder

    Returns:
        pCloud path (e.g., "videos/abc123.mp4") or None on failure
    """
    if not USE_PCLOUD:
        return None

    try:
        # Build the full remote path
        pcloud_folder = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{folder}"

        # Ensure the folder exists
        subprocess.run(
            ['rclone', 'mkdir', pcloud_folder],
            capture_output=True, timeout=30
        )

        # Upload the file
        dest = f"{pcloud_folder}/{remote_path}"
        result = subprocess.run(
            ['rclone', 'copyto', file_path, dest],
            capture_output=True, text=True, timeout=600  # 10 min timeout for large files
        )

        if result.returncode != 0:
            print(f"pCloud upload error: {result.stderr}")
            return None

        # Return the relative path for database storage
        return f"{folder}/{remote_path}"

    except Exception as e:
        print(f"pCloud upload error: {e}")
        return None

def upload_to_pcloud_from_data(file_data, filename, folder='videos'):
    """
    Upload file data to pCloud.

    Args:
        file_data: Bytes of the file
        filename: Name for the file
        folder: Subfolder within the base folder

    Returns:
        pCloud path or None on failure
    """
    if not USE_PCLOUD:
        return None

    try:
        # Write to temp file first
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
            tmp.write(file_data)
            tmp_path = tmp.name

        # Upload using the path-based function
        result = upload_to_pcloud(tmp_path, filename, folder)

        # Clean up temp file
        os.unlink(tmp_path)

        return result

    except Exception as e:
        print(f"pCloud upload from data error: {e}")
        return None

def delete_from_pcloud(pcloud_path):
    """Delete a file from pCloud."""
    if not USE_PCLOUD:
        return False

    try:
        full_path = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{pcloud_path}"
        result = subprocess.run(
            ['rclone', 'delete', full_path],
            capture_output=True, text=True, timeout=60
        )
        return result.returncode == 0
    except Exception as e:
        print(f"pCloud delete error: {e}")
        return False

def get_pcloud_file_stream(pcloud_path):
    """
    Get a file from pCloud as a stream for proxying.

    Returns a generator that yields chunks of the file.
    """
    if not USE_PCLOUD:
        return None

    try:
        full_path = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{pcloud_path}"

        # Use rclone cat to stream the file
        process = subprocess.Popen(
            ['rclone', 'cat', full_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Yield chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        while True:
            chunk = process.stdout.read(chunk_size)
            if not chunk:
                break
            yield chunk

        process.wait()

    except Exception as e:
        print(f"pCloud stream error: {e}")
        return None

def get_pcloud_file_size(pcloud_path):
    """Get the size of a file on pCloud."""
    if not USE_PCLOUD:
        return None

    try:
        full_path = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{pcloud_path}"
        result = subprocess.run(
            ['rclone', 'size', full_path, '--json'],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            return data.get('bytes', 0)
    except Exception as e:
        print(f"pCloud size error: {e}")

    return None

def list_pcloud_files(folder='videos'):
    """List files in a pCloud folder."""
    if not USE_PCLOUD:
        return []

    try:
        full_path = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{folder}"
        result = subprocess.run(
            ['rclone', 'lsf', full_path, '-R'],
            capture_output=True, text=True, timeout=60
        )

        if result.returncode == 0:
            files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
            return files
    except Exception as e:
        print(f"pCloud list error: {e}")

    return []

def get_pcloud_public_link(pcloud_path):
    """
    Get a public link for a pCloud file.
    Note: This requires the file to be in a public folder or uses pCloud's link API.
    For streaming through the app, use the proxy endpoint instead.
    """
    if not USE_PCLOUD:
        return None

    try:
        full_path = f"{PCLOUD_REMOTE}:{PCLOUD_BASE_FOLDER}/{pcloud_path}"
        result = subprocess.run(
            ['rclone', 'link', full_path],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            return result.stdout.strip()
    except Exception as e:
        print(f"pCloud link error: {e}")

    return None


# Initialize on import
if USE_PCLOUD:
    if check_pcloud_configured():
        print(f"[STARTUP] pCloud configured: Remote={PCLOUD_REMOTE}, Folder={PCLOUD_BASE_FOLDER}")
    else:
        print(f"[STARTUP] pCloud enabled but rclone remote '{PCLOUD_REMOTE}' not found!")
        USE_PCLOUD = False
else:
    print(f"[STARTUP] pCloud not enabled (set USE_PCLOUD=true to enable)")
