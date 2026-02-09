#!/usr/bin/env python3
"""
Google Drive Video Mover
Moves videos from Round subfolders to their parent Category folders.

Setup:
1. Go to https://console.cloud.google.com/
2. Create a new project (or select existing)
3. Enable the Google Drive API
4. Go to Credentials > Create Credentials > OAuth 2.0 Client ID
5. Choose "Desktop app"
6. Download the JSON file and save as 'credentials.json' in this directory
7. Run this script
"""

import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the token.pickle file
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_credentials():
    """Get valid user credentials from storage or initiate OAuth flow."""
    creds = None
    token_path = 'token.pickle'
    creds_path = 'credentials.json'

    # Check for existing token
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # If no valid credentials, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(creds_path):
                print("\n❌ credentials.json not found!")
                print("\nSetup instructions:")
                print("1. Go to https://console.cloud.google.com/")
                print("2. Create a new project (or select existing)")
                print("3. Search for 'Google Drive API' and enable it")
                print("4. Go to 'Credentials' in the left menu")
                print("5. Click 'Create Credentials' > 'OAuth 2.0 Client ID'")
                print("6. If prompted, configure consent screen (External, add your email as test user)")
                print("7. Choose 'Desktop app' as application type")
                print("8. Download the JSON file")
                print("9. Save it as 'credentials.json' in this directory:")
                print(f"   {os.getcwd()}")
                print("\nThen run this script again.")
                return None

            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save credentials for next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return creds

def find_folder_by_name(service, name, parent_id=None):
    """Find a folder by name, optionally within a parent folder."""
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()

    files = results.get('files', [])
    return files[0] if files else None

def list_folders_in_folder(service, parent_id):
    """List all folders within a parent folder."""
    query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"

    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)',
        pageSize=100
    ).execute()

    return results.get('files', [])

def list_files_in_folder(service, parent_id):
    """List all non-folder files within a folder."""
    query = f"'{parent_id}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false"

    all_files = []
    page_token = None

    while True:
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageSize=100,
            pageToken=page_token
        ).execute()

        all_files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')

        if not page_token:
            break

    return all_files

def move_file(service, file_id, old_parent_id, new_parent_id):
    """Move a file from one folder to another."""
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        fields='id, parents'
    ).execute()

def main():
    print("🎬 Google Drive Video Mover")
    print("=" * 50)

    # Get credentials
    creds = get_credentials()
    if not creds:
        return

    # Build the service
    service = build('drive', 'v3', credentials=creds)
    print("✅ Connected to Google Drive")

    # Find the 2016 Mondial folder
    mondial_folder = find_folder_by_name(service, "2016 Mondial")
    if not mondial_folder:
        print("❌ Could not find '2016 Mondial' folder")
        return

    print(f"📁 Found: {mondial_folder['name']} (ID: {mondial_folder['id']})")

    # Get all category folders
    categories = list_folders_in_folder(service, mondial_folder['id'])
    print(f"\n📂 Found {len(categories)} categories:")
    for cat in categories:
        print(f"   - {cat['name']}")

    # Process each category
    total_moved = 0
    for category in categories:
        print(f"\n{'='*50}")
        print(f"📁 Processing: {category['name']}")

        # Get round folders within this category
        round_folders = list_folders_in_folder(service, category['id'])
        round_folders = [f for f in round_folders if f['name'].lower().startswith('round')]

        if not round_folders:
            print("   No round folders found")
            continue

        print(f"   Found {len(round_folders)} round folders")

        # Process each round folder
        for round_folder in round_folders:
            print(f"\n   📂 {round_folder['name']}:")

            # Get files in this round folder
            files = list_files_in_folder(service, round_folder['id'])
            video_files = [f for f in files if f['name'].lower().endswith(('.mp4', '.mkv', '.mov', '.avi', '.mts', '.m2ts'))]

            if not video_files:
                print(f"      No video files found")
                continue

            print(f"      Moving {len(video_files)} videos to {category['name']}...")

            # Move each video file to the category folder
            for i, video in enumerate(video_files, 1):
                try:
                    move_file(service, video['id'], round_folder['id'], category['id'])
                    print(f"      [{i}/{len(video_files)}] ✅ {video['name']}")
                    total_moved += 1
                except Exception as e:
                    print(f"      [{i}/{len(video_files)}] ❌ {video['name']}: {e}")

    print(f"\n{'='*50}")
    print(f"✅ Done! Moved {total_moved} videos total.")
    print("\nNote: The round folders are now empty but still exist.")
    print("You can delete them manually from Google Drive if desired.")

if __name__ == '__main__':
    main()
