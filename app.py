#!/usr/bin/env python3
"""USPA Video Library - Video database for skydiving disciplines."""

import os
import uuid
import json
import subprocess
import shutil
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_from_directory, g
from functools import wraps
import sqlite3

# Database support - Supabase for production, SQLite for local dev
try:
    from supabase import create_client, Client
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
    if SUPABASE_URL and SUPABASE_KEY:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        USE_SUPABASE = True
    else:
        USE_SUPABASE = False
        supabase = None
except ImportError:
    USE_SUPABASE = False
    supabase = None

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'uspa-video-library-secret-key')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')

# Video storage paths (for local development)
VIDEOS_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'videos')
os.makedirs(VIDEOS_FOLDER, exist_ok=True)

# Categories
CATEGORIES = {
    'al': {
        'name': 'Accuracy Landing',
        'abbrev': 'AL',
        'description': 'Chapter 8 - Accuracy Landing competition videos',
        'subcategories': []
    },
    'cf': {
        'name': 'Canopy Formation',
        'abbrev': 'CF',
        'description': 'Chapter 10 - Canopy Formation competition videos',
        'subcategories': [
            {'id': '4way', 'name': '4-Way'},
            {'id': '2way', 'name': '2-Way'}
        ]
    },
    'cp': {
        'name': 'Canopy Piloting',
        'abbrev': 'CP',
        'description': 'Chapters 12-13 - Canopy Piloting competition videos',
        'subcategories': [
            {'id': 'speed', 'name': 'Speed'},
            {'id': 'distance', 'name': 'Distance'},
            {'id': 'zone_accuracy', 'name': 'Zone Accuracy'},
            {'id': 'freestyle', 'name': 'Freestyle'}
        ]
    },
    'ae': {
        'name': 'Artistic Events',
        'abbrev': 'AE',
        'description': 'Chapter 11 - Freestyle and Freefly competition videos',
        'subcategories': [
            {'id': 'freestyle', 'name': 'Freestyle'},
            {'id': 'freefly', 'name': 'Freefly'}
        ]
    },
    'ws': {
        'name': 'Wingsuit',
        'abbrev': 'WS',
        'description': 'Chapter 14 - Wingsuit competition videos',
        'subcategories': []
    },
    'fs': {
        'name': 'Formation Skydiving',
        'abbrev': 'FS',
        'description': 'Chapter 9 - Formation Skydiving competition videos',
        'subcategories': [
            {'id': '4way_fs', 'name': '4-Way FS'},
            {'id': '4way_vfs', 'name': '4-Way VFS'},
            {'id': '2way_mfs', 'name': '2-Way MFS'},
            {'id': '8way', 'name': '8-Way'},
            {'id': '16way', 'name': '16-Way'},
            {'id': '10way', 'name': '10-Way'}
        ]
    }
}

DATABASE = 'videos.db'


def get_sqlite_db():
    """Get SQLite database connection for local development."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exception):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    """Initialize the database."""
    if USE_SUPABASE:
        try:
            result = supabase.table('users').select('username').eq('username', 'admin').execute()
            if not result.data:
                supabase.table('users').insert({
                    'username': 'admin',
                    'password': 'admin123',
                    'role': 'admin',
                    'name': 'Administrator'
                }).execute()
        except Exception as e:
            print(f"Supabase init error: {e}")
    else:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                url TEXT NOT NULL,
                thumbnail TEXT,
                category TEXT NOT NULL,
                subcategory TEXT,
                tags TEXT,
                duration TEXT,
                created_at TEXT NOT NULL,
                views INTEGER DEFAULT 0,
                video_type TEXT DEFAULT 'url',
                local_file TEXT,
                event TEXT
            )
        ''')

        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN video_type TEXT DEFAULT "url"')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN local_file TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN event TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN team TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN round_num TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN jump_num TEXT')
        except:
            pass

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                name TEXT NOT NULL
            )
        ''')

        cursor.execute('SELECT username FROM users WHERE username = ?', ('admin',))
        if not cursor.fetchone():
            cursor.execute(
                'INSERT INTO users (username, password, role, name) VALUES (?, ?, ?, ?)',
                ('admin', 'admin123', 'admin', 'Administrator')
            )

        conn.commit()
        conn.close()


# Database helper functions
def get_all_videos():
    """Get all videos from database."""
    if USE_SUPABASE:
        result = supabase.table('videos').select('*').order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM videos ORDER BY created_at DESC')
        return [dict(row) for row in cursor.fetchall()]


def get_videos_by_category(category, subcategory=None):
    """Get videos by category and optional subcategory."""
    if USE_SUPABASE:
        query = supabase.table('videos').select('*').eq('category', category)
        if subcategory:
            query = query.eq('subcategory', subcategory)
        result = query.order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        if subcategory:
            cursor = db.execute(
                'SELECT * FROM videos WHERE category = ? AND subcategory = ? ORDER BY created_at DESC',
                (category, subcategory)
            )
        else:
            cursor = db.execute(
                'SELECT * FROM videos WHERE category = ? ORDER BY created_at DESC',
                (category,)
            )
        return [dict(row) for row in cursor.fetchall()]


def get_video(video_id):
    """Get a single video by ID."""
    if USE_SUPABASE:
        result = supabase.table('videos').select('*').eq('id', video_id).execute()
        return result.data[0] if result.data else None
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM videos WHERE id = ?', (video_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def save_video(video_data):
    """Save a video to database."""
    if USE_SUPABASE:
        existing = supabase.table('videos').select('id').eq('id', video_data['id']).execute()
        if existing.data:
            supabase.table('videos').update(video_data).eq('id', video_data['id']).execute()
        else:
            supabase.table('videos').insert(video_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO videos (id, title, description, url, thumbnail, category, subcategory, tags, duration, created_at, views, video_type, local_file, event, team, round_num, jump_num)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (video_data['id'], video_data['title'], video_data.get('description', ''),
              video_data.get('url', ''), video_data.get('thumbnail'), video_data['category'],
              video_data.get('subcategory', ''), video_data.get('tags', ''),
              video_data.get('duration', ''), video_data['created_at'],
              video_data.get('views', 0), video_data.get('video_type', 'url'),
              video_data.get('local_file', ''), video_data.get('event', ''),
              video_data.get('team', ''), video_data.get('round_num', ''),
              video_data.get('jump_num', '')))
        db.commit()


def delete_video_db(video_id):
    """Delete a video from database."""
    if USE_SUPABASE:
        supabase.table('videos').delete().eq('id', video_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM videos WHERE id = ?', (video_id,))
        db.commit()


def increment_views(video_id):
    """Increment view count for a video."""
    if USE_SUPABASE:
        video = get_video(video_id)
        if video:
            supabase.table('videos').update({'views': video['views'] + 1}).eq('id', video_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('UPDATE videos SET views = views + 1 WHERE id = ?', (video_id,))
        db.commit()


def get_video_count_by_category(category):
    """Get video count for a category."""
    if USE_SUPABASE:
        result = supabase.table('videos').select('id', count='exact').eq('category', category).execute()
        return result.count or 0
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT COUNT(*) FROM videos WHERE category = ?', (category,))
        return cursor.fetchone()[0]


def search_videos(query):
    """Search videos by title, description, or tags."""
    if USE_SUPABASE:
        result = supabase.table('videos').select('*').or_(
            f"title.ilike.%{query}%,description.ilike.%{query}%,tags.ilike.%{query}%"
        ).order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('''
            SELECT * FROM videos
            WHERE title LIKE ? OR description LIKE ? OR tags LIKE ?
            ORDER BY created_at DESC
        ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
        return [dict(row) for row in cursor.fetchall()]


def get_all_events():
    """Get all unique events."""
    try:
        if USE_SUPABASE:
            result = supabase.table('videos').select('event').execute()
            events = set(v['event'] for v in result.data if v.get('event'))
            return sorted(events)
        else:
            db = get_sqlite_db()
            cursor = db.execute('SELECT DISTINCT event FROM videos WHERE event IS NOT NULL AND event != "" ORDER BY event')
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error getting events: {e}")
        return []


def get_videos_by_event(event_name):
    """Get videos by event name."""
    if USE_SUPABASE:
        result = supabase.table('videos').select('*').eq('event', event_name).order('title').execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM videos WHERE event = ? ORDER BY title', (event_name,))
        return [dict(row) for row in cursor.fetchall()]


def get_user(username):
    """Get user from database."""
    if USE_SUPABASE:
        result = supabase.table('users').select('*').eq('username', username).execute()
        return result.data[0] if result.data else None
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM users WHERE username = ?', (username,))
        row = cursor.fetchone()
        return dict(row) if row else None


# Initialize database
def safe_init_db():
    try:
        init_db()
        print(f"Database initialized ({'Supabase' if USE_SUPABASE else 'SQLite'})")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")

safe_init_db()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session.get('role') != 'admin':
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def is_direct_video_url(url):
    """Check if URL is a direct video file."""
    if not url:
        return False
    url_lower = url.lower()
    # Dropbox direct links are always streamable
    if 'dropboxusercontent.com' in url_lower or 'dropbox.com' in url_lower:
        return True
    video_extensions = ('.mp4', '.webm', '.ogg', '.mov')
    return any(url_lower.endswith(ext) or f'{ext}?' in url_lower or f'{ext}&' in url_lower for ext in video_extensions)


def get_video_embed_url(url):
    """Convert video URL to embeddable format."""
    if not url:
        return url
    if 'youtube.com/watch' in url:
        video_id = url.split('v=')[1].split('&')[0]
        return f'https://www.youtube.com/embed/{video_id}'
    elif 'youtu.be/' in url:
        video_id = url.split('youtu.be/')[1].split('?')[0]
        return f'https://www.youtube.com/embed/{video_id}'
    elif 'vimeo.com/' in url:
        video_id = url.split('vimeo.com/')[1].split('?')[0]
        return f'https://player.vimeo.com/video/{video_id}'
    return url


def get_video_thumbnail(url):
    """Get thumbnail URL from video URL."""
    if not url:
        return None
    if 'youtube.com/watch' in url:
        video_id = url.split('v=')[1].split('&')[0]
        return f'https://img.youtube.com/vi/{video_id}/mqdefault.jpg'
    elif 'youtu.be/' in url:
        video_id = url.split('youtu.be/')[1].split('?')[0]
        return f'https://img.youtube.com/vi/{video_id}/mqdefault.jpg'
    return None


def get_video_duration(file_path):
    """Get video duration using ffprobe."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
            capture_output=True, text=True
        )
        seconds = float(result.stdout.strip())
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}:{secs:02d}"
    except:
        return None


import re

def parse_filename_metadata(filename, folder_path=''):
    """Extract metadata from filename and folder path."""
    # Remove extension and clean up
    name = os.path.splitext(filename)[0]
    name_lower = name.lower()
    folder_lower = folder_path.lower()
    combined = f"{folder_lower} {name_lower}"

    metadata = {
        'category': '',
        'subcategory': '',
        'event': '',
        'team': '',
        'round': '',
        'jump': '',
        'title': ''
    }

    # Category detection
    category_patterns = {
        'cp': [r'\bcp\b', r'canopy.?piloting'],
        'fs': [r'\bfs\b', r'formation.?skydiving'],
        'cf': [r'\bcf\b', r'canopy.?formation', r'\bcrw\b'],
        'ae': [r'\bae\b', r'artistic', r'\bfreestyle\b', r'\bfreefly\b'],
        'ws': [r'\bws\b', r'wingsuit'],
        'al': [r'\bal\b', r'accuracy.?landing', r'\baccuracy\b']
    }

    for cat_id, patterns in category_patterns.items():
        for pattern in patterns:
            if re.search(pattern, combined):
                metadata['category'] = cat_id
                break
        if metadata['category']:
            break

    # Subcategory detection
    subcategory_patterns = {
        'cp': {
            'freestyle': [r'freestyle', r'free.?style'],
            'speed': [r'\bspeed\b'],
            'distance': [r'\bdistance\b'],
            'zone_accuracy': [r'zone', r'zone.?accuracy']
        },
        'fs': {
            '4way_fs': [r'\b4.?way\b(?!.*vfs)', r'4way.?fs'],
            '4way_vfs': [r'vfs', r'vertical', r'4.?way.?vfs'],
            '2way_mfs': [r'2.?way', r'mfs'],
            '8way': [r'\b8.?way\b'],
            '10way': [r'\b10.?way\b'],
            '16way': [r'\b16.?way\b']
        },
        'cf': {
            '4way': [r'\b4.?way\b'],
            '2way': [r'\b2.?way\b']
        },
        'ae': {
            'freestyle': [r'freestyle(?!.*fly)'],
            'freefly': [r'freefly', r'free.?fly']
        }
    }

    if metadata['category'] in subcategory_patterns:
        for sub_id, patterns in subcategory_patterns[metadata['category']].items():
            for pattern in patterns:
                if re.search(pattern, combined):
                    metadata['subcategory'] = sub_id
                    break
            if metadata['subcategory']:
                break

    # Event detection from folder path
    folder_parts = folder_path.split(os.sep)
    for part in folder_parts:
        part_lower = part.lower()
        # Look for year + event keywords
        if re.search(r'20\d{2}', part) or any(kw in part_lower for kw in ['nationals', 'championship', 'world', 'uspa', 'competition']):
            if len(part) > 5:
                metadata['event'] = part.replace('_', ' ').replace('-', ' ').strip()
                break

    # Team/Competitor detection - look for team names or proper nouns
    # Common patterns: "Team_Name", "TeamName", names after "team"
    team_match = re.search(r'team[_\s-]?([a-zA-Z0-9]+)', combined, re.IGNORECASE)
    if team_match:
        metadata['team'] = team_match.group(1).title()
    else:
        # Look for capitalized words that might be team names
        words = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', name)
        # Filter out common non-team words
        skip_words = ['Round', 'Jump', 'Team', 'Final', 'Semi', 'Freestyle', 'Speed', 'Distance']
        teams = [w for w in words if w not in skip_words and len(w) > 2]
        if teams:
            metadata['team'] = teams[0]

    # Round detection
    round_match = re.search(r'(?:round|rd|r)[_\s-]?(\d+)', combined, re.IGNORECASE)
    if round_match:
        metadata['round'] = round_match.group(1)

    # Jump number detection
    jump_match = re.search(r'(?:jump|j)[_\s-]?(\d+)', combined, re.IGNORECASE)
    if jump_match:
        metadata['jump'] = jump_match.group(1)

    # Build a nice title
    title_parts = []
    if metadata['team']:
        title_parts.append(metadata['team'])
    if metadata['round']:
        title_parts.append(f"Round {metadata['round']}")
    if metadata['jump']:
        title_parts.append(f"Jump {metadata['jump']}")

    if title_parts:
        metadata['title'] = ' - '.join(title_parts)
    else:
        # Fall back to cleaned filename - keep original for numeric files
        metadata['title'] = name.replace('_', ' ').replace('-', ' ').strip()

    return metadata


def generate_thumbnail(video_path, thumbnail_path):
    """Generate thumbnail from video using ffmpeg."""
    try:
        subprocess.run([
            'ffmpeg', '-y', '-i', video_path,
            '-ss', '00:00:02', '-vframes', '1',
            '-vf', 'scale=320:-1',
            thumbnail_path
        ], capture_output=True, check=True)
        return True
    except:
        return False


def convert_video_to_mp4(input_path, output_path):
    """Convert video to MP4 using ffmpeg."""
    try:
        subprocess.run([
            'ffmpeg', '-y', '-i', input_path,
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            output_path
        ], capture_output=True, check=True)
        return True
    except Exception as e:
        print(f"Conversion error: {e}")
        return False


@app.route('/')
def index():
    """Home page showing all categories."""
    category_counts = {}
    for cat_id in CATEGORIES:
        category_counts[cat_id] = get_video_count_by_category(cat_id)

    all_videos = get_all_videos()
    recent_videos = all_videos[:8] if all_videos else []

    return render_template('index.html',
                         categories=CATEGORIES,
                         category_counts=category_counts,
                         recent_videos=recent_videos,
                         is_admin=session.get('role') == 'admin')


@app.route('/category/<cat_id>')
def category(cat_id):
    """Show videos in a category."""
    if cat_id not in CATEGORIES:
        return "Category not found", 404

    cat = CATEGORIES[cat_id]
    subcategory = request.args.get('sub')

    videos = get_videos_by_category(cat_id, subcategory)

    return render_template('category.html',
                         category=cat,
                         cat_id=cat_id,
                         videos=videos,
                         current_sub=subcategory,
                         is_admin=session.get('role') == 'admin')


@app.route('/video/<video_id>')
def video(video_id):
    """Show single video page."""
    video = get_video(video_id)

    if not video:
        return "Video not found", 404

    # Determine video source
    if video.get('video_type') == 'local' and video.get('local_file'):
        video['video_src'] = f'/static/videos/{video["local_file"]}'
        video['is_local'] = True
        video['is_direct_url'] = False
    elif is_direct_video_url(video.get('url', '')):
        video['video_src'] = video['url']
        video['is_local'] = False
        video['is_direct_url'] = True
    else:
        video['embed_url'] = get_video_embed_url(video.get('url', ''))
        video['is_local'] = False
        video['is_direct_url'] = False

    # Increment view count
    increment_views(video_id)

    # Get related videos from same category
    related = get_videos_by_category(video['category'])
    related_videos = [v for v in related if v['id'] != video_id][:6]

    cat = CATEGORIES.get(video['category'], {})

    return render_template('video.html',
                         video=video,
                         category=cat,
                         related_videos=related_videos,
                         is_admin=session.get('role') == 'admin')


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page."""
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').lower()
        password = request.form.get('password', '')

        user = get_user(username)

        if user and user['password'] == password:
            session['user'] = username
            session['role'] = user['role']
            session['name'] = user['name']
            return redirect(url_for('admin_dashboard'))
        else:
            error = 'Invalid username or password'

    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    """Logout."""
    session.clear()
    return redirect(url_for('index'))


@app.route('/admin')
@admin_required
def admin_dashboard():
    """Admin dashboard."""
    try:
        videos = get_all_videos()
        total_videos = len(videos)
        total_views = sum(v.get('views', 0) for v in videos)
        events = get_all_events()
    except Exception as e:
        print(f"Admin dashboard error: {e}")
        videos = []
        total_videos = 0
        total_views = 0
        events = []

    return render_template('admin.html',
                         videos=videos,
                         categories=CATEGORIES,
                         total_videos=total_videos,
                         total_views=total_views,
                         events=events,
                         dropbox_app_key=DROPBOX_APP_KEY)


def download_and_convert_video(url, video_id):
    """Download video from URL and convert to MP4 if needed."""
    import urllib.request
    import tempfile

    # Check if it's a video format that needs conversion
    url_lower = url.lower()
    needs_conversion = any(ext in url_lower for ext in ['.mts', '.m2ts', '.avi', '.mkv', '.mov'])

    if not needs_conversion:
        return None, None, None  # Use URL directly

    try:
        # Download the file
        temp_dir = tempfile.gettempdir()
        ext = '.mts' if '.mts' in url_lower else '.m2ts' if '.m2ts' in url_lower else '.avi' if '.avi' in url_lower else '.mkv' if '.mkv' in url_lower else '.mov'
        temp_input = os.path.join(temp_dir, f"{video_id}_input{ext}")

        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, temp_input)

        # Convert to MP4
        output_filename = f"{video_id}.mp4"
        output_path = os.path.join(VIDEOS_FOLDER, output_filename)

        print(f"Converting to MP4...")
        if convert_video_to_mp4(temp_input, output_path):
            # Generate thumbnail
            thumbnail_filename = f"{video_id}_thumb.jpg"
            thumbnail_path = os.path.join(VIDEOS_FOLDER, thumbnail_filename)
            if generate_thumbnail(output_path, thumbnail_path):
                thumbnail = f"/static/videos/{thumbnail_filename}"
            else:
                thumbnail = None

            # Get duration
            duration = get_video_duration(output_path)

            # Clean up temp file
            os.remove(temp_input)

            return output_filename, thumbnail, duration
        else:
            os.remove(temp_input)
            return None, None, None
    except Exception as e:
        print(f"Error downloading/converting: {e}")
        return None, None, None


@app.route('/admin/add-video', methods=['POST'])
@admin_required
def add_video():
    """Add a new video."""
    data = request.json

    title = data.get('title', '').strip()
    description = data.get('description', '').strip()
    url = data.get('url', '').strip()
    category = data.get('category', '')
    subcategory = data.get('subcategory', '')
    tags = data.get('tags', '').strip()
    duration = data.get('duration', '').strip()
    event = data.get('event', '').strip()

    if not title or not url or not category:
        return jsonify({'error': 'Title, URL, and category are required'}), 400

    if category not in CATEGORIES:
        return jsonify({'error': 'Invalid category'}), 400

    video_id = str(uuid.uuid4())[:8]

    # Check if video needs conversion (MTS, etc.)
    url_lower = url.lower()
    needs_conversion = any(ext in url_lower for ext in ['.mts', '.m2ts', '.avi', '.mkv'])

    if needs_conversion and not USE_SUPABASE:
        # Local mode - download and convert
        local_file, thumbnail, vid_duration = download_and_convert_video(url, video_id)
        if local_file:
            save_video({
                'id': video_id,
                'title': title,
                'description': description,
                'url': '',
                'thumbnail': thumbnail,
                'category': category,
                'subcategory': subcategory,
                'tags': tags,
                'duration': vid_duration or duration,
                'created_at': datetime.now().isoformat(),
                'views': 0,
                'video_type': 'local',
                'local_file': local_file,
                'event': event
            })
            return jsonify({'success': True, 'message': 'Video converted and added successfully', 'id': video_id})
        else:
            return jsonify({'error': 'Failed to convert video. Make sure ffmpeg is installed.'}), 400
    elif needs_conversion and USE_SUPABASE:
        return jsonify({'error': 'MTS/AVI/MKV files need to be converted to MP4 first. Convert locally or upload MP4 files to Dropbox.'}), 400

    # Regular URL video (MP4, YouTube, etc.)
    thumbnail = get_video_thumbnail(url)

    save_video({
        'id': video_id,
        'title': title,
        'description': description,
        'url': url,
        'thumbnail': thumbnail,
        'category': category,
        'subcategory': subcategory,
        'tags': tags,
        'duration': duration,
        'created_at': datetime.now().isoformat(),
        'views': 0,
        'video_type': 'url',
        'local_file': '',
        'event': event
    })

    return jsonify({'success': True, 'message': 'Video added successfully', 'id': video_id})


@app.route('/admin/import-folder', methods=['POST'])
@admin_required
def import_folder():
    """Import videos from a local folder (local development only)."""
    if USE_SUPABASE:
        return jsonify({'error': 'Local folder import not available in production. Use YouTube/Vimeo URLs instead.'}), 400

    data = request.json
    folder_path = data.get('folder_path', '').strip()
    category = data.get('category', '')
    subcategory = data.get('subcategory', '')
    event = data.get('event', '').strip()
    convert = data.get('convert', True)

    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({'error': 'Invalid folder path'}), 400

    # User-provided values (can be empty to allow auto-detection per file)
    user_category = category
    user_subcategory = subcategory
    user_event = event

    # If no category provided, try to detect from folder path
    if not user_category:
        folder_meta = parse_filename_metadata('', folder_path)
        if folder_meta['category']:
            user_category = folder_meta['category']

    if user_category and user_category not in CATEGORIES:
        return jsonify({'error': 'Invalid category'}), 400

    video_extensions = ('.mp4', '.mts', '.m2ts', '.mov', '.avi', '.mkv', '.webm')
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(video_extensions)]

    if not files:
        return jsonify({'error': 'No video files found in folder'}), 400

    imported = 0
    errors = []

    for filename in sorted(files):
        try:
            input_path = os.path.join(folder_path, filename)
            video_id = str(uuid.uuid4())[:8]

            # Parse metadata from filename
            file_meta = parse_filename_metadata(filename, folder_path)

            # Use user-provided values if set, otherwise use auto-detected
            final_category = user_category or file_meta['category'] or 'cp'  # default to cp
            final_subcategory = user_subcategory or file_meta['subcategory']
            final_event = user_event or file_meta['event']
            final_title = file_meta['title']

            # Build tags from detected metadata
            tags_list = []
            if file_meta['team']:
                tags_list.append(file_meta['team'])
            if file_meta['round']:
                tags_list.append(f"Round {file_meta['round']}")
            if file_meta['jump']:
                tags_list.append(f"Jump {file_meta['jump']}")
            tags = ', '.join(tags_list)

            needs_conversion = not filename.lower().endswith(('.mp4', '.webm'))

            if needs_conversion and convert:
                output_filename = f"{video_id}.mp4"
                output_path = os.path.join(VIDEOS_FOLDER, output_filename)

                if convert_video_to_mp4(input_path, output_path):
                    local_file = output_filename
                else:
                    errors.append(f"Failed to convert {filename}")
                    continue
            else:
                if filename.lower().endswith(('.mp4', '.webm')):
                    output_filename = f"{video_id}{os.path.splitext(filename)[1]}"
                    output_path = os.path.join(VIDEOS_FOLDER, output_filename)
                    shutil.copy2(input_path, output_path)
                    local_file = output_filename
                else:
                    errors.append(f"Cannot use {filename} without conversion")
                    continue

            thumbnail_filename = f"{video_id}_thumb.jpg"
            thumbnail_path = os.path.join(VIDEOS_FOLDER, thumbnail_filename)
            if generate_thumbnail(os.path.join(VIDEOS_FOLDER, local_file), thumbnail_path):
                thumbnail = f"/static/videos/{thumbnail_filename}"
            else:
                thumbnail = None

            duration = get_video_duration(os.path.join(VIDEOS_FOLDER, local_file))

            save_video({
                'id': video_id,
                'title': final_title,
                'description': '',
                'url': '',
                'thumbnail': thumbnail,
                'category': final_category,
                'subcategory': final_subcategory,
                'tags': tags,
                'duration': duration,
                'created_at': datetime.now().isoformat(),
                'views': 0,
                'video_type': 'local',
                'local_file': local_file,
                'event': final_event,
                'team': file_meta.get('team', ''),
                'round_num': file_meta.get('round', ''),
                'jump_num': file_meta.get('jump', '')
            })

            imported += 1

        except Exception as e:
            errors.append(f"Error with {filename}: {str(e)}")

    message = f"Imported {imported} video(s)"
    if errors:
        message += f". Errors: {len(errors)}"

    return jsonify({
        'success': True,
        'message': message,
        'imported': imported,
        'errors': errors
    })


@app.route('/admin/delete-video/<video_id>', methods=['POST'])
@admin_required
def delete_video(video_id):
    """Delete a video."""
    video = get_video(video_id)

    if video:
        if video.get('local_file'):
            local_path = os.path.join(VIDEOS_FOLDER, video['local_file'])
            if os.path.exists(local_path):
                os.remove(local_path)
        if video.get('thumbnail') and video['thumbnail'].startswith('/static/videos/'):
            thumb_path = os.path.join(VIDEOS_FOLDER, os.path.basename(video['thumbnail']))
            if os.path.exists(thumb_path):
                os.remove(thumb_path)

    delete_video_db(video_id)

    return jsonify({'success': True, 'message': 'Video deleted'})


@app.route('/admin/get-video/<video_id>', methods=['GET'])
@admin_required
def get_video_details(video_id):
    """Get video details for editing."""
    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404
    return jsonify(video)


def convert_dropbox_url_for_streaming(url):
    """Convert Dropbox URL to direct streaming format."""
    if not url:
        return url
    # Convert www.dropbox.com to dl.dropboxusercontent.com for streaming
    if 'www.dropbox.com' in url:
        url = url.replace('www.dropbox.com', 'dl.dropboxusercontent.com')
    elif 'dropbox.com' in url and 'dl.dropboxusercontent.com' not in url:
        url = url.replace('dropbox.com', 'dl.dropboxusercontent.com')
    # Remove query parameters that force download
    url = url.replace('?dl=0', '').replace('?dl=1', '').replace('?raw=1', '')
    url = url.replace('&dl=0', '').replace('&dl=1', '').replace('&raw=1', '')
    return url


@app.route('/admin/fix-dropbox-urls', methods=['POST'])
@admin_required
def fix_dropbox_urls():
    """Fix Dropbox URLs for proper streaming playback."""
    fixed = 0
    try:
        if USE_SUPABASE:
            # Get all videos with Dropbox URLs
            result = supabase.table('videos').select('id, url').execute()
            for video in result.data:
                if video.get('url') and 'dropbox.com' in video['url']:
                    new_url = convert_dropbox_url_for_streaming(video['url'])
                    if new_url != video['url']:
                        supabase.table('videos').update({'url': new_url}).eq('id', video['id']).execute()
                        fixed += 1
        else:
            db = get_sqlite_db()
            cursor = db.execute("SELECT id, url FROM videos WHERE url LIKE '%dropbox.com%'")
            videos = cursor.fetchall()
            for video in videos:
                new_url = convert_dropbox_url_for_streaming(video['url'])
                if new_url != video['url']:
                    db.execute("UPDATE videos SET url = ? WHERE id = ?", (new_url, video['id']))
                    fixed += 1
            db.commit()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({'success': True, 'message': f'Fixed {fixed} Dropbox video URLs'})


@app.route('/admin/edit-video/<video_id>', methods=['POST'])
@admin_required
def edit_video(video_id):
    """Edit a video."""
    data = request.json

    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    video['title'] = data.get('title', video['title']).strip()
    video['description'] = data.get('description', video.get('description', '')).strip()
    video['category'] = data.get('category', video['category'])
    video['subcategory'] = data.get('subcategory', video.get('subcategory', ''))
    video['tags'] = data.get('tags', video.get('tags', '')).strip()
    video['duration'] = data.get('duration', video.get('duration', '')).strip()
    video['event'] = data.get('event', video.get('event', '')).strip()

    save_video(video)

    return jsonify({'success': True, 'message': 'Video updated'})


@app.route('/search')
def search():
    """Search videos."""
    query = request.args.get('q', '').strip()

    if not query:
        return redirect(url_for('index'))

    videos = search_videos(query)

    return render_template('search.html',
                         query=query,
                         videos=videos,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin')


@app.route('/event/<event_name>')
def event_page(event_name):
    """Show all videos in an event."""
    videos = get_videos_by_event(event_name)

    # Group videos by category
    videos_by_category = {}
    for video in videos:
        cat = video.get('category', 'other')
        if cat not in videos_by_category:
            videos_by_category[cat] = []
        videos_by_category[cat].append(video)

    return render_template('event.html',
                         event_name=event_name,
                         videos=videos,
                         videos_by_category=videos_by_category,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin')


@app.route('/events')
def events_list():
    """Show all events."""
    events = get_all_events()

    # Get video count for each event
    event_data = []
    for event_name in events:
        videos = get_videos_by_event(event_name)
        event_data.append({
            'name': event_name,
            'video_count': len(videos)
        })

    return render_template('events.html',
                         events=event_data,
                         is_admin=session.get('role') == 'admin')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_ENV', 'development') == 'development'
    print("\n=== USPA Video Library ===")
    print(f"Database: {'Supabase' if USE_SUPABASE else 'SQLite'}")
    print(f"Open http://localhost:{port} in your browser")
    print("\nAdmin login: admin / admin123\n")
    app.run(debug=debug, host='0.0.0.0', port=port)
