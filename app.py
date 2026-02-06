#!/usr/bin/env python3
"""Video Library - Video database for skydiving disciplines."""

# Load environment variables from .env file (for local development)
from dotenv import load_dotenv
load_dotenv()

import os
import re
import time
import uuid
import json
import subprocess
import shutil
import smtplib
import secrets
import threading
import urllib.parse
import urllib.request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_from_directory, g
from werkzeug.utils import secure_filename
from functools import wraps
import sqlite3
from io import BytesIO

# Background conversion job tracking
conversion_jobs = {}
conversion_lock = threading.Lock()
MAX_CONCURRENT_CONVERSIONS = 1  # Limit to prevent server overload

# PDF generation
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, Flowable
    from reportlab.pdfgen import canvas
    from reportlab.graphics.shapes import Drawing, String, Line, Rect
    from reportlab.graphics import renderPDF
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False

# Database support - Supabase required for all environments
try:
    from supabase import create_client, Client
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
    if SUPABASE_URL and SUPABASE_KEY:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        USE_SUPABASE = True
        print(f"[STARTUP] Supabase connected: URL={SUPABASE_URL[:30]}...")
    else:
        USE_SUPABASE = False
        supabase = None
        print(f"[STARTUP] WARNING: Supabase NOT configured!")
        print(f"[STARTUP] Create a .env file with SUPABASE_URL and SUPABASE_KEY")
        print(f"[STARTUP] Falling back to SQLite (data will NOT sync to production)")
except ImportError as e:
    USE_SUPABASE = False
    supabase = None
    print(f"[STARTUP] Supabase import failed: {e}")
    print(f"[STARTUP] Install with: pip install supabase")
    print(f"[STARTUP] Falling back to SQLite (data will NOT sync to production)")

# Supabase Storage bucket name
SUPABASE_BUCKET = 'videos'

# AWS S3 Configuration
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    AWS_S3_BUCKET = os.environ.get('AWS_S3_BUCKET')
    AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
    AWS_CLOUDFRONT_DOMAIN = os.environ.get('AWS_CLOUDFRONT_DOMAIN', '')  # Optional CDN

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_S3_BUCKET:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        USE_S3 = True
        print(f"[STARTUP] AWS S3 configured: Bucket={AWS_S3_BUCKET}, Region={AWS_REGION}")
    else:
        s3_client = None
        USE_S3 = False
        print(f"[STARTUP] AWS S3 not configured (optional)")
except ImportError:
    s3_client = None
    USE_S3 = False
    print(f"[STARTUP] boto3 not installed, S3 disabled")


def upload_to_s3(file_data, filename, content_type='video/mp4', folder='videos'):
    """Upload a file to AWS S3 and return the public URL."""
    if not USE_S3 or not s3_client:
        return None

    try:
        # Create S3 key (path)
        s3_key = f"{folder}/{filename}" if folder else filename

        # Upload to S3
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=s3_key,
            Body=file_data,
            ContentType=content_type
        )

        # Return URL (CloudFront if configured, otherwise direct S3)
        if AWS_CLOUDFRONT_DOMAIN:
            url = f"https://{AWS_CLOUDFRONT_DOMAIN}/{s3_key}"
        else:
            url = f"https://{AWS_S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"

        return url
    except Exception as e:
        print(f"S3 upload error: {e}")
        return None


def delete_from_s3(s3_key):
    """Delete a file from AWS S3."""
    if not USE_S3 or not s3_client:
        return False

    try:
        s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
        return True
    except Exception as e:
        print(f"S3 delete error: {e}")
        return False


def get_s3_presigned_url(s3_key, expires_in=3600):
    """Generate a presigned URL for private S3 objects."""
    if not USE_S3 or not s3_client:
        return None

    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': AWS_S3_BUCKET, 'Key': s3_key},
            ExpiresIn=expires_in
        )
        return url
    except Exception as e:
        print(f"S3 presigned URL error: {e}")
        return None

def upload_to_supabase_storage(file_path, storage_path):
    """Upload a file to Supabase Storage."""
    if not USE_SUPABASE or not supabase:
        return None
    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()

        # Determine content type
        ext = os.path.splitext(storage_path)[1].lower()
        content_types = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.mov': 'video/quicktime',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.csv': 'text/csv'
        }
        content_type = content_types.get(ext, 'application/octet-stream')

        # Upload to Supabase Storage
        result = supabase.storage.from_(SUPABASE_BUCKET).upload(
            storage_path,
            file_data,
            file_options={"content-type": content_type, "upsert": "true"}
        )

        # Get public URL
        public_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(storage_path)
        return public_url
    except Exception as e:
        print(f"Supabase Storage upload error: {e}")
        return None

def delete_from_supabase_storage(storage_path):
    """Delete a file from Supabase Storage."""
    if not USE_SUPABASE or not supabase:
        return False
    try:
        supabase.storage.from_(SUPABASE_BUCKET).remove([storage_path])
        return True
    except Exception as e:
        print(f"Supabase Storage delete error: {e}")
        return False

def get_supabase_storage_url(storage_path):
    """Get public URL for a file in Supabase Storage."""
    if not USE_SUPABASE or not supabase:
        return None
    try:
        return supabase.storage.from_(SUPABASE_BUCKET).get_public_url(storage_path)
    except Exception as e:
        print(f"Supabase Storage URL error: {e}")
        return None

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'uspa-video-library-secret-key')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
ADMIN_PIN = os.environ.get('ADMIN_PIN', '1234')  # Default PIN for dangerous operations
CHIEF_JUDGE_PIN = os.environ.get('CHIEF_JUDGE_PIN', '9999')  # PIN for Chief Judge to approve scores

# Custom Jinja2 filter for split
@app.template_filter('split')
def split_filter(value, separator=','):
    """Split a string by separator."""
    if not value:
        return []
    return [x.strip() for x in str(value).split(separator) if x.strip()]

# Event type display names mapping
EVENT_DISPLAY_NAMES = {
    'fs_4way_fs': '4-Way FS',
    'fs_4way_vfs': '4-Way VFS',
    'fs_2way_mfs': '2-Way MFS',
    'fs_8way': '8-Way FS',
    'fs_16way': '16-Way FS',
    'fs_10way': '10-Way FS',
    'cf_4way_rot': '4-Way Rotation',
    'cf_4way_seq': '4-Way Sequential',
    'cf_2way_open': '2-Way Sequential Open',
    'cf_2way_proam': '2-Way Sequential Pro/Am',
    'cf_2way': '2-Way CF',
    'al_individual': 'AL Individual',
    'al_team': 'AL Team',
    'cp_dsz': 'Canopy Piloting',
    'cp_team': 'CP Team',
    'cp_freestyle': 'CP Freestyle',
    'ae_freestyle': 'AE Freestyle',
    'ae_freefly': 'AE Freefly',
    'ws_acrobatic': 'WS Acrobatic',
    'ws_performance': 'WS Performance',
    'sp_individual': 'SP Individual',
    'sp_mixed_team': 'SP Mixed Team',
    'indoor_4way_fs': 'Indoor 4-Way FS',
    'indoor_4way_vfs': 'Indoor 4-Way VFS',
    'indoor_2way_fs': 'Indoor 2-Way FS',
    'indoor_2way_vfs': 'Indoor 2-Way VFS',
    'indoor_8way': 'Indoor 8-Way',
    'indoor_freestyle': 'Indoor Freestyle',
    'indoor_freefly': 'Indoor Freefly',
}

@app.template_filter('event_name')
def event_name_filter(event_type):
    """Convert event type code to display name."""
    return EVENT_DISPLAY_NAMES.get(event_type, event_type.upper().replace('_', ' '))


def normalize_event_type(input_str):
    """Normalize event type string for flexible CSV matching.

    Matches inputs like '4 way fs', '4wayFS', '4-Way FS' to 'fs_4way_fs'.
    """
    if not input_str:
        return ''

    # Normalize: lowercase, remove spaces/hyphens/underscores
    normalized = input_str.lower().replace(' ', '').replace('-', '').replace('_', '')

    # Build lookup from EVENT_DISPLAY_NAMES (both keys and values)
    for event_code, display_name in EVENT_DISPLAY_NAMES.items():
        # Normalize the event code
        code_normalized = event_code.replace('_', '')
        # Normalize the display name
        display_normalized = display_name.lower().replace(' ', '').replace('-', '')

        if normalized == code_normalized or normalized == display_normalized:
            return event_code

    # Also check common variations
    event_aliases = {
        '4wayfs': 'fs_4way_fs',
        '4wayvfs': 'fs_4way_vfs',
        '2waymfs': 'fs_2way_mfs',
        '8wayfs': 'fs_8way',
        '8way': 'fs_8way',
        '16wayfs': 'fs_16way',
        '16way': 'fs_16way',
        '10wayfs': 'fs_10way',
        '10way': 'fs_10way',
        '4wayrotation': 'cf_4way_rot',
        '4wayrot': 'cf_4way_rot',
        'cf4wayrot': 'cf_4way_rot',
        '4waysequential': 'cf_4way_seq',
        '4wayseq': 'cf_4way_seq',
        'cf4wayseq': 'cf_4way_seq',
        '2waysequentialopen': 'cf_2way_open',
        '2wayopen': 'cf_2way_open',
        'cf2wayopen': 'cf_2way_open',
        '2waysequentialproam': 'cf_2way_proam',
        '2wayproam': 'cf_2way_proam',
        'cf2wayproam': 'cf_2way_proam',
        '2waycf': 'cf_2way',
        'cf2way': 'cf_2way',
        'alindividual': 'al_individual',
        'alind': 'al_individual',
        'alteam': 'al_team',
        'cpindividual': 'cp_dsz',
        'cpind': 'cp_dsz',
        'cpdsz': 'cp_dsz',
        'cpteam': 'cp_team',
        'cpfreestyle': 'cp_freestyle',
        'aefreestyle': 'ae_freestyle',
        'freestyle': 'ae_freestyle',
        'aefreefly': 'ae_freefly',
        'freefly': 'ae_freefly',
        'wsacrobatic': 'ws_acrobatic',
        'wsperformance': 'ws_performance',
        'spindividual': 'sp_individual',
        'spind': 'sp_individual',
        'spmixedteam': 'sp_mixed_team',
        'spmixed': 'sp_mixed_team',
    }

    if normalized in event_aliases:
        return event_aliases[normalized]

    # If no match found, return original stripped
    return input_str.strip()

# SocketIO for real-time sync viewing
try:
    from flask_socketio import SocketIO, emit, join_room, leave_room
    # Configure SocketIO to work in both development and production
    # async_mode='threading' works without additional dependencies
    # cors_allowed_origins="*" allows connections from any origin
    socketio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode='threading',
        ping_timeout=60,
        ping_interval=25,
        logger=False,
        engineio_logger=False
    )
    SOCKETIO_ENABLED = True
except ImportError:
    SOCKETIO_ENABLED = False
    socketio = None

# Sync rooms for synchronized video viewing
# Structure: {room_id: {'video_id': str, 'event_judge': str, 'judges': {username: {'ready': bool, 'start_time': float}}, 'state': 'waiting'|'playing'|'syncing'}}
sync_rooms = {}

# Email configuration for password reset
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
SMTP_USERNAME = os.environ.get('SMTP_USERNAME', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SMTP_FROM_EMAIL = os.environ.get('SMTP_FROM_EMAIL', '')
APP_URL = os.environ.get('APP_URL', 'http://localhost:5001')

# Password reset tokens (in-memory for simplicity, resets on server restart)
password_reset_tokens = {}  # {token: {'username': str, 'expires': datetime}}

def send_reset_email(email, username, reset_token):
    """Send password reset email."""
    if not SMTP_USERNAME or not SMTP_PASSWORD:
        print(f"Email not configured. Reset link: {APP_URL}/reset-password/{reset_token}")
        return False

    reset_link = f"{APP_URL}/reset-password/{reset_token}"

    msg = MIMEMultipart()
    msg['From'] = SMTP_FROM_EMAIL or SMTP_USERNAME
    msg['To'] = email
    msg['Subject'] = 'Video Library - Password Reset'

    body = f"""
Hello,

You requested a password reset for your Video Library account ({username}).

Click the link below to reset your password:
{reset_link}

This link will expire in 1 hour.

If you did not request this reset, please ignore this email.

- Video Library
"""
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_FROM_EMAIL or SMTP_USERNAME, email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

def send_welcome_email(email, username, password, name):
    """Send welcome email with login credentials to new user."""
    if not SMTP_USERNAME or not SMTP_PASSWORD:
        print(f"Email not configured. New user: {username}, password: {password}")
        return False

    if not email:
        print(f"No email provided for user {username}")
        return False

    msg = MIMEMultipart()
    msg['From'] = SMTP_FROM_EMAIL or SMTP_USERNAME
    msg['To'] = email
    msg['Subject'] = 'Welcome to Video Library - Your Account Details'

    body = f"""
Hello {name},

Your Video Library account has been created.

Here are your login credentials:

Username: {username}
Password: {password}

Login at: {APP_URL}

You will be asked to change your password when you first log in.

If you have any questions, please contact your administrator.

Best regards,
Video Library Team
"""

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_FROM_EMAIL or SMTP_USERNAME, email, msg.as_string())
        server.quit()
        print(f"Welcome email sent to {email}")
        return True
    except Exception as e:
        print(f"Failed to send welcome email: {e}")
        return False

# Global error handler to show actual errors
def _is_api_request_check():
    """Check if the current request is an API/AJAX request expecting JSON."""
    try:
        content_type = request.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            return True
        accept = request.headers.get('Accept', '')
        if 'application/json' in accept:
            return True
        if request.is_json:
            return True
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return True
    except:
        pass
    return False

@app.errorhandler(500)
def handle_500_error(e):
    import traceback
    error_msg = f"500 Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
    print(error_msg)
    # Return JSON for API requests
    try:
        if _is_api_request_check():
            return jsonify({'success': False, 'error': str(e)}), 500
    except:
        pass
    return f"<pre>{error_msg}</pre>", 500

@app.errorhandler(Exception)
def handle_exception(e):
    import traceback
    error_msg = f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
    print(error_msg)
    # Return JSON for API requests
    try:
        if _is_api_request_check():
            return jsonify({'success': False, 'error': str(e)}), 500
    except:
        pass
    return f"<pre>{error_msg}</pre>", 500

# Video storage paths (for local development)
VIDEOS_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'videos')
os.makedirs(VIDEOS_FOLDER, exist_ok=True)

# Categories
CATEGORIES = {
    'uncategorized': {
        'name': 'Uncategorized',
        'abbrev': 'UN',
        'description': 'Videos pending categorization',
        'subcategories': []
    },
    'cf': {
        'name': 'Canopy Formation',
        'abbrev': 'CF',
        'description': 'Chapter 10 - Canopy Formation competition videos',
        'subcategories': [
            {'id': '4way_rot', 'name': '4-Way Rotation'},
            {'id': '4way_seq', 'name': '4-Way Sequential'},
            {'id': '2way_open', 'name': '2-Way Sequential Open'},
            {'id': '2way_proam', 'name': '2-Way Sequential Pro/Am'}
        ]
    },
    'cp': {
        'name': 'Canopy Piloting',
        'abbrev': 'CP',
        'description': 'Chapters 12-13 - Canopy Piloting competition videos',
        'subcategories': [
            {'id': 'dsz', 'name': 'Distance/Speed/Zone (Individual)'},
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
        'subcategories': [
            {'id': 'acrobatic', 'name': 'Acrobatic'}
        ]
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
            {'id': '10way', 'name': '10-Way'},
            {'id': 'indoor_4way_fs', 'name': 'Indoor 4-Way FS'},
            {'id': 'indoor_4way_vfs', 'name': 'Indoor 4-Way VFS'},
            {'id': 'indoor_2way_fs', 'name': 'Indoor 2-Way FS'},
            {'id': 'indoor_2way_vfs', 'name': 'Indoor 2-Way VFS'},
            {'id': 'indoor_8way', 'name': 'Indoor 8-Way'}
        ]
    }
}

DATABASE = 'videos.db'

# Role hierarchy (higher number = more access)
ROLES = {
    'judge': 1,           # Can view and score videos
    'event_judge': 2,     # Can manage event-specific content
    'chief_judge': 3,     # Can manage all competitions
    'admin': 4            # Full access
}

def get_user_role_level(role):
    """Get numeric level for a role."""
    return ROLES.get(role, 0)

def has_role(required_role):
    """Check if current user has at least the required role level."""
    user_role = session.get('role', '')
    return get_user_role_level(user_role) >= get_user_role_level(required_role)

def role_required(required_role):
    """Decorator to require a minimum role level."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('username'):
                return redirect(url_for('login'))
            if not has_role(required_role):
                return "Access denied. Insufficient permissions.", 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# Convenience decorators for each role
def judge_required(f):
    return role_required('judge')(f)

def event_judge_required(f):
    return role_required('event_judge')(f)

def chief_judge_required(f):
    return role_required('chief_judge')(f)


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
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN start_time REAL DEFAULT 0')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN draw TEXT')
        except:
            pass

        # Competitions tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS competitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_types TEXT,
                total_rounds INTEGER DEFAULT 10,
                created_at TEXT NOT NULL,
                status TEXT DEFAULT 'active'
            )
        ''')

        # Add event_types column if it doesn't exist (for existing databases)
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN event_types TEXT')
        except:
            pass

        # Add event_rounds column if it doesn't exist (rounds per event type)
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN event_rounds TEXT')
        except:
            pass

        # Add chief_judge column if it doesn't exist
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN chief_judge TEXT')
        except:
            pass

        # Add chief_judge_pin column if it doesn't exist
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN chief_judge_pin TEXT')
        except:
            pass

        # Add event_locations column if it doesn't exist (JSON: event_type -> location)
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN event_locations TEXT')
        except:
            pass

        # Add event_dates column if it doesn't exist (JSON: event_type -> date)
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN event_dates TEXT')
        except:
            pass

        # Add draws column if it doesn't exist (JSON: event_type -> class -> rounds -> formations)
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN draws TEXT')
        except:
            pass

        # Add WS Performance columns
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN ws_reference_points TEXT')
        except:
            pass

        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN ws_validation_window TEXT')
        except:
            pass

        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN ws_competitor_ref_points TEXT')
        except:
            pass

        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN ws_field_elevation REAL')
        except:
            pass

        # Add score_approvals column if it doesn't exist (JSON: event_type -> round -> {approved_at, approved_by})
        try:
            cursor.execute('ALTER TABLE competitions ADD COLUMN score_approvals TEXT')
        except:
            pass

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS competition_teams (
                id TEXT PRIMARY KEY,
                competition_id TEXT NOT NULL,
                team_number TEXT NOT NULL,
                team_name TEXT NOT NULL,
                class TEXT NOT NULL,
                members TEXT,
                category TEXT,
                event TEXT,
                photo TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (competition_id) REFERENCES competitions(id)
            )
        ''')

        # Add display_order column if it doesn't exist
        try:
            cursor.execute('ALTER TABLE competition_teams ADD COLUMN display_order INTEGER DEFAULT 0')
        except:
            pass

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS competition_scores (
                id TEXT PRIMARY KEY,
                competition_id TEXT NOT NULL,
                team_id TEXT NOT NULL,
                round_num INTEGER NOT NULL,
                score REAL,
                score_data TEXT,
                video_id TEXT,
                scored_by TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (competition_id) REFERENCES competitions(id),
                FOREIGN KEY (team_id) REFERENCES competition_teams(id)
            )
        ''')

        # Add scored_by column if it doesn't exist
        try:
            cursor.execute('ALTER TABLE competition_scores ADD COLUMN scored_by TEXT')
        except:
            pass

        # Add rejump column if it doesn't exist
        try:
            cursor.execute('ALTER TABLE competition_scores ADD COLUMN rejump INTEGER DEFAULT 0')
        except:
            pass

        # Add training_flag column if it doesn't exist (for flagging videos as training material)
        try:
            cursor.execute('ALTER TABLE competition_scores ADD COLUMN training_flag INTEGER DEFAULT 0')
        except:
            pass

        # Add exit_time_penalty column for CF events (20% penalty when exit time not determined)
        try:
            cursor.execute('ALTER TABLE competition_scores ADD COLUMN exit_time_penalty INTEGER DEFAULT 0')
        except:
            pass

        # Practice competitions tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS practice_competitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT DEFAULT 'active'
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS practice_assignments (
                id TEXT PRIMARY KEY,
                practice_id TEXT NOT NULL,
                judge_username TEXT NOT NULL,
                assigned_at TEXT NOT NULL,
                FOREIGN KEY (practice_id) REFERENCES practice_competitions(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                must_change_password INTEGER DEFAULT 0
            )
        ''')

        # Add columns if they don't exist (for existing databases)
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN email TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN signature_pin TEXT')
        except:
            pass

        # Add signature_data column if it doesn't exist (stores base64 PNG of signature)
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN signature_data TEXT')
        except:
            pass

        # Add assigned_categories column for judge category restrictions (JSON array)
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN assigned_categories TEXT')
        except:
            pass

        # Video assignments table (for chief judge to assign videos to judges)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_assignments (
                id TEXT PRIMARY KEY,
                video_id TEXT NOT NULL,
                assigned_to TEXT NOT NULL,
                assigned_by TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (video_id) REFERENCES videos(id),
                FOREIGN KEY (assigned_to) REFERENCES users(username),
                FOREIGN KEY (assigned_by) REFERENCES users(username)
            )
        ''')

        # Add practice_score columns to video_assignments if they don't exist
        try:
            cursor.execute('ALTER TABLE video_assignments ADD COLUMN practice_score REAL')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE video_assignments ADD COLUMN practice_score_data TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE video_assignments ADD COLUMN scored_at TEXT')
        except:
            pass

        cursor.execute('SELECT username FROM users WHERE username = ?', ('admin',))
        if not cursor.fetchone():
            cursor.execute(
                'INSERT INTO users (username, password, role, name, email, must_change_password) VALUES (?, ?, ?, ?, ?, ?)',
                ('admin', 'admin123', 'admin', 'Administrator', '', 0)
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


def find_duplicate_video(title, duration, url=None):
    """Check if a video with the same title and duration already exists.
    Returns the existing video if found, None otherwise."""
    if USE_SUPABASE:
        # First check by URL (exact match)
        if url:
            result = supabase.table('videos').select('*').eq('url', url).execute()
            if result.data:
                return result.data[0]

        # Then check by title and duration
        query = supabase.table('videos').select('*').eq('title', title)
        if duration:
            query = query.eq('duration', duration)
        result = query.execute()
        if result.data:
            return result.data[0]
    else:
        db = get_sqlite_db()
        # Check by URL first
        if url:
            cursor = db.execute('SELECT * FROM videos WHERE url = ?', (url,))
            row = cursor.fetchone()
            if row:
                return dict(row)

        # Check by title and duration
        if duration:
            cursor = db.execute('SELECT * FROM videos WHERE title = ? AND duration = ?', (title, duration))
        else:
            cursor = db.execute('SELECT * FROM videos WHERE title = ?', (title,))
        row = cursor.fetchone()
        if row:
            return dict(row)

    return None


def save_video(video_data):
    """Save a video to database."""
    if USE_SUPABASE:
        # Filter to only include core Supabase columns (start_time/draw handled separately)
        known_columns = {'id', 'title', 'description', 'url', 'thumbnail', 'category',
                        'subcategory', 'tags', 'duration', 'created_at', 'views',
                        'video_type', 'local_file', 'event', 'team', 'round_num', 'jump_num'}
        filtered_data = {k: v for k, v in video_data.items() if k in known_columns}

        existing = supabase.table('videos').select('id').eq('id', video_data['id']).execute()
        if existing.data:
            supabase.table('videos').update(filtered_data).eq('id', video_data['id']).execute()
        else:
            supabase.table('videos').insert(filtered_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO videos (id, title, description, url, thumbnail, category, subcategory, tags, duration, created_at, views, video_type, local_file, event, team, round_num, jump_num, start_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (video_data['id'], video_data['title'], video_data.get('description', ''),
              video_data.get('url', ''), video_data.get('thumbnail'), video_data['category'],
              video_data.get('subcategory', ''), video_data.get('tags', ''),
              video_data.get('duration', ''), video_data['created_at'],
              video_data.get('views', 0), video_data.get('video_type', 'url'),
              video_data.get('local_file', ''), video_data.get('event', ''),
              video_data.get('team', ''), video_data.get('round_num', ''),
              video_data.get('jump_num', ''), video_data.get('start_time', 0)))
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


def get_all_users():
    """Get all users from database."""
    if USE_SUPABASE:
        result = supabase.table('users').select('*').order('username').execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM users ORDER BY username')
        return [dict(row) for row in cursor.fetchall()]


def save_user(user_data):
    """Save or update a user."""
    must_change = user_data.get('must_change_password', 0)
    email = user_data.get('email', '')
    signature_pin = user_data.get('signature_pin', '')
    assigned_categories = user_data.get('assigned_categories', '')
    if USE_SUPABASE:
        # Only send fields that exist in Supabase users table
        supabase_data = {
            'username': user_data['username'],
            'password': user_data['password'],
            'role': user_data['role'],
            'name': user_data['name'],
            'email': email,
            'must_change_password': must_change,
            'signature_pin': signature_pin,
            'assigned_categories': assigned_categories
        }
        existing = supabase.table('users').select('username').eq('username', user_data['username']).execute()
        if existing.data:
            supabase.table('users').update(supabase_data).eq('username', user_data['username']).execute()
        else:
            supabase.table('users').insert(supabase_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO users (username, password, role, name, email, must_change_password, signature_pin, assigned_categories)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_data['username'], user_data['password'], user_data['role'], user_data['name'], email, must_change, signature_pin, assigned_categories))
        db.commit()


def get_user_by_email(email):
    """Get user by email address."""
    if USE_SUPABASE:
        result = supabase.table('users').select('*').eq('email', email).execute()
        return result.data[0] if result.data else None
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM users WHERE email = ?', (email,))
        row = cursor.fetchone()
        return dict(row) if row else None


def delete_user(username):
    """Delete a user."""
    if USE_SUPABASE:
        supabase.table('users').delete().eq('username', username).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM users WHERE username = ?', (username,))
        db.commit()


# Video assignment functions
def create_video_assignment(video_id, assigned_to, assigned_by, notes=''):
    """Create a video assignment."""
    assignment_id = str(uuid.uuid4())[:8]
    assignment = {
        'id': assignment_id,
        'video_id': video_id,
        'assigned_to': assigned_to,
        'assigned_by': assigned_by,
        'status': 'pending',
        'notes': notes,
        'created_at': datetime.now().isoformat()
    }
    if USE_SUPABASE:
        supabase.table('video_assignments').insert(assignment).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT INTO video_assignments (id, video_id, assigned_to, assigned_by, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (assignment_id, video_id, assigned_to, assigned_by, 'pending', notes, assignment['created_at']))
        db.commit()
    return assignment_id


def get_assignments_for_user(username):
    """Get all video assignments for a user."""
    if USE_SUPABASE:
        result = supabase.table('video_assignments').select('*').eq('assigned_to', username).order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM video_assignments WHERE assigned_to = ? ORDER BY created_at DESC', (username,))
        return [dict(row) for row in cursor.fetchall()]


def get_assignments_by_assigner(username):
    """Get all assignments created by a user (chief judge)."""
    if USE_SUPABASE:
        result = supabase.table('video_assignments').select('*').eq('assigned_by', username).order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM video_assignments WHERE assigned_by = ? ORDER BY created_at DESC', (username,))
        return [dict(row) for row in cursor.fetchall()]


def update_assignment_status(assignment_id, status):
    """Update assignment status (pending, in_progress, completed)."""
    if USE_SUPABASE:
        supabase.table('video_assignments').update({'status': status}).eq('id', assignment_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('UPDATE video_assignments SET status = ? WHERE id = ?', (status, assignment_id))
        db.commit()


def delete_assignment(assignment_id):
    """Delete a video assignment."""
    if USE_SUPABASE:
        supabase.table('video_assignments').delete().eq('id', assignment_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM video_assignments WHERE id = ?', (assignment_id,))
        db.commit()


def get_all_assignments():
    """Get all video assignments."""
    if USE_SUPABASE:
        result = supabase.table('video_assignments').select('*').order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM video_assignments ORDER BY created_at DESC')
        return [dict(row) for row in cursor.fetchall()]


# Competition database functions
def get_all_competitions():
    """Get all competitions."""
    if USE_SUPABASE:
        result = supabase.table('competitions').select('*').order('created_at', desc=True).execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competitions ORDER BY created_at DESC')
        return [dict(row) for row in cursor.fetchall()]


def get_competition(comp_id):
    """Get a single competition."""
    if USE_SUPABASE:
        result = supabase.table('competitions').select('*').eq('id', comp_id).execute()
        return result.data[0] if result.data else None
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competitions WHERE id = ?', (comp_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def save_competition(comp_data):
    """Save a competition."""
    if USE_SUPABASE:
        existing = supabase.table('competitions').select('id').eq('id', comp_data['id']).execute()
        if existing.data:
            supabase.table('competitions').update(comp_data).eq('id', comp_data['id']).execute()
        else:
            supabase.table('competitions').insert(comp_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO competitions (id, name, event_type, event_types, event_rounds, total_rounds, created_at, status, chief_judge, chief_judge_pin, event_locations, event_dates, draws, ws_reference_points, ws_validation_window, ws_competitor_ref_points, ws_field_elevation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (comp_data['id'], comp_data['name'], comp_data['event_type'],
              comp_data.get('event_types', ''), comp_data.get('event_rounds', '{}'),
              comp_data.get('total_rounds', 10), comp_data['created_at'], comp_data.get('status', 'active'),
              comp_data.get('chief_judge', ''), comp_data.get('chief_judge_pin', ''),
              comp_data.get('event_locations', '{}'), comp_data.get('event_dates', '{}'),
              comp_data.get('draws', '{}'),
              comp_data.get('ws_reference_points'),
              comp_data.get('ws_validation_window'),
              comp_data.get('ws_competitor_ref_points'),
              comp_data.get('ws_field_elevation', 0)))
        db.commit()


def delete_competition_db(comp_id):
    """Delete a competition and its teams/scores."""
    if USE_SUPABASE:
        supabase.table('competition_scores').delete().eq('competition_id', comp_id).execute()
        supabase.table('competition_teams').delete().eq('competition_id', comp_id).execute()
        supabase.table('competitions').delete().eq('id', comp_id).execute()
    else:
        db = get_sqlite_db()
        try:
            # Delete in correct order to avoid foreign key issues
            db.execute('DELETE FROM competition_scores WHERE competition_id = ?', (comp_id,))
            db.execute('DELETE FROM competition_teams WHERE competition_id = ?', (comp_id,))
            db.execute('DELETE FROM competitions WHERE id = ?', (comp_id,))
            db.commit()
        except Exception as e:
            db.rollback()
            raise e


def get_competition_teams(comp_id, class_filter=None):
    """Get teams for a competition."""
    if USE_SUPABASE:
        query = supabase.table('competition_teams').select('*').eq('competition_id', comp_id)
        if class_filter:
            query = query.eq('class', class_filter)
        result = query.order('team_number').execute()
        return result.data
    else:
        db = get_sqlite_db()
        if class_filter:
            cursor = db.execute(
                'SELECT * FROM competition_teams WHERE competition_id = ? AND class = ? ORDER BY team_number',
                (comp_id, class_filter)
            )
        else:
            cursor = db.execute(
                'SELECT * FROM competition_teams WHERE competition_id = ? ORDER BY class, team_number',
                (comp_id,)
            )
        return [dict(row) for row in cursor.fetchall()]


def get_team(team_id):
    """Get a single team."""
    if USE_SUPABASE:
        result = supabase.table('competition_teams').select('*').eq('id', team_id).execute()
        return result.data[0] if result.data else None
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competition_teams WHERE id = ?', (team_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def save_team(team_data):
    """Save a team."""
    if USE_SUPABASE:
        existing = supabase.table('competition_teams').select('id').eq('id', team_data['id']).execute()
        if existing.data:
            supabase.table('competition_teams').update(team_data).eq('id', team_data['id']).execute()
        else:
            supabase.table('competition_teams').insert(team_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO competition_teams (id, competition_id, team_number, team_name, class, members, category, event, photo, created_at, display_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (team_data['id'], team_data['competition_id'], team_data['team_number'],
              team_data['team_name'], team_data['class'], team_data.get('members', ''),
              team_data.get('category', ''), team_data.get('event', ''),
              team_data.get('photo', ''), team_data['created_at'], team_data.get('display_order', 0)))
        db.commit()


def delete_team_db(team_id):
    """Delete a team and its scores."""
    if USE_SUPABASE:
        supabase.table('competition_scores').delete().eq('team_id', team_id).execute()
        supabase.table('competition_teams').delete().eq('id', team_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM competition_scores WHERE team_id = ?', (team_id,))
        db.execute('DELETE FROM competition_teams WHERE id = ?', (team_id,))
        db.commit()


def get_team_scores(team_id):
    """Get all scores for a team."""
    if USE_SUPABASE:
        result = supabase.table('competition_scores').select('*').eq('team_id', team_id).order('round_num').execute()
        return result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competition_scores WHERE team_id = ? ORDER BY round_num', (team_id,))
        return [dict(row) for row in cursor.fetchall()]


def save_score(score_data):
    """Save a score."""
    if USE_SUPABASE:
        # Only include columns that exist in Supabase
        supabase_data = {
            'id': score_data['id'],
            'competition_id': score_data['competition_id'],
            'team_id': score_data['team_id'],
            'round_num': score_data['round_num'],
            'score': score_data.get('score'),
            'score_data': score_data.get('score_data', ''),
            'video_id': score_data.get('video_id', ''),
            'scored_by': score_data.get('scored_by', ''),
            'rejump': score_data.get('rejump', 0),
            'created_at': score_data['created_at']
        }
        # Add optional columns if they have values (these may not exist in all Supabase setups)
        # training_flag and exit_time_penalty are newer columns
        existing = supabase.table('competition_scores').select('id').eq('id', score_data['id']).execute()
        if existing.data:
            supabase.table('competition_scores').update(supabase_data).eq('id', score_data['id']).execute()
        else:
            supabase.table('competition_scores').insert(supabase_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT OR REPLACE INTO competition_scores (id, competition_id, team_id, round_num, score, score_data, video_id, scored_by, rejump, training_flag, exit_time_penalty, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (score_data['id'], score_data['competition_id'], score_data['team_id'],
              score_data['round_num'], score_data.get('score'), score_data.get('score_data', ''),
              score_data.get('video_id', ''), score_data.get('scored_by', ''), score_data.get('rejump', 0),
              score_data.get('training_flag', 0), score_data.get('exit_time_penalty', 0), score_data['created_at']))
        db.commit()


# Initialize database
def safe_init_db():
    try:
        init_db()
        print(f"Database initialized ({'Supabase' if USE_SUPABASE else 'SQLite'})")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")

safe_init_db()


def is_api_request():
    """Check if the current request is an API/AJAX request expecting JSON."""
    # Check Content-Type header
    content_type = request.headers.get('Content-Type', '')
    if 'application/json' in content_type:
        return True
    # Check Accept header
    accept = request.headers.get('Accept', '')
    if 'application/json' in accept:
        return True
    # Check if request has JSON body
    if request.is_json:
        return True
    # Check X-Requested-With header (common for AJAX)
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return True
    return False


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            # For API requests (JSON), return JSON error instead of redirect
            if is_api_request():
                return jsonify({'success': False, 'error': 'Login required. Please log in.'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session.get('role') != 'admin':
            # For API requests (JSON), return JSON error instead of redirect
            if is_api_request():
                return jsonify({'success': False, 'error': 'Admin access required. Please log in.'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def is_direct_video_url(url):
    """Check if URL is a direct video file."""
    if not url:
        return False
    url_lower = url.lower()
    # Supabase Storage URLs are always streamable
    if 'supabase.co/storage' in url_lower or 'supabase.in/storage' in url_lower:
        return True
    # Dropbox direct links are always streamable
    if 'dropboxusercontent.com' in url_lower or 'dropbox.com' in url_lower:
        return True
    # Browser-supported video formats
    video_extensions = ('.mp4', '.webm', '.ogg', '.ogv', '.mov', '.m4v')
    return any(url_lower.endswith(ext) or f'{ext}?' in url_lower or f'{ext}&' in url_lower for ext in video_extensions)


def fetch_vimeo_metadata(url):
    """Fetch title and thumbnail from Vimeo using oEmbed API."""
    import socket
    old_timeout = socket.getdefaulttimeout()
    try:
        # Set aggressive socket timeout to prevent worker hangs
        socket.setdefaulttimeout(5)

        # Clean URL - ensure it's the standard format
        clean_url = url.split('?')[0]  # Remove query params for oEmbed

        oembed_url = f"https://vimeo.com/api/oembed.json?url={urllib.parse.quote(clean_url, safe='')}"
        req = urllib.request.Request(oembed_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            return {
                'title': data.get('title', ''),
                'thumbnail': data.get('thumbnail_url', ''),
                'duration': data.get('duration', 0)
            }
    except Exception as e:
        print(f"Vimeo metadata fetch error for {url}: {e}")
        # Return basic info even if API fails
        return {
            'title': 'Vimeo Video',
            'thumbnail': '',
            'duration': 0
        }
    finally:
        socket.setdefaulttimeout(old_timeout)


def fetch_youtube_metadata(url):
    """Fetch title and thumbnail from YouTube using oEmbed API."""
    import socket
    old_timeout = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(5)
        oembed_url = f"https://www.youtube.com/oembed?url={urllib.parse.quote(url, safe='')}&format=json"
        req = urllib.request.Request(oembed_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())

            # Get video ID for high-quality thumbnail
            yt_id = None
            if 'youtu.be/' in url:
                yt_id = url.split('youtu.be/')[-1].split('?')[0]
            elif 'v=' in url:
                yt_id = url.split('v=')[-1].split('&')[0]

            thumbnail = f"https://img.youtube.com/vi/{yt_id}/hqdefault.jpg" if yt_id else ''

            return {
                'title': data.get('title', ''),
                'thumbnail': thumbnail
            }
    except Exception as e:
        print(f"YouTube metadata fetch error: {e}")
        return None
    finally:
        socket.setdefaulttimeout(old_timeout)


# Formats that browsers can play natively
BROWSER_PLAYABLE_FORMATS = ('.mp4', '.webm', '.ogg', '.ogv', '.mov', '.m4v')
# Formats that need conversion
CONVERSION_FORMATS = ('.mts', '.m2ts', '.avi', '.mkv', '.wmv', '.flv', '.3gp')


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
    elif 'player.vimeo.com/video/' in url:
        # Already an embed URL
        return url.split('?')[0]
    elif 'vimeo.com/' in url:
        # Handle various Vimeo URL formats:
        # https://vimeo.com/123456789
        # https://vimeo.com/123456789/abcdef (unlisted with hash)
        # https://vimeo.com/channels/xxx/123456789
        # Extract video ID (just the numbers)
        match = re.search(r'vimeo\.com/(?:channels/[^/]+/|video/)?(\d+)', url)
        if match:
            video_id = match.group(1)
            # Check if there's an unlisted hash
            hash_match = re.search(r'vimeo\.com/\d+/([a-f0-9]+)', url)
            if hash_match:
                return f'https://player.vimeo.com/video/{video_id}?h={hash_match.group(1)}'
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
        'team_number': '',
        'round': '',
        'jump': '',
        'title': '',
        'class': ''
    }

    # Check for structured filename format: Event_EventType_TeamNum-TeamName_Round
    # Examples:
    #   5thFAIWorldIndoorSkydivingChampionships-FormationSkydiving_FS4-Way-Female_421-SingaporeFemale_1.mkv
    #   JudgeSeminarMeet_FS4-Way-Open_408-Brazil4_1.mkv
    parts = name.split('_')
    if len(parts) >= 4:
        # Try to parse structured format
        event_part = parts[0]
        event_type_part = parts[1]
        team_part = parts[2]
        round_part = parts[3] if len(parts) > 3 else ''

        # Map event types to category and subcategory (indoor events now under fs)
        event_type_mapping = {
            'fs4-way-open': ('fs', 'indoor_4way_fs', 'open'),
            'fs4-way-female': ('fs', 'indoor_4way_fs', 'female'),
            'fs4-way-junior': ('fs', 'indoor_4way_fs', 'junior'),
            'fs8-way-open': ('fs', 'indoor_8way', 'open'),
            'fs8-way-female': ('fs', 'indoor_8way', 'female'),
            'vfs-open': ('fs', 'indoor_4way_vfs', 'open'),
            'vfs-female': ('fs', 'indoor_4way_vfs', 'female'),
            '2way-mfs': ('fs', 'indoor_2way_fs', 'open'),
            '2way-fs': ('fs', 'indoor_2way_fs', 'open'),
            '2way-vfs': ('fs', 'indoor_2way_vfs', 'open'),
            'fs-4way-open': ('fs', 'indoor_4way_fs', 'open'),
            'fs-4way-female': ('fs', 'indoor_4way_fs', 'female'),
            'fs-8way-open': ('fs', 'indoor_8way', 'open'),
            'fs-8way-female': ('fs', 'indoor_8way', 'female'),
        }

        event_type_lower = event_type_part.lower()
        if event_type_lower in event_type_mapping:
            cat, subcat, class_name = event_type_mapping[event_type_lower]
            metadata['category'] = cat
            metadata['subcategory'] = subcat
            metadata['class'] = class_name

        # Parse event name (clean up the event part)
        event_name = event_part.replace('-', ' ').strip()
        # Add event type descriptor for clarity
        if event_type_part:
            event_name = f"{event_name} - {event_type_part.replace('-', ' ')}"
        metadata['event'] = event_name

        # Parse team number and name (format: 421-SingaporeFemale or 408-Brazil4)
        team_match = re.match(r'(\d+)-(.+)', team_part)
        if team_match:
            metadata['team_number'] = team_match.group(1)
            metadata['team'] = team_match.group(2)
        else:
            metadata['team'] = team_part

        # Parse round number
        if round_part and round_part.isdigit():
            metadata['round'] = round_part

        # Build title
        title_parts = []
        if metadata['team']:
            title_parts.append(metadata['team'])
        if metadata['team_number']:
            title_parts.append(f"#{metadata['team_number']}")
        if metadata['round']:
            title_parts.append(f"Round {metadata['round']}")
        metadata['title'] = ' - '.join(title_parts) if title_parts else name

        return metadata

    # Fall back to generic parsing for non-structured filenames
    # Category detection
    category_patterns = {
        'cp': [r'\bcp\b', r'canopy.?piloting'],
        'fs': [r'\bfs\b', r'formation.?skydiving'],
        'cf': [r'\bcf\b', r'canopy.?formation', r'\bcrw\b'],
        'ae': [r'\bae\b', r'artistic', r'\bfreestyle\b', r'\bfreefly\b'],
        'ws': [r'\bws\b', r'wingsuit']
    }

    # Check if indoor content
    is_indoor_content = bool(re.search(r'\bindoor\b|wind.?tunnel|\bifly\b', combined))

    for cat_id, patterns in category_patterns.items():
        for pattern in patterns:
            if re.search(pattern, combined):
                metadata['category'] = cat_id
                break
        if metadata['category']:
            break

    # If indoor detected, set category to fs
    if is_indoor_content and not metadata['category']:
        metadata['category'] = 'fs'

    # Subcategory detection
    subcategory_patterns = {
        'cp': {
            'freestyle': [r'freestyle', r'free.?style'],
            'speed': [r'\bspeed\b'],
            'distance': [r'\bdistance\b'],
            'zone_accuracy': [r'zone', r'zone.?accuracy']
        },
        'fs': {
            'indoor_4way_fs': [r'indoor.*4.?way(?!.*vfs)', r'indoor.*fs.?4'],
            'indoor_4way_vfs': [r'indoor.*vfs', r'indoor.*vertical'],
            'indoor_2way_fs': [r'indoor.*2.?way(?!.*vfs)', r'indoor.*mfs'],
            'indoor_2way_vfs': [r'indoor.*2.?way.*vfs'],
            'indoor_8way': [r'indoor.*8.?way'],
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

    # If indoor content and fs category, prefix subcategory with indoor_
    if is_indoor_content and metadata['category'] == 'fs' and metadata['subcategory']:
        if not metadata['subcategory'].startswith('indoor_'):
            indoor_sub = f"indoor_{metadata['subcategory']}"
            if indoor_sub in ['indoor_4way_fs', 'indoor_4way_vfs', 'indoor_2way_fs', 'indoor_2way_vfs', 'indoor_8way']:
                metadata['subcategory'] = indoor_sub

    # Event detection from folder path and filename
    folder_parts = folder_path.split(os.sep)
    for part in folder_parts:
        part_lower = part.lower()
        # Look for year + event keywords
        if re.search(r'20\d{2}', part) or any(kw in part_lower for kw in ['nationals', 'championship', 'world', 'uspa', 'competition']):
            if len(part) > 5:
                metadata['event'] = part.replace('_', ' ').replace('-', ' ').strip()
                break

    # If no event found from folder, try to detect from filename
    if not metadata['event']:
        event_patterns = [
            (r'(\d{4})\s*nationals?', r'\1 Nationals'),
            (r'nationals?\s*(\d{4})', r'\1 Nationals'),
            (r'uspa\s*nationals?\s*(\d{4})', r'USPA Nationals \1'),
            (r'(\d{4})\s*uspa\s*nationals?', r'USPA Nationals \1'),
            (r'(\d{4})\s*worlds?', r'\1 World Championships'),
            (r'worlds?\s*(\d{4})', r'\1 World Championships'),
            (r'world\s*championships?\s*(\d{4})', r'\1 World Championships'),
            (r'(\d{4})\s*regionals?', r'\1 Regionals'),
            (r'regionals?\s*(\d{4})', r'\1 Regionals'),
            (r'(\d{4})\s*indoor\s*nationals?', r'\1 Indoor Nationals'),
            (r'pops\s*(\d{4})', r'POPs \1'),
            (r'(\d{4})\s*pops', r'POPs \1'),
        ]

        for pattern, replacement in event_patterns:
            match = re.search(pattern, combined)
            if match:
                detected_event = re.sub(pattern, replacement, match.group(0))
                metadata['event'] = ' '.join(word.capitalize() for word in detected_event.split())
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


def detect_category_from_filename(filename):
    """Auto-detect category, subcategory, and event name from filename."""
    import re

    if not filename:
        return None, None, None

    name_lower = filename.lower()
    detected_category = None
    detected_subcategory = None
    detected_event = None

    # Check for "indoor" first - it takes priority as main category
    # VFS (Vertical Formation Skydiving) is typically indoor/tunnel
    indoor_patterns = ['indoor', 'wind tunnel', 'windtunnel', 'ifly', 'tunnel']
    is_indoor = any(pattern in name_lower for pattern in indoor_patterns)

    if is_indoor:
        detected_category = 'fs'  # Indoor is now under FS

    # Category detection patterns (order matters - more specific first)
    category_patterns = {
        'cp': ['canopy piloting', '_cp_', '-cp-', ' cp ', 'cp_', '_cp', 'canopypiloting', 'swooping'],
        'cf': ['canopy formation', '_cf_', '-cf-', ' cf ', 'cf_', '_cf', 'canopyformation', 'crw'],
        'fs': ['formation skydiving', '_fs_', '-fs-', ' fs ', 'fs_', '_fs', 'formationskydiving', 'rw', 'vfs'],
        'ae': ['artistic', '_ae_', '-ae-', ' ae ', 'ae_', '_ae', 'freestyle', 'freefly'],
        'ws': ['wingsuit', '_ws_', '-ws-', ' ws ', 'ws_', '_ws'],
    }

    # Subcategory detection patterns (order matters - more specific first)
    subcategory_patterns = {
        'cp': {
            'freestyle': ['cp freestyle', 'cp_freestyle', 'cpfreestyle'],
            'speed': ['speed run', 'speedrun'],
            'distance': ['distance'],
            'zone_accuracy': ['zone', 'pond swoop']
        },
        'fs': {
            # Indoor subcategories (check first when is_indoor)
            'indoor_2way_vfs': ['indoor 2way vfs', 'indoor 2-way vfs', '2way vfs', '2-way vfs', '2wayvfs'],
            'indoor_4way_vfs': ['indoor 4way vfs', 'indoor 4-way vfs', '4way vfs', '4-way vfs', '4wayvfs', 'indoor vfs'],
            'indoor_2way_fs': ['indoor 2way', 'indoor 2-way', 'indoor 2 way'],
            'indoor_4way_fs': ['indoor 4way', 'indoor 4-way', 'indoor 4 way'],
            'indoor_8way': ['indoor 8way', 'indoor 8-way', 'indoor 8 way'],
            # Regular FS subcategories
            '4way_vfs': ['vfs', 'vertical'],
            '4way_fs': ['4way', '4-way', '4 way'],
            '2way_mfs': ['2way', '2-way', '2 way', 'mfs'],
            '8way': ['8way', '8-way', '8 way'],
            '10way': ['10way', '10-way', '10 way'],
            '16way': ['16way', '16-way', '16 way']
        },
        'cf': {
            '4way_rot': ['4way rot', '4-way rot', 'rotation'],
            '4way_seq': ['4way seq', '4-way seq', 'sequential'],
            '2way': ['2way', '2-way', '2 way']
        },
        'ae': {
            'freefly': ['freefly', 'free fly'],
            'freestyle': ['freestyle', 'free style']
        },
        'ws': {
            'performance': ['performance', 'perf'],
            'acrobatic': ['acrobatic', 'acro']
        }
    }

    # Event name patterns (common competition names)
    event_patterns = [
        # Nationals patterns
        (r'(\d{4})\s*nationals?', r'\1 Nationals'),
        (r'nationals?\s*(\d{4})', r'\1 Nationals'),
        (r'uspa\s*nationals?\s*(\d{4})', r'USPA Nationals \1'),
        (r'(\d{4})\s*uspa\s*nationals?', r'USPA Nationals \1'),
        # World patterns
        (r'(\d{4})\s*worlds?', r'\1 World Championships'),
        (r'worlds?\s*(\d{4})', r'\1 World Championships'),
        (r'world\s*championships?\s*(\d{4})', r'\1 World Championships'),
        # Regional patterns
        (r'(\d{4})\s*regionals?', r'\1 Regionals'),
        (r'regionals?\s*(\d{4})', r'\1 Regionals'),
        # Other common events
        (r'(\d{4})\s*indoor\s*nationals?', r'\1 Indoor Nationals'),
        (r'pops\s*(\d{4})', r'POPs \1'),
        (r'(\d{4})\s*pops', r'POPs \1'),
        # Generic year-based event detection
        (r'([a-z\s]+)\s*(\d{4})', None),  # Will be handled specially
    ]

    # Detect category (only if not already detected)
    if not detected_category:
        for cat_id, patterns in category_patterns.items():
            for pattern in patterns:
                if pattern in name_lower:
                    detected_category = cat_id
                    break
            if detected_category:
                break

    # Detect subcategory if category was found
    if detected_category and detected_category in subcategory_patterns:
        for sub_id, patterns in subcategory_patterns[detected_category].items():
            for pattern in patterns:
                if pattern in name_lower:
                    detected_subcategory = sub_id
                    break
            if detected_subcategory:
                break

        # If indoor was detected but no indoor-specific subcategory found, prefix with indoor_
        if is_indoor and detected_category == 'fs' and detected_subcategory and not detected_subcategory.startswith('indoor_'):
            indoor_sub = f'indoor_{detected_subcategory}'
            # Check if this indoor subcategory exists
            if indoor_sub in ['indoor_4way_fs', 'indoor_4way_vfs', 'indoor_2way_fs', 'indoor_2way_vfs', 'indoor_8way']:
                detected_subcategory = indoor_sub

    # Detect event name
    for pattern, replacement in event_patterns:
        match = re.search(pattern, name_lower)
        if match:
            if replacement:
                detected_event = re.sub(pattern, replacement, match.group(0))
                # Capitalize properly
                detected_event = ' '.join(word.capitalize() for word in detected_event.split())
            else:
                # Generic pattern - extract event name with year
                groups = match.groups()
                if len(groups) >= 2:
                    event_name = groups[0].strip()
                    year = groups[1]
                    # Clean up event name
                    event_name = ' '.join(word.capitalize() for word in event_name.split())
                    if event_name and len(event_name) > 2:
                        detected_event = f"{event_name} {year}"
            break

    return detected_category, detected_subcategory, detected_event


def convert_video_to_mp4(input_path, output_path):
    """Convert video to MP4 using ffmpeg."""
    try:
        subprocess.run([
            'ffmpeg', '-y', '-i', input_path,
            '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            output_path
        ], capture_output=True, check=True)
        return True
    except Exception as e:
        print(f"Conversion error: {e}")
        return False


def get_video_duration_seconds(file_path):
    """Get video duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
            capture_output=True, text=True
        )
        return float(result.stdout.strip())
    except:
        return None


def background_convert_video(job_id, input_path, output_path, video_data, temp_file=None):
    """Run video conversion in background thread with real-time progress."""
    try:
        # Wait in queue if too many conversions are running
        with conversion_lock:
            conversion_jobs[job_id]['status'] = 'queued'
            conversion_jobs[job_id]['progress'] = 0

        while True:
            with conversion_lock:
                active_count = sum(1 for j in conversion_jobs.values()
                                   if j.get('status') == 'converting')
                if active_count < MAX_CONCURRENT_CONVERSIONS:
                    conversion_jobs[job_id]['status'] = 'converting'
                    break
            time.sleep(2)  # Check every 2 seconds

        # Get input video duration for progress calculation
        total_duration = get_video_duration_seconds(input_path)

        # Run ffmpeg with progress output (stderr to DEVNULL to prevent blocking)
        process = subprocess.Popen([
            'ffmpeg', '-y', '-i', input_path,
            '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-progress', 'pipe:1',
            '-nostats',
            output_path
        ], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

        # Parse progress output in real-time
        for line in process.stdout:
            line = line.strip()
            # FFmpeg outputs out_time_us (microseconds) or out_time (HH:MM:SS.us format)
            if line.startswith('out_time_us='):
                try:
                    time_us = int(line.split('=')[1])
                    current_time = time_us / 1000000.0  # Convert microseconds to seconds
                    if total_duration and total_duration > 0:
                        # Progress 0-65% for conversion (leave room for thumbnail/upload)
                        progress = min(65, int((current_time / total_duration) * 65))
                        with conversion_lock:
                            conversion_jobs[job_id]['progress'] = progress
                except:
                    pass
            elif line.startswith('out_time=') and not line.startswith('out_time_us'):
                # Fallback: parse HH:MM:SS.microseconds format
                try:
                    time_str = line.split('=')[1]
                    parts = time_str.split(':')
                    if len(parts) == 3:
                        hours = int(parts[0])
                        minutes = int(parts[1])
                        seconds = float(parts[2])
                        current_time = hours * 3600 + minutes * 60 + seconds
                        if total_duration and total_duration > 0:
                            progress = min(65, int((current_time / total_duration) * 65))
                            with conversion_lock:
                                conversion_jobs[job_id]['progress'] = progress
                except:
                    pass
            elif line.startswith('progress=end'):
                break

        process.wait()

        if process.returncode != 0:
            with conversion_lock:
                conversion_jobs[job_id]['status'] = 'failed'
                conversion_jobs[job_id]['error'] = 'FFmpeg conversion failed'
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
            return

        with conversion_lock:
            conversion_jobs[job_id]['progress'] = 70

        # Clean up temp file
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)

        # Generate thumbnail
        video_id = video_data['id']
        thumbnail_filename = f"{video_id}_thumb.jpg"
        thumbnail_path = os.path.join(VIDEOS_FOLDER, thumbnail_filename)

        with conversion_lock:
            conversion_jobs[job_id]['status'] = 'generating_thumbnail'
            conversion_jobs[job_id]['progress'] = 80

        if generate_thumbnail(output_path, thumbnail_path):
            video_data['thumbnail'] = f"/static/videos/{thumbnail_filename}"

        # Get duration
        duration = get_video_duration(output_path)
        if duration:
            video_data['duration'] = duration

        with conversion_lock:
            conversion_jobs[job_id]['progress'] = 90

        # Upload to Supabase Storage if enabled
        if USE_SUPABASE:
            with conversion_lock:
                conversion_jobs[job_id]['status'] = 'uploading'

            # Upload video file
            video_filename = os.path.basename(output_path)
            video_url = upload_to_supabase_storage(output_path, f"videos/{video_filename}")
            if video_url:
                video_data['url'] = video_url
                video_data['video_type'] = 'url'
                video_data['local_file'] = ''
                # Clean up local file after upload
                if os.path.exists(output_path):
                    os.remove(output_path)
            else:
                # Fallback to local if upload fails
                video_data['local_file'] = video_filename

            # Upload thumbnail
            if os.path.exists(thumbnail_path):
                thumb_url = upload_to_supabase_storage(thumbnail_path, f"thumbnails/{thumbnail_filename}")
                if thumb_url:
                    video_data['thumbnail'] = thumb_url
                    os.remove(thumbnail_path)
        else:
            # Local storage
            video_data['local_file'] = os.path.basename(output_path)

        # Save video to database
        save_video(video_data)

        with conversion_lock:
            conversion_jobs[job_id]['status'] = 'completed'
            conversion_jobs[job_id]['progress'] = 100
            conversion_jobs[job_id]['video_id'] = video_id
            conversion_jobs[job_id]['completed_at'] = datetime.now().isoformat()

    except Exception as e:
        with conversion_lock:
            conversion_jobs[job_id]['status'] = 'failed'
            conversion_jobs[job_id]['error'] = str(e)
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)


@app.route('/conversion/status/<job_id>')
def conversion_status(job_id):
    """Get status of a background conversion job."""
    with conversion_lock:
        job = conversion_jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


@app.route('/conversion/active')
def active_conversions():
    """Get list of active conversion jobs for current session."""
    session_id = session.get('_id', request.remote_addr)
    with conversion_lock:
        active = {
            jid: job for jid, job in conversion_jobs.items()
            if job.get('session_id') == session_id and job.get('status') not in ('completed', 'failed')
        }
    return jsonify(active)


@app.route('/conversion/all')
def all_conversions():
    """Get all conversion jobs for current session (including completed)."""
    session_id = session.get('_id', request.remote_addr)
    with conversion_lock:
        jobs = {
            jid: job for jid, job in conversion_jobs.items()
            if job.get('session_id') == session_id
        }
    return jsonify(jobs)


@app.route('/conversion/clear-completed', methods=['POST'])
def clear_completed_conversions():
    """Clear completed/failed conversion jobs from the list."""
    session_id = session.get('_id', request.remote_addr)
    with conversion_lock:
        to_remove = [
            jid for jid, job in conversion_jobs.items()
            if job.get('session_id') == session_id and job.get('status') in ('completed', 'failed')
        ]
        for jid in to_remove:
            del conversion_jobs[jid]
    return jsonify({'success': True, 'cleared': len(to_remove)})


@app.route('/debug/db-status')
def debug_db_status():
    """Debug endpoint to check database connection status."""
    try:
        status = {
            'use_supabase': USE_SUPABASE,
            'supabase_url': SUPABASE_URL[:30] + '...' if SUPABASE_URL else None,
            'supabase_key_set': bool(SUPABASE_KEY),
        }

        if USE_SUPABASE:
            # Test query
            result = supabase.table('videos').select('category').execute()
            categories = {}
            for v in result.data:
                cat = v.get('category', 'unknown')
                categories[cat] = categories.get(cat, 0) + 1
            status['total_videos'] = len(result.data)
            status['categories'] = categories
        else:
            db = get_sqlite_db()
            cursor = db.execute('SELECT category, COUNT(*) FROM videos GROUP BY category')
            categories = {row[0]: row[1] for row in cursor.fetchall()}
            status['total_videos'] = sum(categories.values())
            status['categories'] = categories

        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e), 'use_supabase': USE_SUPABASE}), 500


@app.route('/favicon.ico')
def favicon():
    """Return empty favicon to avoid 404 errors."""
    return '', 204

def get_user_assigned_categories(username):
    """Get list of category IDs assigned to a user. Returns None if all categories are allowed."""
    if not username:
        return None
    user = get_user(username)
    if not user:
        return None
    assigned = user.get('assigned_categories', '')
    if not assigned or assigned == '[]':
        return None  # No restrictions - can see all categories
    try:
        cat_list = json.loads(assigned)
        if not cat_list:
            return None
        return cat_list
    except:
        return None


@app.route('/')
def index():
    """Home page showing all categories."""
    category_counts = {}
    for cat_id in CATEGORIES:
        category_counts[cat_id] = get_video_count_by_category(cat_id)

    all_videos = get_all_videos()
    recent_videos = all_videos[:8] if all_videos else []

    user_role = session.get('role', '')
    username = session.get('username')

    # Get assigned categories for the current user (for filtering)
    assigned_categories = get_user_assigned_categories(username)

    return render_template('index.html',
                         categories=CATEGORIES,
                         category_counts=category_counts,
                         recent_videos=recent_videos,
                         is_admin=user_role == 'admin',
                         is_chief_judge=has_role('chief_judge'),
                         is_logged_in=bool(session.get('username')),
                         user_name=session.get('name', ''),
                         user_role=user_role,
                         assigned_categories=assigned_categories)


@app.route('/category/<cat_id>')
def category(cat_id):
    """Show videos in a category."""
    if cat_id not in CATEGORIES:
        return "Category not found", 404

    # Check if user has access to this category
    username = session.get('username')
    assigned_categories = get_user_assigned_categories(username)
    if assigned_categories and cat_id != 'uncategorized' and cat_id not in assigned_categories:
        return "You don't have access to this category", 403

    cat = CATEGORIES[cat_id]
    subcategory = request.args.get('sub')
    current_event = request.args.get('event')

    videos = get_videos_by_category(cat_id, subcategory)

    # Group videos by event
    videos_by_event = {}
    videos_no_event = []
    event_list = []

    for video in videos:
        event_name = video.get('event', '').strip() if video.get('event') else ''
        if event_name:
            if event_name not in videos_by_event:
                videos_by_event[event_name] = []
                event_list.append(event_name)
            videos_by_event[event_name].append(video)
        else:
            videos_no_event.append(video)

    # Sort events alphabetically
    event_list.sort()

    # Get all events for autocomplete (admin only)
    events = get_all_events() if session.get('role') == 'admin' else []

    return render_template('category.html',
                         category=cat,
                         cat_id=cat_id,
                         videos=videos,
                         videos_by_event=videos_by_event,
                         videos_no_event=videos_no_event,
                         event_list=event_list,
                         current_event=current_event,
                         current_sub=subcategory,
                         is_admin=session.get('role') == 'admin',
                         all_categories=CATEGORIES,
                         events=events)


@app.route('/video/<video_id>')
def video(video_id):
    """Show single video page."""
    video = get_video(video_id)

    if not video:
        return "Video not found", 404

    # Determine video source
    # Check URL first (Supabase Storage, Dropbox, direct video URLs)
    if video.get('url') and is_direct_video_url(video.get('url', '')):
        video['video_src'] = video['url']
        video['is_local'] = False
        video['is_direct_url'] = True
    elif video.get('video_type') == 'local' and video.get('local_file'):
        video['video_src'] = f'/static/videos/{video["local_file"]}'
        video['is_local'] = True
        video['is_direct_url'] = False
    elif video.get('url'):
        video['embed_url'] = get_video_embed_url(video.get('url', ''))
        video['is_local'] = False
        video['is_direct_url'] = False
    else:
        # No valid video source
        video['video_src'] = ''
        video['is_local'] = False
        video['is_direct_url'] = False

    # Increment view count
    increment_views(video_id)

    # Get related videos from same category
    related = get_videos_by_category(video['category'])
    related_videos = [v for v in related if v['id'] != video_id][:6]

    cat = CATEGORIES.get(video['category'], {})

    # Check for competition context (when opened from competition page)
    competition_context = None
    comp_id = request.args.get('competition')
    team_id = request.args.get('team')
    round_num = request.args.get('round')

    if comp_id and team_id and round_num:
        competition = get_competition(comp_id)
        team = get_team(team_id)
        if competition and team:
            # Get current score for this round
            scores = get_team_scores(team_id)
            round_score = next((s for s in scores if s['round_num'] == int(round_num)), None)
            competition_context = {
                'competition_id': comp_id,
                'competition_name': competition['name'],
                'team_id': team_id,
                'team_name': team['team_name'],
                'team_number': team['team_number'],
                'round_num': round_num,
                'current_score': round_score.get('score') if round_score else None
            }

    # Get all users for judge selection dropdowns
    all_users = get_all_users()

    # Check if current user has an assignment for this video
    current_assignment = None
    if session.get('username'):
        user_assignments = get_assignments_for_user(session.get('username'))
        for a in user_assignments:
            if a['video_id'] == video_id:
                current_assignment = a
                break

    return render_template('video.html',
                         video=video,
                         category=cat,
                         categories=CATEGORIES,
                         related_videos=related_videos,
                         competition_context=competition_context,
                         current_assignment=current_assignment,
                         is_admin=session.get('role') == 'admin',
                         is_chief_judge=session.get('role') in ['admin', 'chief_judge'],
                         is_event_judge=session.get('role') in ['admin', 'chief_judge', 'event_judge'],
                         users=all_users)


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page."""
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').lower()
        password = request.form.get('password', '')

        user = get_user(username)

        if user and user['password'] == password:
            session['username'] = username
            session['user'] = username
            session['role'] = user['role']
            session['name'] = user['name']

            # Check if user must change password
            if user.get('must_change_password'):
                return redirect(url_for('change_password'))

            return redirect(url_for('admin_dashboard'))
        else:
            error = 'Invalid username or password'

    return render_template('login.html', error=error)


@app.route('/change-password', methods=['GET', 'POST'])
def change_password():
    """Force password change page."""
    if not session.get('username'):
        return redirect(url_for('login'))

    error = None
    if request.method == 'POST':
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')

        if len(new_password) < 6:
            error = 'Password must be at least 6 characters'
        elif new_password != confirm_password:
            error = 'Passwords do not match'
        elif new_password == 'password':
            error = 'Please choose a different password'
        else:
            # Update password and clear must_change_password flag
            user = get_user(session['username'])
            if user:
                save_user({
                    'username': user['username'],
                    'password': new_password,
                    'role': user['role'],
                    'name': user['name'],
                    'must_change_password': 0
                })
                return redirect(url_for('index'))

    return render_template('change_password.html', error=error)


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Forgot password page - sends reset email."""
    message = None
    error = None

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()

        if not email:
            error = 'Please enter your email address'
        else:
            user = get_user_by_email(email)
            if user:
                # Generate reset token
                token = secrets.token_urlsafe(32)
                password_reset_tokens[token] = {
                    'username': user['username'],
                    'expires': datetime.now() + timedelta(hours=1)
                }

                # Try to send email
                if send_reset_email(email, user['username'], token):
                    message = 'Password reset link has been sent to your email'
                else:
                    # If email not configured, show the link (for development)
                    if not SMTP_USERNAME:
                        message = f'Email not configured. Reset link: {APP_URL}/reset-password/{token}'
                    else:
                        error = 'Failed to send email. Please try again or contact admin.'
            else:
                # Don't reveal if email exists or not
                message = 'If an account with that email exists, a reset link has been sent'

    return render_template('forgot_password.html', message=message, error=error)


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Reset password using token."""
    # Check if token is valid
    token_data = password_reset_tokens.get(token)
    if not token_data or token_data['expires'] < datetime.now():
        return render_template('reset_password.html', error='Invalid or expired reset link', expired=True)

    error = None
    if request.method == 'POST':
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')

        if len(new_password) < 6:
            error = 'Password must be at least 6 characters'
        elif new_password != confirm_password:
            error = 'Passwords do not match'
        else:
            # Update password
            user = get_user(token_data['username'])
            if user:
                save_user({
                    'username': user['username'],
                    'password': new_password,
                    'role': user['role'],
                    'name': user['name'],
                    'email': user.get('email', ''),
                    'must_change_password': 0
                })
                # Remove used token
                del password_reset_tokens[token]
                return redirect(url_for('login'))

    return render_template('reset_password.html', error=error, token=token)


@app.route('/logout')
def logout():
    """Logout."""
    session.clear()
    return redirect(url_for('index'))


@app.route('/example-csv/<csv_type>')
def example_csv(csv_type):
    """Serve example CSV files for import."""
    from flask import Response

    if csv_type == 'teams':
        content = """name,class,event,team_number,members
Skydivers United,open,4 way FS,101,"John Smith, Jane Doe, Bob Wilson, Alice Brown"
Flying Aces,open,4 way FS,102,"Mike Johnson, Sarah Davis, Tom Anderson, Lisa White"
Blue Sky Team,advanced,4 way VFS,103,"Chris Martin, Emma Taylor, David Lee, Amy Chen"
Cloud Jumpers,intermediate,4 way sequential,104,"James Wilson, Mary Jones, Robert Garcia, Jennifer Miller"
Air Force One,open,8 way FS,105,"William Brown, Elizabeth Davis, Michael Moore, Patricia Taylor"
"""
        filename = 'teams_import_template.csv'
    else:  # competitors
        content = """name,class,event,number,country
John Smith,open,WS Performance,1,USA
Maria Garcia,open,WS Performance,2,ESP
Hans Mueller,open,Speed,3,GER
Yuki Tanaka,open,Speed,4,JPN
Pierre Dubois,open,Accuracy,5,FRA
Anna Kowalski,open,Accuracy,6,POL
James Wilson,open,Distance,7,GBR
Sofia Rossi,open,Distance,8,ITA
Lars Andersson,open,Canopy Piloting,9,SWE
Emma Chen,open,Canopy Piloting,10,CHN
"""
        filename = 'competitors_import_template.csv'

    return Response(
        content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@app.route('/videoupload')
@chief_judge_required
def admin_dashboard():
    """Video upload dashboard (chief judge and admin)."""
    try:
        videos = get_all_videos()
        total_videos = len(videos)
        total_views = sum(v.get('views', 0) for v in videos)
        events = get_all_events()

        # Count videos per category
        category_counts = {}
        for cat_id in CATEGORIES:
            category_counts[cat_id] = 0
        for video in videos:
            cat = video.get('category', 'uncategorized')
            if cat in category_counts:
                category_counts[cat] += 1
            else:
                category_counts['uncategorized'] = category_counts.get('uncategorized', 0) + 1
    except Exception as e:
        print(f"Admin dashboard error: {e}")
        videos = []
        total_videos = 0
        total_views = 0
        events = []
        category_counts = {}

    is_admin = session.get('role') == 'admin'

    return render_template('admin.html',
                         videos=videos,
                         categories=CATEGORIES,
                         total_videos=total_videos,
                         total_views=total_views,
                         category_counts=category_counts,
                         events=events,
                         dropbox_app_key=DROPBOX_APP_KEY,
                         is_admin=is_admin)


@app.route('/admin/users')
@admin_required
def admin_users():
    """User management page (admin only)."""
    users = get_all_users()
    return render_template('admin_users.html',
                         users=users,
                         roles=ROLES,
                         categories=CATEGORIES,
                         is_admin=True)


@app.route('/admin/user/create', methods=['POST'])
@admin_required
def admin_create_user():
    """Create a new user with default password."""
    data = request.json
    username = data.get('username', '').strip().lower()
    name = data.get('name', '').strip()
    email = data.get('email', '').strip().lower()
    role = data.get('role', 'judge')
    send_email = data.get('send_email', True)

    if not username or not name:
        return jsonify({'error': 'Username and name are required'}), 400

    if role not in ROLES:
        return jsonify({'error': 'Invalid role'}), 400

    # Check if user already exists
    existing = get_user(username)
    if existing:
        return jsonify({'error': 'Username already exists'}), 400

    # All new users get default password and must change on first login
    default_password = 'password'
    save_user({
        'username': username,
        'password': default_password,
        'role': role,
        'name': name,
        'email': email,
        'must_change_password': 1
    })

    # Send welcome email with credentials if requested
    email_sent = False
    message = 'User created with default password'

    if send_email and email:
        email_sent = send_welcome_email(email, username, default_password, name)
        if email_sent:
            message += ' - welcome email sent'
        else:
            message += ' - failed to send welcome email'
    elif send_email and not email:
        message += ' - no email provided'

    return jsonify({'success': True, 'message': message})


@app.route('/admin/user/<username>/send-credentials', methods=['POST'])
@admin_required
def admin_send_credentials(username):
    """Send login credentials email to an existing user."""
    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    email = user.get('email')
    if not email:
        return jsonify({'error': 'User has no email address'}), 400

    name = user.get('name', username)
    password = user.get('password', 'password')

    if send_welcome_email(email, username, password, name):
        return jsonify({'success': True, 'message': f'Credentials sent to {email}'})
    else:
        return jsonify({'error': 'Failed to send email'}), 500


@app.route('/admin/user/<username>/update', methods=['POST'])
@admin_required
def admin_update_user(username):
    """Update a user."""
    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    data = request.json
    name = data.get('name', user['name']).strip()
    email = data.get('email', user.get('email', '')).strip().lower()
    role = data.get('role', user['role'])
    password = data.get('password', '').strip()
    signature_pin = data.get('signature_pin', '').strip()

    if role not in ROLES:
        return jsonify({'error': 'Invalid role'}), 400

    # Validate PIN if provided
    if signature_pin and (len(signature_pin) < 4 or len(signature_pin) > 6 or not signature_pin.isdigit()):
        return jsonify({'error': 'Signature PIN must be 4-6 digits'}), 400

    # Don't allow demoting the last admin
    if user['role'] == 'admin' and role != 'admin':
        all_users = get_all_users()
        admin_count = sum(1 for u in all_users if u['role'] == 'admin')
        if admin_count <= 1:
            return jsonify({'error': 'Cannot demote the last admin'}), 400

    # Hash the signature PIN if provided
    hashed_pin = user.get('signature_pin', '')
    if signature_pin:
        import hashlib
        hashed_pin = hashlib.sha256(signature_pin.encode()).hexdigest()

    update_data = {
        'username': username,
        'name': name,
        'email': email,
        'role': role,
        'password': password if password else user['password'],
        'must_change_password': user.get('must_change_password', 0),
        'signature_pin': hashed_pin
    }

    save_user(update_data)
    return jsonify({'success': True, 'message': 'User updated'})


@app.route('/admin/user/<username>/assign-categories', methods=['POST'])
@admin_required
def admin_assign_categories(username):
    """Assign categories to a judge/user."""
    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    data = request.json
    categories = data.get('categories', [])

    # Validate categories
    valid_categories = []
    for cat in categories:
        if cat in CATEGORIES:
            valid_categories.append(cat)

    # Store as JSON string
    assigned_categories = json.dumps(valid_categories) if valid_categories else ''

    # Update user with new category assignments
    if USE_SUPABASE:
        supabase.table('users').update({'assigned_categories': assigned_categories}).eq('username', username).execute()
    else:
        db = get_sqlite_db()
        db.execute('UPDATE users SET assigned_categories = ? WHERE username = ?', (assigned_categories, username))
        db.commit()

    return jsonify({
        'success': True,
        'message': f'Assigned {len(valid_categories)} categories to {username}',
        'categories': valid_categories
    })


@app.route('/admin/users/sample-csv')
@admin_required
def admin_users_sample_csv():
    """Download a sample CSV file for user import."""
    csv_content = """username,name,email,role,signature_pin
johndoe,John Doe,john.doe@example.com,judge,
janesmith,Jane Smith,jane.smith@example.com,event_judge,
chiefjudge1,Chief Judge One,chief@example.com,chief_judge,1234
adminuser,Admin User,admin@example.com,admin,5678
judge2,Judge Two,,judge,
"""
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=users_import_sample.csv'}
    )


@app.route('/admin/users/import-csv', methods=['POST'])
@admin_required
def admin_import_users_csv():
    """Import users from CSV data."""
    data = request.json
    users_data = data.get('users', [])

    if not users_data:
        return jsonify({'error': 'No users provided'}), 400

    valid_roles = ['judge', 'event_judge', 'chief_judge', 'admin']
    created = 0
    skipped = 0
    errors = []

    for row in users_data:
        username = row.get('username', '').strip().lower()
        name = row.get('name', '').strip()
        email = row.get('email', '').strip().lower()
        role = row.get('role', '').strip().lower()
        signature_pin = row.get('signature_pin', '').strip()

        # Validate required fields
        if not username:
            errors.append(f"Row missing username")
            continue
        if not name:
            errors.append(f"User '{username}' missing name")
            continue
        if not role:
            errors.append(f"User '{username}' missing role")
            continue
        if role not in valid_roles:
            errors.append(f"User '{username}' has invalid role: {role}")
            continue

        # Check if user already exists
        existing = get_user(username)
        if existing:
            skipped += 1
            continue

        # Validate signature_pin format if provided
        if signature_pin and (not signature_pin.isdigit() or len(signature_pin) < 4 or len(signature_pin) > 6):
            errors.append(f"User '{username}' has invalid signature_pin (must be 4-6 digits)")
            signature_pin = ''

        # Create user with default password
        import hashlib
        default_password = 'password'
        password_hash = hashlib.sha256(default_password.encode()).hexdigest()

        user_data = {
            'username': username,
            'password': password_hash,
            'role': role,
            'name': name,
            'email': email,
            'must_change_password': 1,
            'signature_pin': signature_pin
        }

        try:
            if USE_SUPABASE:
                supabase.table('users').insert(user_data).execute()
            else:
                db = get_sqlite_db()
                db.execute('''INSERT INTO users (username, password, role, name, email, must_change_password, signature_pin)
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (username, password_hash, role, name, email, 1, signature_pin))
                db.commit()
            created += 1
        except Exception as e:
            errors.append(f"Failed to create user '{username}': {str(e)}")

    return jsonify({
        'success': True,
        'created': created,
        'skipped': skipped,
        'errors': errors
    })


@app.route('/admin/user/<username>/delete', methods=['POST'])
@admin_required
def admin_delete_user(username):
    """Delete a user."""
    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Don't allow deleting the last admin
    if user['role'] == 'admin':
        all_users = get_all_users()
        admin_count = sum(1 for u in all_users if u['role'] == 'admin')
        if admin_count <= 1:
            return jsonify({'error': 'Cannot delete the last admin'}), 400

    # Don't allow deleting yourself
    if username == session.get('username'):
        return jsonify({'error': 'Cannot delete your own account'}), 400

    delete_user(username)
    return jsonify({'success': True, 'message': 'User deleted'})


# Video Assignment Routes (Chief Judge+)
@app.route('/assignments')
@chief_judge_required
def assignments_page():
    """Manage video assignments (chief judge and above)."""
    videos = get_all_videos()
    users = get_all_users()
    # Allow any user to be assigned for practice judging
    judges = [u for u in users if u['role'] != 'admin']  # Exclude only admins
    assignments = get_all_assignments()

    # Enrich assignments with video and user info
    videos_dict = {v['id']: v for v in videos}
    users_dict = {u['username']: u for u in users}

    for a in assignments:
        a['video'] = videos_dict.get(a['video_id'], {})
        a['judge'] = users_dict.get(a['assigned_to'], {})
        a['assigner'] = users_dict.get(a['assigned_by'], {})

    return render_template('assignments.html',
                         videos=videos,
                         judges=judges,
                         assignments=assignments,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin')


@app.route('/assign-videos', methods=['POST'])
@chief_judge_required
def assign_videos():
    """Assign multiple videos to a judge."""
    data = request.json
    video_ids = data.get('video_ids', [])
    assigned_to = data.get('assigned_to', '')
    notes = data.get('notes', '')

    if not video_ids or not assigned_to:
        return jsonify({'error': 'Video IDs and assignee are required'}), 400

    # Verify judge exists
    judge = get_user(assigned_to)
    if not judge:
        return jsonify({'error': 'Judge not found'}), 404

    assigned_by = session.get('username')
    count = 0

    for video_id in video_ids:
        create_video_assignment(video_id, assigned_to, assigned_by, notes)
        count += 1

    return jsonify({'success': True, 'message': f'Assigned {count} video(s) to {judge["name"]}'})


@app.route('/assignment/<assignment_id>/delete', methods=['POST'])
@chief_judge_required
def delete_assignment_route(assignment_id):
    """Delete a video assignment."""
    delete_assignment(assignment_id)
    return jsonify({'success': True})


@app.route('/my-assignments')
@judge_required
def my_assignments():
    """View videos assigned to current user."""
    username = session.get('username')
    assignments = get_assignments_for_user(username)

    # Get video details for each assignment
    for a in assignments:
        video = get_video(a['video_id'])
        a['video'] = video if video else {}
        assigner = get_user(a['assigned_by'])
        a['assigner'] = assigner if assigner else {}

    return render_template('my_assignments.html',
                         assignments=assignments,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin',
                         is_chief_judge=session.get('role') in ['admin', 'chief_judge'])


@app.route('/my-assignments/competition')
@judge_required
def competition_scoresheet():
    """View assigned videos organized as a competition scoresheet by team and round."""
    import re
    username = session.get('username')
    assignments = get_assignments_for_user(username)

    # Get video details and parse team/round numbers
    teams = {}  # { team_number: { round_number: assignment } }
    all_rounds = set()

    for a in assignments:
        video = get_video(a['video_id'])
        a['video'] = video if video else {}

        # Parse team and round from video title (e.g., "226 5" = team 226, round 5)
        title = video.get('title', '') if video else ''
        numbers = re.findall(r'\d+', title)

        if len(numbers) >= 2:
            team_num = int(numbers[0])
            round_num = int(numbers[1])
        elif len(numbers) == 1:
            team_num = int(numbers[0])
            round_num = 1
        else:
            team_num = 0
            round_num = 1

        a['team_number'] = team_num
        a['round_number'] = round_num
        all_rounds.add(round_num)

        if team_num not in teams:
            teams[team_num] = {}
        teams[team_num][round_num] = a

    # Sort teams and rounds
    sorted_teams = sorted(teams.keys())
    sorted_rounds = sorted(all_rounds)

    return render_template('competition_scoresheet.html',
                         teams=teams,
                         sorted_teams=sorted_teams,
                         sorted_rounds=sorted_rounds,
                         total_assignments=len(assignments),
                         scored_count=len([a for a in assignments if a.get('status') == 'completed']),
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin',
                         is_chief_judge=session.get('role') in ['admin', 'chief_judge'])


@app.route('/assignment/<assignment_id>/status', methods=['POST'])
@judge_required
def update_assignment_status_route(assignment_id):
    """Update assignment status."""
    data = request.json
    status = data.get('status', 'pending')

    if status not in ('pending', 'in_progress', 'completed'):
        return jsonify({'error': 'Invalid status'}), 400

    update_assignment_status(assignment_id, status)
    return jsonify({'success': True})


@app.route('/assignment/<assignment_id>/score', methods=['POST'])
@judge_required
def submit_practice_score(assignment_id):
    """Submit a practice score for an assigned video."""
    data = request.json
    score = data.get('score')
    score_data = data.get('score_data', '')

    if score is None:
        return jsonify({'error': 'Score is required'}), 400

    if USE_SUPABASE:
        supabase.table('video_assignments').update({
            'practice_score': score,
            'practice_score_data': score_data,
            'scored_at': datetime.now().isoformat(),
            'status': 'completed'
        }).eq('id', assignment_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            UPDATE video_assignments
            SET practice_score = ?, practice_score_data = ?, scored_at = ?, status = 'completed'
            WHERE id = ?
        ''', (score, score_data, datetime.now().isoformat(), assignment_id))
        db.commit()

    return jsonify({'success': True})


@app.route('/assignments/report')
@chief_judge_required
def practice_scores_report():
    """Generate a report of all practice scores."""
    assignments = get_all_assignments()

    # Enrich with video and user info
    for a in assignments:
        video = get_video(a['video_id'])
        a['video'] = video if video else {}
        assigned_user = get_user(a['assigned_to'])
        a['assigned_user'] = assigned_user if assigned_user else {}
        assigner = get_user(a['assigned_by'])
        a['assigner'] = assigner if assigner else {}

    # Group by assigner (chief judge)
    grouped = {}
    for a in assignments:
        assigner = a['assigned_by']
        if assigner not in grouped:
            grouped[assigner] = {
                'assigner': a['assigner'],
                'assignments': [],
                'total': 0,
                'completed': 0
            }
        grouped[assigner]['assignments'].append(a)
        grouped[assigner]['total'] += 1
        if a.get('practice_score') is not None:
            grouped[assigner]['completed'] += 1

    return render_template('practice_report.html',
                         grouped_assignments=grouped,
                         all_assignments=assignments,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin')


@app.route('/assignments/report/csv')
@chief_judge_required
def practice_scores_csv():
    """Download practice scores as CSV."""
    assignments = get_all_assignments()

    # Build CSV
    output = "Video Title,Category,Assigned To,Assigned By,Practice Score,Score Data,Status,Scored At\n"
    for a in assignments:
        video = get_video(a['video_id'])
        video_title = video['title'] if video else 'Unknown'
        video_cat = video.get('category', '') if video else ''
        assigned_user = get_user(a['assigned_to'])
        assigned_name = assigned_user['name'] if assigned_user else a['assigned_to']
        assigner = get_user(a['assigned_by'])
        assigner_name = assigner['name'] if assigner else a['assigned_by']

        output += f'"{video_title}","{video_cat}","{assigned_name}","{assigner_name}",'
        output += f'{a.get("practice_score", "")},"{a.get("practice_score_data", "")}","{a.get("status", "")}","{a.get("scored_at", "")}"\n'

    return Response(
        output,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=practice_scores_report.csv'}
    )


def download_and_convert_video(url, video_id):
    """Download video from URL and convert to MP4 if needed."""
    import urllib.request
    import tempfile

    # Check if it's a video format that needs conversion
    url_lower = url.lower()
    needs_conversion = any(ext in url_lower for ext in CONVERSION_FORMATS)

    if not needs_conversion:
        return None, None, None  # Use URL directly

    try:
        # Detect file extension
        ext = None
        for format_ext in CONVERSION_FORMATS:
            if format_ext in url_lower:
                ext = format_ext
                break
        if not ext:
            ext = '.mts'

        # Download the file
        temp_dir = tempfile.gettempdir()
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
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Invalid request data'}), 400

        title = data.get('title', '').strip()
        description = data.get('description', '').strip()
        url = data.get('url', '').strip()
        category = data.get('category', '') or 'uncategorized'
        subcategory = data.get('subcategory', '')
        tags = data.get('tags', '').strip()
        duration = data.get('duration', '').strip()
        event = data.get('event', '').strip()

        if not title or not url:
            return jsonify({'success': False, 'error': 'Title and URL are required'}), 400

        if category not in CATEGORIES:
            category = 'uncategorized'

        video_id = str(uuid.uuid4())[:8]

        # Check if video needs conversion (MTS, AVI, etc.)
        url_lower = url.lower()
        needs_conversion = any(ext in url_lower for ext in CONVERSION_FORMATS)

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
                return jsonify({'success': False, 'error': 'Failed to convert video. Make sure ffmpeg is installed.'}), 400
        elif needs_conversion and USE_SUPABASE:
            return jsonify({'success': False, 'error': 'MTS/AVI/MKV files need to be converted to MP4 first. Convert locally or upload MP4 files to Dropbox.'}), 400

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

    except Exception as e:
        return jsonify({'success': False, 'error': f'Failed to add video: {str(e)}'}), 500


@app.route('/admin/bulk-import-urls', methods=['POST'])
@admin_required
def bulk_import_urls():
    """Bulk import videos from URLs (Dropbox, YouTube, etc.)."""
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Invalid request data'}), 400

        urls_text = data.get('urls', '').strip()
        category = data.get('category', '') or 'uncategorized'
        subcategory = data.get('subcategory', '')
        event = data.get('event', '').strip()

        if not urls_text:
            return jsonify({'success': False, 'error': 'No URLs provided'}), 400

        if category not in CATEGORIES:
            category = 'uncategorized'

        # Parse URLs (one per line, skip empty lines and comments)
        urls = []
        for line in urls_text.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)

        if not urls:
            return jsonify({'error': 'No valid URLs found'}), 400

        added = 0
        skipped = 0
        errors = []

        for url in urls:
            try:
                # Extract title from URL - NO external API calls for speed/reliability
                title = ''
                yt_meta = None
                vimeo_meta = None

                url_lower = url.lower()

                if 'dropbox.com' in url_lower or 'dropboxusercontent.com' in url_lower:
                    # Extract filename from Dropbox URL
                    import urllib.parse
                    parsed = urllib.parse.urlparse(url)
                    path = urllib.parse.unquote(parsed.path)
                    if '/' in path:
                        filename = path.split('/')[-1]
                        # Remove query params from filename
                        if '?' in filename:
                            filename = filename.split('?')[0]
                        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

                elif 'youtube.com' in url_lower or 'youtu.be' in url_lower:
                    # Extract video ID for title and thumbnail - no API call
                    yt_id = None
                    if 'youtu.be/' in url:
                        yt_id = url.split('youtu.be/')[-1].split('?')[0]
                    elif 'v=' in url:
                        yt_id = url.split('v=')[-1].split('&')[0]
                    title = f'YouTube Video {yt_id}' if yt_id else 'YouTube Video'
                    yt_meta = {'thumbnail': f'https://img.youtube.com/vi/{yt_id}/hqdefault.jpg'} if yt_id else None

                elif 'vimeo.com' in url_lower:
                    # Extract video ID for title - no API call
                    vimeo_id = url.rstrip('/').split('/')[-1].split('?')[0]
                    # Handle unlisted videos with hash
                    if '/' in vimeo_id:
                        vimeo_id = vimeo_id.split('/')[0]
                    title = f'Vimeo Video {vimeo_id}' if vimeo_id.isdigit() else 'Vimeo Video'

                else:
                    # Generic URL - try to get filename
                    import urllib.parse
                    parsed = urllib.parse.urlparse(url)
                    path = urllib.parse.unquote(parsed.path)
                    if '/' in path:
                        filename = path.split('/')[-1]
                        if '?' in filename:
                            filename = filename.split('?')[0]
                        if '.' in filename:
                            title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

                if not title:
                    title = f"Video {added + 1}"

                # Auto-detect category from title if uncategorized
                detected_cat = None
                detected_sub = None
                detected_event = None

                if category == 'uncategorized':
                    detected_cat, detected_sub, detected_event = detect_category_from_filename(title)
                    if detected_cat and detected_cat in CATEGORIES:
                        final_category = detected_cat
                        final_subcategory = detected_sub or subcategory
                    else:
                        final_category = category
                        final_subcategory = subcategory
                else:
                    final_category = category
                    final_subcategory = subcategory

                final_event = event or detected_event or ''

                # Get thumbnail and duration for YouTube/Vimeo (need duration for duplicate check)
                thumbnail = None
                duration = ''
                if 'youtube.com' in url.lower() or 'youtu.be' in url.lower():
                    # Use metadata if already fetched, otherwise get thumbnail
                    if yt_meta:
                        thumbnail = yt_meta.get('thumbnail', '')
                    else:
                        yt_id = None
                        if 'youtu.be/' in url:
                            yt_id = url.split('youtu.be/')[-1].split('?')[0]
                        elif 'v=' in url:
                            yt_id = url.split('v=')[-1].split('&')[0]
                        if yt_id:
                            thumbnail = f"https://img.youtube.com/vi/{yt_id}/hqdefault.jpg"

                elif 'vimeo.com' in url.lower():
                    # Use metadata if already fetched
                    if vimeo_meta:
                        thumbnail = vimeo_meta.get('thumbnail', '')
                        dur_seconds = vimeo_meta.get('duration', 0)
                        if dur_seconds:
                            mins = dur_seconds // 60
                            secs = dur_seconds % 60
                            duration = f"{mins}:{secs:02d}"

                # Check for duplicate (same title + duration, or same URL)
                existing = find_duplicate_video(title, duration, url)
                if existing:
                    skipped += 1
                    continue

                video_id = str(uuid.uuid4())[:8]

                save_video({
                    'id': video_id,
                    'title': title,
                    'description': '',
                    'url': url,
                    'thumbnail': thumbnail,
                    'category': final_category,
                    'subcategory': final_subcategory,
                    'tags': '',
                    'duration': duration,
                    'created_at': datetime.now().isoformat(),
                    'views': 0,
                    'video_type': 'url',
                    'local_file': '',
                    'event': final_event,
                    'category_auto': category == 'uncategorized' and detected_cat is not None
                })

                added += 1

            except Exception as e:
                errors.append(f"{url[:50]}...: {str(e)}")

        result = {
            'success': True,
            'added': added,
            'skipped': skipped,
            'total': len(urls),
            'message': f'Imported {added} videos, skipped {skipped} duplicates (of {len(urls)} total)'
        }

        if errors:
            result['errors'] = errors

        return jsonify(result)

    except Exception as e:
        return jsonify({'success': False, 'error': f'Import failed: {str(e)}'}), 500


@app.route('/admin/export-urls', methods=['GET'])
@admin_required
def export_urls():
    """Export all video URLs for copying."""
    try:
        videos = get_all_videos()
        video_list = []

        for video in videos:
            url = video.get('url', '')
            if url:
                video_list.append({
                    'id': video.get('id', ''),
                    'title': video.get('title', 'Untitled'),
                    'url': url,
                    'category': video.get('category', 'uncategorized'),
                    'subcategory': video.get('subcategory', ''),
                    'event': video.get('event', '')
                })

        return jsonify({
            'success': True,
            'videos': video_list,
            'total': len(video_list)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/admin/upload-video', methods=['POST'])
@admin_required
def upload_video():
    """Upload a video file directly."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Get form data
    title = request.form.get('title', '').strip()
    category = request.form.get('category', '') or 'uncategorized'
    subcategory = request.form.get('subcategory', '')
    event = request.form.get('event', '').strip()
    background = request.form.get('background', 'true').lower() == 'true'

    if category not in CATEGORIES:
        category = 'uncategorized'

    # Check file extension
    filename = secure_filename(file.filename)

    # Auto-detect category, subcategory, and event from filename if uncategorized
    category_auto = False  # Track if category was auto-detected
    if category == 'uncategorized' or not category:
        detected_cat, detected_sub, detected_event = detect_category_from_filename(file.filename)
        if detected_cat and detected_cat in CATEGORIES:
            category = detected_cat
            category_auto = True  # Mark as auto-categorized
            if detected_sub and not subcategory:
                subcategory = detected_sub
        if detected_event and not event:
            event = detected_event
    ext = os.path.splitext(filename)[1].lower()
    allowed_extensions = ('.mp4', '.webm', '.mov', '.m4v', '.ogg', '.ogv', '.mts', '.m2ts', '.avi', '.mkv')

    if ext not in allowed_extensions:
        return jsonify({'error': f'Invalid file type. Allowed: {", ".join(allowed_extensions)}'}), 400

    video_id = str(uuid.uuid4())[:8]

    # Generate title from filename if not provided
    if not title:
        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

    needs_conversion = ext in CONVERSION_FORMATS

    try:
        if needs_conversion and background:
            # Background conversion - save file and start thread
            import tempfile
            temp_path = os.path.join(tempfile.gettempdir(), f"{video_id}_input{ext}")
            file.save(temp_path)

            output_filename = f"{video_id}.mp4"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)

            # Create job tracking entry
            job_id = str(uuid.uuid4())[:8]
            session_id = session.get('_id', request.remote_addr)

            video_data = {
                'id': video_id,
                'title': title,
                'description': '',
                'url': '',
                'thumbnail': None,
                'category': category,
                'subcategory': subcategory,
                'tags': '',
                'duration': None,
                'created_at': datetime.now().isoformat(),
                'views': 0,
                'video_type': 'local',
                'local_file': output_filename,
                'event': event,
                'category_auto': category_auto
            }

            with conversion_lock:
                conversion_jobs[job_id] = {
                    'job_id': job_id,
                    'video_id': video_id,
                    'filename': filename,
                    'title': title,
                    'status': 'queued',
                    'progress': 0,
                    'session_id': session_id,
                    'created_at': datetime.now().isoformat(),
                    'error': None
                }

            # Start background thread
            thread = threading.Thread(
                target=background_convert_video,
                args=(job_id, temp_path, output_path, video_data, temp_path)
            )
            thread.daemon = True
            thread.start()

            return jsonify({
                'success': True,
                'background': True,
                'job_id': job_id,
                'video_id': video_id,
                'message': 'Video upload started - conversion running in background'
            })

        elif needs_conversion:
            # Synchronous conversion (legacy behavior)
            import tempfile
            temp_path = os.path.join(tempfile.gettempdir(), f"{video_id}_input{ext}")
            file.save(temp_path)

            output_filename = f"{video_id}.mp4"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)

            if convert_video_to_mp4(temp_path, output_path):
                os.remove(temp_path)
                local_file = output_filename
            else:
                os.remove(temp_path)
                return jsonify({'error': 'Failed to convert video. Make sure ffmpeg is installed.'}), 400
        else:
            # Save directly (no conversion needed)
            output_filename = f"{video_id}{ext}"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)
            file.save(output_path)
            local_file = output_filename

        # Generate thumbnail
        thumbnail_filename = f"{video_id}_thumb.jpg"
        thumbnail_path = os.path.join(VIDEOS_FOLDER, thumbnail_filename)
        if generate_thumbnail(output_path, thumbnail_path):
            thumbnail = f"/static/videos/{thumbnail_filename}"
        else:
            thumbnail = None

        # Get duration
        duration = get_video_duration(output_path)

        # Upload to Supabase Storage if enabled
        video_url = ''
        video_type = 'local'
        final_local_file = local_file

        if USE_SUPABASE:
            # Upload video file
            supabase_video_url = upload_to_supabase_storage(output_path, f"videos/{local_file}")
            if supabase_video_url:
                video_url = supabase_video_url
                video_type = 'url'
                final_local_file = ''
                # Clean up local file after upload
                if os.path.exists(output_path):
                    os.remove(output_path)

            # Upload thumbnail
            if thumbnail and os.path.exists(thumbnail_path):
                thumb_url = upload_to_supabase_storage(thumbnail_path, f"thumbnails/{thumbnail_filename}")
                if thumb_url:
                    thumbnail = thumb_url
                    os.remove(thumbnail_path)

        # Save to database
        save_video({
            'id': video_id,
            'title': title,
            'description': '',
            'url': video_url,
            'thumbnail': thumbnail,
            'category': category,
            'subcategory': subcategory,
            'tags': '',
            'duration': duration,
            'created_at': datetime.now().isoformat(),
            'views': 0,
            'video_type': video_type,
            'local_file': final_local_file,
            'event': event,
            'category_auto': category_auto
        })

        return jsonify({
            'success': True,
            'message': 'Video uploaded successfully',
            'id': video_id,
            'converted': needs_conversion
        })

    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@app.route('/admin/upload-to-s3', methods=['POST'])
@admin_required
def upload_to_s3_endpoint():
    """Upload a video file directly to AWS S3."""
    if not USE_S3:
        return jsonify({'error': 'S3 is not configured. Add AWS credentials to environment variables.'}), 400

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Get form data
    title = request.form.get('title', '').strip()
    category = request.form.get('category', '') or 'uncategorized'
    subcategory = request.form.get('subcategory', '')
    event = request.form.get('event', '').strip()
    folder_name = request.form.get('folder_name', '').strip()

    if category not in CATEGORIES:
        category = 'uncategorized'

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    allowed_extensions = ('.mp4', '.webm', '.mov', '.m4v', '.ogg', '.ogv')

    if ext not in allowed_extensions:
        return jsonify({'error': f'Invalid file type for S3. Allowed: {", ".join(allowed_extensions)}. Convert MTS/AVI locally first.'}), 400

    video_id = str(uuid.uuid4())[:8]

    # Generate title from filename if not provided
    if not title:
        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

    # Auto-detect category from folder name first, then filename
    category_auto = False
    if category == 'uncategorized' or not category:
        # Try folder name first if provided
        if folder_name:
            detected_cat, detected_sub, detected_event = detect_category_from_filename(folder_name)
            if detected_cat and detected_cat in CATEGORIES:
                category = detected_cat
                category_auto = True
                if detected_sub and not subcategory:
                    subcategory = detected_sub
                if detected_event and not event:
                    event = detected_event

        # If no category from folder, try filename
        if category == 'uncategorized' or not category:
            detected_cat, detected_sub, detected_event = detect_category_from_filename(file.filename)
            if detected_cat and detected_cat in CATEGORIES:
                category = detected_cat
                category_auto = True
                if detected_sub and not subcategory:
                    subcategory = detected_sub
            if detected_event and not event:
                event = detected_event

    try:
        # Read file data
        file_data = file.read()

        # Determine content type
        content_types = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.mov': 'video/quicktime',
            '.m4v': 'video/mp4',
            '.ogg': 'video/ogg',
            '.ogv': 'video/ogg'
        }
        content_type = content_types.get(ext, 'video/mp4')

        # Create S3 filename with category folder structure
        s3_filename = f"{video_id}{ext}"
        s3_folder = f"{category}/{subcategory}" if subcategory else category

        # Upload to S3
        video_url = upload_to_s3(file_data, s3_filename, content_type, s3_folder)

        if not video_url:
            return jsonify({'error': 'Failed to upload to S3'}), 500

        # Generate thumbnail from the uploaded video
        thumbnail_url = ''
        try:
            import tempfile

            # Save video to temp file for thumbnail generation
            temp_video = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
            temp_thumb = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            temp_video.write(file_data)
            temp_video.close()
            temp_thumb.close()

            # Generate thumbnail with ffmpeg
            if generate_thumbnail(temp_video.name, temp_thumb.name):
                # Upload thumbnail to S3
                with open(temp_thumb.name, 'rb') as f:
                    thumb_data = f.read()
                thumb_filename = f"{video_id}_thumb.jpg"
                thumbnail_url = upload_to_s3(thumb_data, thumb_filename, 'image/jpeg', 'thumbnails')

            # Clean up temp files
            try:
                os.unlink(temp_video.name)
                os.unlink(temp_thumb.name)
            except:
                pass
        except Exception as thumb_err:
            print(f"Thumbnail generation error: {thumb_err}")

        # Save video record to database
        save_video({
            'id': video_id,
            'title': title,
            'description': '',
            'url': video_url,
            'thumbnail': thumbnail_url,
            'category': category,
            'subcategory': subcategory,
            'tags': '',
            'duration': '',
            'created_at': datetime.now().isoformat(),
            'views': 0,
            'video_type': 's3',
            'local_file': '',
            'event': event,
            'category_auto': category_auto
        })

        return jsonify({
            'success': True,
            'message': 'Video uploaded to S3 successfully',
            'id': video_id,
            'url': video_url,
            'thumbnail': thumbnail_url
        })

    except Exception as e:
        return jsonify({'error': f'S3 upload failed: {str(e)}'}), 500


@app.route('/admin/s3-status', methods=['GET'])
@admin_required
def s3_status():
    """Check S3 configuration status."""
    return jsonify({
        'enabled': USE_S3,
        'bucket': AWS_S3_BUCKET if USE_S3 else None,
        'region': AWS_REGION if USE_S3 else None,
        'cloudfront': AWS_CLOUDFRONT_DOMAIN if USE_S3 and AWS_CLOUDFRONT_DOMAIN else None
    })


@app.route('/admin/migrate-to-s3', methods=['POST'])
@admin_required
def migrate_to_s3():
    """Migrate existing videos to AWS S3."""
    if not USE_S3:
        return jsonify({'error': 'S3 is not configured'}), 400

    import requests
    import socket

    videos = get_all_videos()

    # Filter videos that need migration (not already on S3, have a valid URL)
    to_migrate = []
    for v in videos:
        video_type = v.get('video_type', 'url')
        url = v.get('url', '')

        # Skip if already on S3
        if video_type == 's3':
            continue

        # Skip if already pointing to our S3 bucket
        if AWS_S3_BUCKET and AWS_S3_BUCKET in url:
            continue

        # Skip local files and invalid URLs
        if video_type == 'local' or not url or not url.startswith('http'):
            continue

        to_migrate.append(v)

    if not to_migrate:
        return jsonify({'success': True, 'message': 'No videos to migrate', 'migrated': 0, 'failed': 0})

    migrated = 0
    failed = 0
    errors = []

    # Set a shorter timeout for downloads
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)

    try:
        for video in to_migrate:
            video_id = video.get('id')
            url = video.get('url', '')
            title = video.get('title', '')
            category = video.get('category', 'uncategorized')
            subcategory = video.get('subcategory', '')

            try:
                # Download video from URL
                print(f"[S3 MIGRATE] Downloading: {title} ({video_id})")

                # Skip streaming service URLs that can't be downloaded
                if 'vimeo.com' in url or 'youtube.com' in url or 'youtu.be' in url:
                    errors.append(f"{title}: Streaming service videos cannot be migrated")
                    failed += 1
                    continue

                # Download the video
                response = requests.get(download_url, timeout=60, stream=True)
                response.raise_for_status()

                # Check content type
                content_type = response.headers.get('Content-Type', 'video/mp4')
                if 'video' not in content_type and 'octet-stream' not in content_type:
                    errors.append(f"{title}: Not a video file (content-type: {content_type})")
                    failed += 1
                    continue

                # Read file data
                file_data = response.content

                # Determine file extension
                ext = '.mp4'
                if 'webm' in content_type:
                    ext = '.webm'
                elif 'quicktime' in content_type or 'mov' in url.lower():
                    ext = '.mov'
                elif '.mov' in url.lower():
                    ext = '.mov'
                elif '.webm' in url.lower():
                    ext = '.webm'

                # Create S3 filename
                s3_filename = f"{video_id}{ext}"
                s3_folder = f"{category}/{subcategory}" if subcategory else category

                # Upload to S3
                print(f"[S3 MIGRATE] Uploading to S3: {s3_folder}/{s3_filename}")
                new_url = upload_to_s3(file_data, s3_filename, content_type if 'video' in content_type else 'video/mp4', s3_folder)

                if not new_url:
                    errors.append(f"{title}: S3 upload failed")
                    failed += 1
                    continue

                # Update database
                video['url'] = new_url
                video['video_type'] = 's3'
                save_video(video)

                print(f"[S3 MIGRATE] Success: {title}")
                migrated += 1

            except requests.exceptions.Timeout:
                errors.append(f"{title}: Download timeout")
                failed += 1
            except requests.exceptions.RequestException as e:
                errors.append(f"{title}: Download failed - {str(e)}")
                failed += 1
            except Exception as e:
                errors.append(f"{title}: {str(e)}")
                failed += 1

    finally:
        socket.setdefaulttimeout(old_timeout)

    return jsonify({
        'success': True,
        'message': f'Migration complete: {migrated} migrated, {failed} failed',
        'migrated': migrated,
        'failed': failed,
        'total': len(to_migrate),
        'errors': errors[:20]  # Limit errors returned
    })


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
        user_category = 'uncategorized'

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
            final_category = user_category or file_meta['category'] or 'uncategorized'
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


@app.route('/admin/auto-categorize', methods=['POST'])
@admin_required
def auto_categorize_videos():
    """Auto-categorize uncategorized videos based on their filenames."""
    data = request.json or {}
    only_uncategorized = data.get('only_uncategorized', True)
    admin_pin = data.get('admin_pin', '')

    # Require PIN to process ALL files (not just uncategorized)
    if not only_uncategorized:
        if admin_pin != ADMIN_PIN:
            return jsonify({'error': 'Invalid admin PIN. Required for processing all files.'}), 403

    videos = get_all_videos()
    updated = 0
    skipped = 0
    skipped_manual = 0
    details = []

    for video in videos:
        current_cat = video.get('category', 'uncategorized')
        is_uncategorized = current_cat in ('uncategorized', '', None)
        was_auto_categorized = video.get('category_auto', True)  # Default True for backwards compat

        # Skip logic:
        # - If only_uncategorized: skip anything that's not uncategorized
        # - If processing all: skip manually categorized videos (category_auto = False)
        if only_uncategorized:
            if not is_uncategorized:
                continue
        else:
            # Processing all files - but skip manually categorized ones
            if not is_uncategorized and not was_auto_categorized:
                skipped_manual += 1
                continue

        # Get filename from title or local_file
        filename = video.get('local_file') or video.get('title') or ''

        if not filename:
            skipped += 1
            continue

        # Detect category, subcategory, and event
        detected_cat, detected_sub, detected_event = detect_category_from_filename(filename)

        # Also try the title if local_file didn't give results
        if not detected_cat and video.get('title'):
            detected_cat, detected_sub, detected_event = detect_category_from_filename(video.get('title'))

        changes = {}
        change_desc = []

        # Update category if detected and different
        if detected_cat and detected_cat in CATEGORIES:
            if is_uncategorized or (not only_uncategorized and was_auto_categorized):
                if detected_cat != current_cat:
                    changes['category'] = detected_cat
                    changes['category_auto'] = True  # Mark as auto-categorized
                    change_desc.append(f"category: {current_cat} → {detected_cat}")

        # Update subcategory if detected and not already set
        if detected_sub and not video.get('subcategory'):
            changes['subcategory'] = detected_sub
            change_desc.append(f"subcategory: {detected_sub}")

        # Update event if detected and not already set
        if detected_event and not video.get('event'):
            changes['event'] = detected_event
            change_desc.append(f"event: {detected_event}")

        if changes:
            # Update the video
            video_id = video.get('id')
            if USE_SUPABASE:
                supabase.table('videos').update(changes).eq('id', video_id).execute()
            else:
                db = get_sqlite_db()
                set_clause = ', '.join(f"{k} = ?" for k in changes.keys())
                values = list(changes.values()) + [video_id]
                db.execute(f"UPDATE videos SET {set_clause} WHERE id = ?", values)
                db.commit()

            updated += 1
            details.append({
                'id': video_id,
                'title': video.get('title', filename),
                'changes': change_desc
            })
        else:
            skipped += 1

    msg = f"Updated {updated} video(s), skipped {skipped}"
    if skipped_manual > 0:
        msg += f", preserved {skipped_manual} manually categorized"

    return jsonify({
        'success': True,
        'message': msg,
        'updated': updated,
        'skipped': skipped,
        'skipped_manual': skipped_manual,
        'details': details[:50]  # Limit details to first 50
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
    try:
        video = get_video(video_id)
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        # Ensure all values are JSON serializable
        safe_video = {k: (str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v)
                      for k, v in video.items()}
        return jsonify(safe_video)
    except Exception as e:
        return jsonify({'error': f'Error loading video: {str(e)}'}), 500


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


@app.route('/admin/browse-folders', methods=['GET'])
@admin_required
def browse_folders():
    """Browse local folders (local development only)."""
    if USE_SUPABASE:
        return jsonify({'error': 'Folder browsing not available in production'}), 400

    path = request.args.get('path', os.path.expanduser('~'))

    try:
        if not os.path.isdir(path):
            path = os.path.expanduser('~')

        items = []
        # Add parent directory
        parent = os.path.dirname(path)
        if parent and parent != path:
            items.append({'name': '..', 'path': parent, 'is_dir': True})

        # List directory contents
        for name in sorted(os.listdir(path)):
            full_path = os.path.join(path, name)
            if os.path.isdir(full_path) and not name.startswith('.'):
                items.append({'name': name, 'path': full_path, 'is_dir': True})

        # Count video files in current folder
        video_extensions = ('.mp4', '.mts', '.m2ts', '.mov', '.avi', '.mkv', '.webm')
        video_count = len([f for f in os.listdir(path) if f.lower().endswith(video_extensions)])

        return jsonify({
            'current_path': path,
            'items': items,
            'video_count': video_count
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/delete-vimeo-videos', methods=['POST'])
@admin_required
def delete_vimeo_videos():
    """Delete all Vimeo videos from the database."""
    deleted = 0
    try:
        if USE_SUPABASE:
            # Get all Vimeo videos
            result = supabase.table('videos').select('id, url').execute()
            for video in result.data:
                if video.get('url') and 'vimeo.com' in video['url']:
                    supabase.table('videos').delete().eq('id', video['id']).execute()
                    deleted += 1
        else:
            db = get_sqlite_db()
            cursor = db.execute("SELECT COUNT(*) FROM videos WHERE url LIKE '%vimeo.com%'")
            deleted = cursor.fetchone()[0]
            db.execute("DELETE FROM videos WHERE url LIKE '%vimeo.com%'")
            db.commit()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({'success': True, 'message': f'Deleted {deleted} Vimeo videos', 'deleted': deleted})


@app.route('/admin/fix-duplicates', methods=['POST'])
@admin_required
def fix_duplicates():
    """Remove duplicate videos (same URL or same title+duration)."""
    removed = 0
    try:
        if USE_SUPABASE:
            result = supabase.table('videos').select('*').execute()
            videos = result.data
        else:
            db = get_sqlite_db()
            cursor = db.execute('SELECT * FROM videos')
            videos = [dict(row) for row in cursor.fetchall()]

        # Track seen videos by URL and by title+duration
        seen_urls = {}
        seen_title_duration = {}
        duplicates_to_remove = []

        for video in videos:
            video_id = video['id']
            url = video.get('url', '')
            title = video.get('title', '')
            duration = video.get('duration', '')

            is_duplicate = False

            # Check for URL duplicate
            if url and url in seen_urls:
                is_duplicate = True
            elif url:
                seen_urls[url] = video_id

            # Check for title+duration duplicate (only if duration exists)
            if not is_duplicate and title and duration:
                key = f"{title}|{duration}"
                if key in seen_title_duration:
                    is_duplicate = True
                else:
                    seen_title_duration[key] = video_id

            if is_duplicate:
                duplicates_to_remove.append(video_id)

        # Remove duplicates
        for video_id in duplicates_to_remove:
            if USE_SUPABASE:
                supabase.table('videos').delete().eq('id', video_id).execute()
            else:
                db.execute('DELETE FROM videos WHERE id = ?', (video_id,))
            removed += 1

        if not USE_SUPABASE:
            db.commit()

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({
        'success': True,
        'removed': removed,
        'message': f'Removed {removed} duplicate videos'
    })


def extract_title_pattern(title):
    """Extract a pattern from a video title for matching similar videos.

    Examples:
    - "4-Way FS Round 1 Team Alpha" -> matches "4-Way FS Round 2 Team Alpha"
    - "Team Beta - Rd 3 - 4way" -> matches "Team Beta - Rd 1 - 4way"
    - "Nationals 2024 - FS Open - Jump 5" -> matches "Nationals 2024 - FS Open - Jump 1"
    """
    import re

    # Normalize the title
    normalized = title.lower().strip()

    # Replace round/jump numbers with a placeholder
    # Match patterns like: round 1, rd 1, r1, jump 1, j1, #1, etc.
    pattern = re.sub(r'\b(round|rd|r|jump|j|draw|d|#)\s*(\d+)\b', r'\1 {NUM}', normalized, flags=re.IGNORECASE)
    pattern = re.sub(r'\b(\d+)\s*(round|rd|r|jump|j|draw|d)\b', r'{NUM} \2', pattern, flags=re.IGNORECASE)

    # Also replace standalone numbers that look like round numbers (1-20)
    pattern = re.sub(r'\b([1-9]|1[0-9]|20)\b', '{NUM}', pattern)

    return pattern


def find_similar_uncategorized_videos(title, exclude_id=None):
    """Find uncategorized videos with similar title patterns."""
    pattern = extract_title_pattern(title)
    if not pattern or pattern == title.lower():
        return []

    all_videos = get_all_videos()
    similar = []

    for video in all_videos:
        # Skip if not uncategorized or is the same video
        if video.get('category') != 'uncategorized':
            continue
        if exclude_id and video.get('id') == exclude_id:
            continue

        # Check if this video's title matches the pattern
        video_pattern = extract_title_pattern(video.get('title', ''))
        if video_pattern == pattern:
            similar.append(video)

    return similar


@app.route('/admin/edit-video/<video_id>', methods=['POST'])
@admin_required
def edit_video(video_id):
    """Edit a video."""
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'No data provided'}), 400
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {str(e)}'}), 400

    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    old_category = video.get('category', 'uncategorized')
    new_category = data.get('category')
    new_subcategory = data.get('subcategory', '')

    # Check if category is being manually changed
    if new_category and new_category != old_category:
        video['category_auto'] = False  # Mark as manually categorized

    video['title'] = data.get('title', video['title']).strip()
    video['description'] = data.get('description', video.get('description', '')).strip()
    video['category'] = data.get('category', video['category'])
    video['subcategory'] = data.get('subcategory', video.get('subcategory', ''))
    video['tags'] = data.get('tags', video.get('tags', '')).strip()
    video['duration'] = data.get('duration', video.get('duration', '')).strip()
    video['event'] = data.get('event', video.get('event', '')).strip()

    save_video(video)

    # Auto-move similar videos if moving from uncategorized to a category
    auto_moved = 0
    if old_category == 'uncategorized' and new_category and new_category != 'uncategorized':
        similar_videos = find_similar_uncategorized_videos(video['title'], exclude_id=video_id)
        for similar in similar_videos:
            similar['category'] = new_category
            similar['subcategory'] = new_subcategory
            similar['category_auto'] = False  # Mark as manually categorized
            # Also copy the event if set
            if video.get('event'):
                similar['event'] = video['event']
            save_video(similar)
            auto_moved += 1

    if auto_moved > 0:
        return jsonify({
            'success': True,
            'message': f'Video updated. Also moved {auto_moved} similar video(s) to the same category.',
            'auto_moved': auto_moved
        })

    return jsonify({'success': True, 'message': 'Video updated'})


@app.route('/admin/delete-score/<team_id>/<score_id>', methods=['DELETE'])
@admin_required
def delete_score(team_id, score_id):
    """Delete a score for a team."""
    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    scores = team.get('scores', [])

    # Find and remove the score
    score_found = False
    new_scores = []
    deleted_score = None

    for score in scores:
        if score.get('id') == score_id:
            score_found = True
            deleted_score = score
        else:
            new_scores.append(score)

    if not score_found:
        return jsonify({'error': 'Score not found'}), 404

    team['scores'] = new_scores
    save_team(team)

    return jsonify({
        'success': True,
        'message': f'Score deleted for round {deleted_score.get("round_num", "unknown")}',
        'deleted_score': deleted_score
    })


@app.route('/admin/bulk-move-videos', methods=['POST'])
@admin_required
def bulk_move_videos():
    """Move multiple videos to a new category at once."""
    try:
        data = request.json or {}
        video_ids = data.get('video_ids', [])
        new_category = data.get('category', '')
        new_subcategory = data.get('subcategory', '')

        if not video_ids:
            return jsonify({'error': 'No videos selected'}), 400
        if not new_category:
            return jsonify({'error': 'No category specified'}), 400

        # Limit batch size to avoid memory issues
        if len(video_ids) > 50:
            video_ids = video_ids[:50]

        if USE_SUPABASE:
            # Single batch update for all videos using IN filter
            supabase.table('videos').update({
                'category': new_category,
                'subcategory': new_subcategory
            }).in_('id', video_ids).execute()
        else:
            db = get_sqlite_db()
            placeholders = ','.join('?' * len(video_ids))
            db.execute(f'UPDATE videos SET category = ?, subcategory = ? WHERE id IN ({placeholders})',
                      [new_category, new_subcategory] + video_ids)
            db.commit()

        return jsonify({
            'success': True,
            'message': f'Moved {len(video_ids)} video(s)',
            'moved_count': len(video_ids)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/bulk-set-event', methods=['POST'])
@admin_required
def bulk_set_event():
    """Set event name for multiple videos at once."""
    data = request.json
    video_ids = data.get('video_ids', [])
    event_name = data.get('event', '').strip()

    if not video_ids:
        return jsonify({'error': 'No videos selected'}), 400

    if not event_name:
        return jsonify({'error': 'No event name specified'}), 400

    success_count = 0
    for video_id in video_ids:
        video = get_video(video_id)
        if video:
            video['event'] = event_name
            save_video(video)
            success_count += 1

    return jsonify({
        'success': True,
        'message': f'Set event "{event_name}" for {success_count} video(s)',
        'updated_count': success_count
    })


def get_ffmpeg_path():
    """Get path to ffmpeg binary - checks custom location first, then system."""
    custom_path = '/opt/render/project/src/bin/ffmpeg'
    if os.path.exists(custom_path):
        return custom_path
    return 'ffmpeg'  # Use system ffmpeg


def generate_thumbnail_from_s3_video(video_url, video_id):
    """Generate thumbnail from S3 video URL using ffmpeg (streams directly, no full download).
    Returns (thumbnail_url, error_message) tuple."""
    import tempfile

    if not USE_S3:
        return None, "S3 not configured"

    temp_thumb = None
    ffmpeg = get_ffmpeg_path()

    try:
        # Create temp file for thumbnail
        temp_thumb = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        temp_thumb.close()

        # Use ffmpeg to read directly from URL (streams only what's needed)
        print(f"[THUMB] Generating thumbnail for {video_id} using {ffmpeg}...")
        print(f"[THUMB] URL: {video_url[:100]}...")
        result = subprocess.run([
            ffmpeg, '-y',
            '-ss', '2',  # Seek to 2 seconds BEFORE opening (faster)
            '-i', video_url,
            '-vframes', '1',
            '-vf', 'scale=320:-1',
            temp_thumb.name
        ], capture_output=True, timeout=30)

        stderr = result.stderr.decode()
        print(f"[THUMB] Return code: {result.returncode}, stderr length: {len(stderr)}")

        if result.returncode != 0:
            # Look for actual error lines (not version info)
            err_lines = [l for l in stderr.split('\n') if 'error' in l.lower() or 'invalid' in l.lower() or 'denied' in l.lower() or 'failed' in l.lower()]
            if err_lines:
                err = '; '.join(err_lines)[:200]
            else:
                err = stderr[-300:] if len(stderr) > 300 else stderr
            print(f"[THUMB] FFmpeg error for {video_id}: {err}")
            return None, f"{err}"

        # Check if thumbnail was created
        file_exists = os.path.exists(temp_thumb.name)
        file_size = os.path.getsize(temp_thumb.name) if file_exists else 0
        print(f"[THUMB] File exists: {file_exists}, size: {file_size}")

        if not file_exists or file_size == 0:
            print(f"[THUMB] Thumbnail file not created or empty for {video_id}")
            # Return more of stderr for debugging
            return None, f"Empty output. stderr: {stderr[-500:]}"

        # Upload thumbnail to S3
        with open(temp_thumb.name, 'rb') as f:
            thumb_data = f.read()

        thumb_filename = f"{video_id}_thumb.jpg"
        thumb_url = upload_to_s3(thumb_data, thumb_filename, 'image/jpeg', 'thumbnails')

        if thumb_url:
            print(f"[THUMB] Uploaded: {thumb_url}")
            return thumb_url, None

        return None, "S3 upload failed"

    except subprocess.TimeoutExpired:
        print(f"[THUMB] Timeout for {video_id}")
        return None, "Timeout"
    except Exception as e:
        print(f"[THUMB] Error for {video_id}: {e}")
        return None, str(e)
    finally:
        # Clean up temp file
        try:
            if temp_thumb and os.path.exists(temp_thumb.name):
                os.unlink(temp_thumb.name)
        except:
            pass


@app.route('/admin/test-thumbnail', methods=['POST'])
@admin_required
def test_thumbnail():
    """Test thumbnail generation with a single video - returns full debug info."""
    ffmpeg = get_ffmpeg_path()

    # Get one video without thumbnail
    result = supabase.table('videos').select('id, url, thumbnail').execute()
    videos = [v for v in (result.data or []) if not v.get('thumbnail')]

    if not videos:
        return jsonify({'error': 'No videos without thumbnails'})

    video = videos[0]
    video_url = video.get('url', '')
    video_id = video.get('id')

    import tempfile
    temp_thumb = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
    temp_thumb.close()

    # Check ffmpeg protocols
    proto_result = subprocess.run([ffmpeg, '-protocols'], capture_output=True, timeout=10)
    protocols = proto_result.stdout.decode() if proto_result.returncode == 0 else "Failed to get protocols"

    # Try to generate thumbnail
    cmd = [ffmpeg, '-y', '-ss', '2', '-i', video_url, '-vframes', '1', '-vf', 'scale=320:-1', temp_thumb.name]
    result = subprocess.run(cmd, capture_output=True, timeout=60)

    stdout = result.stdout.decode()
    stderr = result.stderr.decode()

    # Check file
    file_exists = os.path.exists(temp_thumb.name)
    file_size = os.path.getsize(temp_thumb.name) if file_exists else 0

    # Cleanup
    try:
        os.unlink(temp_thumb.name)
    except:
        pass

    return jsonify({
        'video_id': video_id,
        'video_url': video_url,
        'ffmpeg_path': ffmpeg,
        'has_https': 'https' in protocols.lower(),
        'command': ' '.join(cmd[:6]) + '... ' + cmd[-1],
        'return_code': result.returncode,
        'file_created': file_exists,
        'file_size': file_size,
        'stderr_length': len(stderr),
        'stderr_last_500': stderr[-500:] if stderr else '',
        'stdout': stdout[:200] if stdout else ''
    })


@app.route('/admin/refresh-thumbnails', methods=['POST'])
@admin_required
def refresh_thumbnails():
    """Generate thumbnails for S3 videos that are missing them."""
    if not USE_SUPABASE:
        return jsonify({'error': 'This feature requires Supabase'}), 400

    if not USE_S3:
        return jsonify({'error': 'This feature requires S3 to be configured'}), 400

    # Check if ffmpeg is available
    ffmpeg = get_ffmpeg_path()
    try:
        result = subprocess.run([ffmpeg, '-version'], capture_output=True, timeout=5)
        if result.returncode != 0:
            return jsonify({'error': f'ffmpeg not available at {ffmpeg}'}), 500
    except FileNotFoundError:
        return jsonify({'error': f'ffmpeg not found at {ffmpeg}. Run build.sh first.'}), 500
    except Exception as e:
        return jsonify({'error': f'ffmpeg check failed: {str(e)}'}), 500

    try:
        # Get all videos with missing thumbnails
        result = supabase.table('videos').select('id, url, thumbnail').execute()
        videos = result.data or []

        missing_thumbs = [v for v in videos if not v.get('thumbnail')]

        if not missing_thumbs:
            return jsonify({'success': True, 'message': 'All videos already have thumbnails', 'updated': 0})

        # Process 10 videos per request
        batch = missing_thumbs[:10]
        updated = 0
        errors = []

        for video in batch:
            url = video.get('url', '')
            video_id = video.get('id')

            if not url:
                errors.append(f"{video_id}: No URL")
                continue

            thumbnail = None
            err_msg = None

            # S3/CloudFront videos - generate thumbnail
            if 's3.' in url or 'cloudfront' in url or (AWS_S3_BUCKET and AWS_S3_BUCKET in url):
                thumbnail, err_msg = generate_thumbnail_from_s3_video(url, video_id)
                if not thumbnail:
                    errors.append(f"{video_id}: {err_msg or 'Unknown error'}")
            # Vimeo
            elif 'vimeo.com' in url:
                meta = fetch_vimeo_metadata(url)
                thumbnail = meta.get('thumbnail', '')
            # YouTube
            elif 'youtube.com' in url or 'youtu.be' in url:
                meta = fetch_youtube_metadata(url)
                thumbnail = meta.get('thumbnail', '')
            else:
                errors.append(f"{video_id}: Unknown source - {url[:50]}")
                continue

            if thumbnail:
                try:
                    supabase.table('videos').update({'thumbnail': thumbnail}).eq('id', video_id).execute()
                    updated += 1
                except Exception as e:
                    errors.append(f"{video_id}: DB error - {str(e)}")

        remaining = len(missing_thumbs) - len(batch)
        msg = f'Generated {updated} thumbnails.'
        if remaining > 0:
            msg += f' {remaining} videos remaining - click again to continue.'

        return jsonify({
            'success': True,
            'message': msg,
            'updated': updated,
            'processed': len(batch),
            'total_missing': len(missing_thumbs),
            'remaining': remaining,
            'errors': errors[:10] if errors else []
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/admin/rename-event-folder', methods=['POST'])
@admin_required
def rename_event_folder():
    """Rename an event folder (updates all videos with that event name)."""
    data = request.json
    old_name = data.get('old_name', '').strip()
    new_name = data.get('new_name', '').strip()
    category = data.get('category', '')
    subcategory = data.get('subcategory', '')

    if not old_name or not new_name:
        return jsonify({'error': 'Both old and new names are required'}), 400

    if old_name == new_name:
        return jsonify({'error': 'New name must be different'}), 400

    # Get all videos in this category/subcategory with the old event name
    if subcategory:
        videos = get_videos_by_category(category, subcategory)
    else:
        videos = get_videos_by_category(category)

    # Filter to only videos with the old event name
    videos_to_update = [v for v in videos if v.get('event', '') == old_name]

    if not videos_to_update:
        return jsonify({'error': f'No videos found with event "{old_name}"'}), 404

    # Update all matching videos
    success_count = 0
    for video in videos_to_update:
        video['event'] = new_name
        save_video(video)
        success_count += 1

    return jsonify({
        'success': True,
        'message': f'Renamed folder to "{new_name}" ({success_count} videos updated)',
        'updated_count': success_count
    })


@app.route('/api/video/<video_id>/set-start-time', methods=['POST'])
def set_video_start_time(video_id):
    """Set the start time for a video (videographer/judge access)."""
    data = request.json

    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    start_time = float(data.get('start_time', 0))
    if start_time < 0:
        start_time = 0

    # Update start_time directly in database
    if USE_SUPABASE:
        try:
            supabase.table('videos').update({'start_time': start_time}).eq('id', video_id).execute()
        except Exception as e:
            error_msg = str(e)
            if 'start_time' in error_msg and 'column' in error_msg:
                return jsonify({'error': 'Please add start_time column (type: float8) to your Supabase videos table'}), 400
            raise
    else:
        db = get_sqlite_db()
        db.execute('UPDATE videos SET start_time = ? WHERE id = ?', (start_time, video_id))
        db.commit()

    return jsonify({'success': True, 'message': 'Start time saved', 'start_time': start_time})


@app.route('/api/videos-by-event')
def api_videos_by_event():
    """Get all videos in a specific event/folder."""
    event = request.args.get('event', '').strip()
    if not event:
        return jsonify({'success': False, 'error': 'Event name required'}), 400

    all_videos = get_all_videos()
    videos = [v for v in all_videos if v.get('event', '') == event]

    return jsonify({
        'success': True,
        'videos': videos,
        'total': len(videos)
    })


@app.route('/video/<video_id>/draw', methods=['GET'])
def get_video_draw(video_id):
    """Get the draw (formation sequence) for a video."""
    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    draw = video.get('draw', [])
    if isinstance(draw, str):
        try:
            import json
            draw = json.loads(draw)
        except:
            draw = []

    return jsonify({'success': True, 'draw': draw})


@app.route('/video/<video_id>/draw', methods=['POST'])
@login_required
def save_video_draw(video_id):
    """Save the draw (formation sequence) for a video. Chief judge or admin only."""
    if session.get('role') not in ['admin', 'chief_judge']:
        return jsonify({'error': 'Permission denied'}), 403

    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    data = request.json
    draw = data.get('draw', [])

    # Store as JSON string
    import json
    draw_json = json.dumps(draw)

    # Update draw directly in database
    if USE_SUPABASE:
        try:
            supabase.table('videos').update({'draw': draw_json}).eq('id', video_id).execute()
        except Exception as e:
            error_msg = str(e)
            if 'draw' in error_msg and 'column' in error_msg:
                return jsonify({'error': 'Please add draw column (type: text) to your Supabase videos table'}), 400
            raise
    else:
        db = get_sqlite_db()
        db.execute('UPDATE videos SET draw = ? WHERE id = ?', (draw_json, video_id))
        db.commit()

    return jsonify({'success': True, 'message': 'Draw saved'})


@app.route('/search')
def search():
    """Search videos."""
    import re as re_module
    query = request.args.get('q', '').strip()

    if not query:
        return redirect(url_for('index'))

    videos = search_videos(query)

    # If query contains a number (team search), sort by team number then round
    if re_module.search(r'\d', query):
        def parse_team_round(title):
            nums = re_module.findall(r'\d+', title or '')
            if len(nums) >= 2:
                return (int(nums[0]), int(nums[1]))
            elif len(nums) == 1:
                return (int(nums[0]), 0)
            return (999999, 999999)

        videos.sort(key=lambda v: parse_team_round(v.get('title', '')))

    return render_template('search.html',
                         query=query,
                         videos=videos,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin',
                         is_chief_judge=session.get('role') in ['admin', 'chief_judge'])


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


# Draw generators
@app.route('/draw-generator')
def draw_generator():
    """Draw generator page for creating competition draws based on USPA rules."""
    return render_template('draw_generator.html')


# Competition routes
@app.route('/competitions')
@chief_judge_required
def competitions_list():
    """Show all competitions (chief judge and above)."""
    try:
        competitions = get_all_competitions()

        # Parse event_types for each competition for display
        for comp in competitions:
            if comp.get('event_types'):
                try:
                    comp['parsed_event_types'] = json.loads(comp['event_types'])
                except:
                    comp['parsed_event_types'] = [comp.get('event_type', 'fs')]
            else:
                comp['parsed_event_types'] = [comp.get('event_type', 'fs')]

        return render_template('competitions.html',
                             competitions=competitions,
                             categories=CATEGORIES,
                             is_admin=session.get('role') == 'admin')
    except Exception as e:
        print(f"Error in competitions_list: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading competitions: {str(e)}", 500


# Practice Competitions Routes
@app.route('/practice-competitions')
@chief_judge_required
def practice_competitions_list():
    """Show all practice competitions (chief judge only)."""
    practice_comps = get_all_practice_competitions()
    judges = get_all_users()
    judges = [u for u in judges if u.get('role') in ['judge', 'event_judge']]

    return render_template('practice_competitions.html',
                         practice_competitions=practice_comps,
                         judges=judges,
                         categories=CATEGORIES,
                         is_admin=session.get('role') == 'admin')


@app.route('/practice-competition/create', methods=['POST'])
@chief_judge_required
def create_practice_competition():
    """Create a new practice competition."""
    data = request.json
    name = data.get('name', '').strip()
    category = data.get('category', '').strip()

    if not name:
        return jsonify({'error': 'Name is required'}), 400
    if not category:
        return jsonify({'error': 'Category is required'}), 400

    practice_id = str(uuid.uuid4())
    practice_data = {
        'id': practice_id,
        'name': name,
        'category': category,
        'created_by': session.get('username'),
        'created_at': datetime.now().isoformat(),
        'status': 'active'
    }

    if USE_SUPABASE:
        supabase.table('practice_competitions').insert(practice_data).execute()
    else:
        db = get_sqlite_db()
        db.execute('''
            INSERT INTO practice_competitions (id, name, category, created_by, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (practice_data['id'], practice_data['name'], practice_data['category'],
              practice_data['created_by'], practice_data['created_at'], practice_data['status']))
        db.commit()

    return jsonify({'success': True, 'id': practice_id})


@app.route('/practice-competition/<practice_id>/assign', methods=['POST'])
@chief_judge_required
def assign_judge_to_practice(practice_id):
    """Assign a judge to a practice competition."""
    data = request.json
    judge_username = data.get('judge_username', '').strip()

    if not judge_username:
        return jsonify({'error': 'Judge username is required'}), 400

    # Check if already assigned
    if USE_SUPABASE:
        existing = supabase.table('practice_assignments').select('id').eq('practice_id', practice_id).eq('judge_username', judge_username).execute()
        if existing.data:
            return jsonify({'error': 'Judge already assigned'}), 400

        assignment_id = str(uuid.uuid4())
        supabase.table('practice_assignments').insert({
            'id': assignment_id,
            'practice_id': practice_id,
            'judge_username': judge_username,
            'assigned_at': datetime.now().isoformat()
        }).execute()
    else:
        db = get_sqlite_db()
        existing = db.execute('SELECT id FROM practice_assignments WHERE practice_id = ? AND judge_username = ?',
                             (practice_id, judge_username)).fetchone()
        if existing:
            return jsonify({'error': 'Judge already assigned'}), 400

        assignment_id = str(uuid.uuid4())
        db.execute('''
            INSERT INTO practice_assignments (id, practice_id, judge_username, assigned_at)
            VALUES (?, ?, ?, ?)
        ''', (assignment_id, practice_id, judge_username, datetime.now().isoformat()))
        db.commit()

    return jsonify({'success': True})


@app.route('/practice-competition/<practice_id>/unassign', methods=['POST'])
@chief_judge_required
def unassign_judge_from_practice(practice_id):
    """Remove a judge from a practice competition."""
    data = request.json
    judge_username = data.get('judge_username', '').strip()

    if not judge_username:
        return jsonify({'error': 'Judge username is required'}), 400

    if USE_SUPABASE:
        supabase.table('practice_assignments').delete().eq('practice_id', practice_id).eq('judge_username', judge_username).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM practice_assignments WHERE practice_id = ? AND judge_username = ?',
                  (practice_id, judge_username))
        db.commit()

    return jsonify({'success': True})


@app.route('/practice-competition/<practice_id>/delete', methods=['POST'])
@chief_judge_required
def delete_practice_competition(practice_id):
    """Delete a practice competition."""
    if USE_SUPABASE:
        supabase.table('practice_assignments').delete().eq('practice_id', practice_id).execute()
        supabase.table('practice_competitions').delete().eq('id', practice_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('DELETE FROM practice_assignments WHERE practice_id = ?', (practice_id,))
        db.execute('DELETE FROM practice_competitions WHERE id = ?', (practice_id,))
        db.commit()

    return jsonify({'success': True})


def get_all_practice_competitions():
    """Get all practice competitions with assigned judges."""
    if USE_SUPABASE:
        result = supabase.table('practice_competitions').select('*').order('created_at', desc=True).execute()
        practice_comps = result.data or []

        # Get assignments for each practice competition
        for pc in practice_comps:
            assignments = supabase.table('practice_assignments').select('judge_username').eq('practice_id', pc['id']).execute()
            pc['assigned_judges'] = [a['judge_username'] for a in (assignments.data or [])]
    else:
        db = get_sqlite_db()
        practice_comps = [dict(row) for row in db.execute(
            'SELECT * FROM practice_competitions ORDER BY created_at DESC').fetchall()]

        for pc in practice_comps:
            assignments = db.execute('SELECT judge_username FROM practice_assignments WHERE practice_id = ?',
                                    (pc['id'],)).fetchall()
            pc['assigned_judges'] = [a['judge_username'] for a in assignments]

    return practice_comps


@app.route('/competition/<comp_id>')
def competition_page(comp_id):
    """Show competition details."""
    competition = get_competition(comp_id)
    if not competition:
        return "Competition not found", 404

    # Parse event_types from JSON
    event_types = []
    if competition.get('event_types'):
        try:
            event_types = json.loads(competition['event_types'])
        except:
            event_types = [competition.get('event_type', 'fs')]
    else:
        event_types = [competition.get('event_type', 'fs')]

    # Sort event_types by category order, then by custom event order
    category_order = {'fs': 0, 'cf': 1, 'ae': 2, 'cp': 3, 'ws': 4}
    # Custom order within each category
    event_order = {
        # FS: 4-Way, 8-Way, 16-Way, 10-Way, 4-Way VFS, 2-Way MFS
        'fs_4way_fs': 0, 'fs_8way': 1, 'fs_16way': 2, 'fs_10way': 3, 'fs_4way_vfs': 4, 'fs_2way_mfs': 5,
        # CF: 4-Way Rotation, 4-Way Sequential, 2-Way Open, 2-Way Pro/Am
        'cf_4way_rot': 0, 'cf_4way_seq': 1, 'cf_2way_open': 2, 'cf_2way_proam': 3, 'cf_2way': 4,
        # AE: Freestyle, Freefly
        'ae_freestyle': 0, 'ae_freefly': 1,
        # CP: Individual, Team, Freestyle
        'cp_dsz': 0, 'cp_team': 1, 'cp_freestyle': 2,
        # WS: Performance, Acrobatic
        'ws_performance': 0, 'ws_acrobatic': 1,
        # SP: Individual, Mixed Team
        'sp_individual': 0, 'sp_mixed_team': 1,
        # AL: Individual, Team
        'al_individual': 0, 'al_team': 1,
    }
    def event_sort_key(et):
        prefix = et.split('_')[0] if '_' in et else et
        return (category_order.get(prefix, 99), event_order.get(et, 99))
    event_types = sorted(event_types, key=event_sort_key)

    # Default rounds per event type
    default_event_rounds = {
        'fs_4way_fs': 10, 'fs_4way_vfs': 10, 'fs_2way_mfs': 10, 'fs_8way': 10,
        'fs_16way': 6, 'fs_10way': 6,
        'cf_4way_rot': 8, 'cf_4way_seq': 8, 'cf_2way_open': 8, 'cf_2way_proam': 8, 'cf_2way': 8,
        'ae_freestyle': 7, 'ae_freefly': 7,
        'cp_dsz': 9, 'cp_team': 9, 'cp_freestyle': 3,
        'ws_performance': 9, 'ws_acrobatic': 7,  # WS Performance: 3 Time + 3 Distance + 3 Speed
        'sp_individual': 8, 'sp_mixed_team': 3,
        'al_individual': 8, 'al_team': 8,
    }

    # Parse event_rounds from JSON (rounds per event type)
    event_rounds = {}
    if competition.get('event_rounds'):
        try:
            event_rounds = json.loads(competition['event_rounds'])
        except:
            pass
    # Ensure all events have correct rounds - always use defaults for known event types
    for et in event_types:
        if et in default_event_rounds:
            # Always use the correct default for known event types
            event_rounds[et] = default_event_rounds[et]
        elif et not in event_rounds:
            event_rounds[et] = competition.get('total_rounds', 10)

    competition['parsed_event_types'] = event_types
    competition['parsed_event_rounds'] = event_rounds
    is_multi_event = len(event_types) > 1

    # Parse score approvals
    score_approvals = {}
    if competition.get('score_approvals'):
        try:
            score_approvals = json.loads(competition['score_approvals'])
        except:
            score_approvals = {}

    teams = get_competition_teams(comp_id)

    # Check if any scores have been entered (to disable delete)
    has_scores = False
    if USE_SUPABASE:
        scores_check = supabase.table('competition_scores').select('id').eq('competition_id', comp_id).not_.is_('score', 'null').limit(1).execute()
        has_scores = len(scores_check.data) > 0
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT id FROM competition_scores WHERE competition_id = ? AND score IS NOT NULL LIMIT 1', (comp_id,))
        has_scores = cursor.fetchone() is not None

    # For multi-event competitions, group by event first, then by class
    # For single-event, just group by class
    if is_multi_event:
        teams_by_event = {}
        for event_type in event_types:
            teams_by_event[event_type] = {
                'beginner': [],
                'intermediate': [],
                'advanced': [],
                'open': []
            }

        for team in teams:
            team_class = team.get('class', 'open').lower()
            team_event = team.get('event', event_types[0])  # Default to first event

            # Get scores for this team
            team['scores'] = get_team_scores(team['id'])
            team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'])

            # Add to appropriate event/class bucket
            if team_event in teams_by_event:
                if team_class in teams_by_event[team_event]:
                    teams_by_event[team_event][team_class].append(team)
                else:
                    teams_by_event[team_event]['open'].append(team)
            else:
                # Unknown event, add to first event's open class
                teams_by_event[event_types[0]]['open'].append(team)

        # Calculate weighted scores for CP Individual (cp_dsz)
        # Rounds 1-3: Zone Accuracy (higher is better)
        # Rounds 4-6: Distance (higher is better)
        # Rounds 7-9: Speed (lower time is better, score^1.333, inverse weighted)
        # NOTE: Weighted scores only calculated when ALL competitors in a class have scored that round
        if 'cp_dsz' in teams_by_event:
            # Process each class separately
            for class_name in teams_by_event['cp_dsz']:
                class_teams = teams_by_event['cp_dsz'][class_name]
                if not class_teams:
                    continue

                total_teams_in_class = len(class_teams)

                # Find best raw score for each round AND check if all teams have scored
                best_scores = {}
                round_complete = {}  # Track if all teams have scored each round

                for round_num in range(1, 10):  # 9 rounds
                    is_speed_round = round_num >= 7  # Rounds 7-9 are Speed
                    best_score = None
                    scored_count = 0

                    for team in class_teams:
                        team_has_score = False
                        for score in team.get('scores', []):
                            if score.get('round_num') == round_num:
                                raw = score.get('score')
                                score_data = score.get('score_data', '')

                                # Count this as scored if has score or penalty
                                if raw is not None or (score_data and not score_data.startswith('{')):
                                    team_has_score = True

                                # Skip penalty results for best score calculation
                                if score_data and not score_data.startswith('{'):
                                    continue

                                if raw is not None and raw > 0:
                                    if best_score is None:
                                        best_score = raw
                                    elif is_speed_round and raw < best_score:
                                        best_score = raw  # For speed, lower is better
                                    elif not is_speed_round and raw > best_score:
                                        best_score = raw  # For ZA/Distance, higher is better

                        if team_has_score:
                            scored_count += 1

                    best_scores[round_num] = best_score
                    round_complete[round_num] = (scored_count == total_teams_in_class)

                # Calculate weighted scores for each team (only for complete rounds)
                for team in class_teams:
                    weighted_total = 0
                    for score in team.get('scores', []):
                        round_num = score.get('round_num')
                        raw_score = score.get('score')
                        score_data = score.get('score_data', '')
                        is_speed_round = round_num >= 7

                        # Handle penalties (score_data contains penalty code, not JSON)
                        if score_data and not score_data.startswith('{'):
                            # Penalty result - weighted score is 0 (not counted in weighted total)
                            score['weighted_score'] = 0
                            score['penalty'] = score_data
                            continue

                        # Only calculate weighted score if round is complete (everyone scored)
                        if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                            best = best_scores[round_num]
                            if is_speed_round:
                                # Speed: score^1.333, then inverse weighted
                                # Points = (best^1.333 / score^1.333) * 100
                                score_calc = raw_score ** 1.333
                                best_calc = best ** 1.333
                                weighted = (best_calc / score_calc) * 100
                            else:
                                # Zone Accuracy & Distance: (score / best) * 100
                                weighted = (raw_score / best) * 100
                            # 3 decimal places, no rounding
                            score['weighted_score'] = int(weighted * 1000) / 1000
                            weighted_total += score['weighted_score']
                        else:
                            score['weighted_score'] = None

                    # Calculate total (3 decimal places)
                    team['total_score'] = int(weighted_total * 1000) / 1000

        # Calculate weighted scores for WS Performance (ws_performance)
        # Rounds 1-3: Time (higher is better - longer time in competition window)
        # Rounds 4-6: Distance (higher is better - longer distance flown)
        # Rounds 7-9: Speed (higher is better - faster horizontal speed in km/h)
        # All tasks: Score = (result / best_result) × 100
        if 'ws_performance' in teams_by_event:
            for class_name in teams_by_event['ws_performance']:
                class_teams = teams_by_event['ws_performance'][class_name]
                if not class_teams:
                    continue

                total_teams_in_class = len(class_teams)

                # Find best raw score for each round and check if all teams have scored
                best_scores = {}
                round_complete = {}

                for round_num in range(1, 10):  # 9 rounds
                    best_score = None
                    scored_count = 0

                    for team in class_teams:
                        team_has_score = False
                        for score in team.get('scores', []):
                            if score.get('round_num') == round_num:
                                raw = score.get('score')
                                score_data = score.get('score_data', '')

                                if raw is not None or (score_data and not score_data.startswith('{')):
                                    team_has_score = True

                                if score_data and not score_data.startswith('{'):
                                    continue

                                # All WS Performance tasks: higher is better
                                if raw is not None and raw > 0:
                                    if best_score is None or raw > best_score:
                                        best_score = raw

                        if team_has_score:
                            scored_count += 1

                    best_scores[round_num] = best_score
                    round_complete[round_num] = (scored_count == total_teams_in_class)

                # Calculate weighted scores for each team
                for team in class_teams:
                    weighted_total = 0
                    for score in team.get('scores', []):
                        round_num = score.get('round_num')
                        raw_score = score.get('score')
                        score_data = score.get('score_data', '')

                        if score_data and not score_data.startswith('{'):
                            score['weighted_score'] = 0
                            score['penalty'] = score_data
                            continue

                        if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                            best = best_scores[round_num]
                            # All WS tasks: Score = (result / best) × 100
                            weighted = (raw_score / best) * 100
                            score['weighted_score'] = int(weighted * 1000) / 1000
                            weighted_total += score['weighted_score']
                        else:
                            score['weighted_score'] = None

                    team['total_score'] = int(weighted_total * 1000) / 1000

        # Sort each class within each event by total score descending
        for event_type in teams_by_event:
            for class_name in teams_by_event[event_type]:
                teams_by_event[event_type][class_name].sort(key=lambda t: t['total_score'], reverse=True)

        return render_template('competition.html',
                             competition=competition,
                             teams_by_event=teams_by_event,
                             teams_by_class={'beginner': [], 'intermediate': [], 'advanced': [], 'open': []},
                             is_multi_event=True,
                             event_types=event_types,
                             event_rounds=event_rounds,
                             categories=CATEGORIES,
                             is_admin=session.get('role') == 'admin',
                             has_scores=has_scores,
                             EVENT_DISPLAY_NAMES=EVENT_DISPLAY_NAMES,
                             is_public_view=False,
                             score_approvals=score_approvals)
    else:
        # Single event - group by class only
        teams_by_class = {
            'beginner': [],
            'intermediate': [],
            'advanced': [],
            'open': []
        }
        for team in teams:
            team_class = team.get('class', 'open').lower()
            if team_class in teams_by_class:
                # Get scores for this team
                team['scores'] = get_team_scores(team['id'])
                team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'])
                teams_by_class[team_class].append(team)
            else:
                # Unknown class, default to open
                team['scores'] = get_team_scores(team['id'])
                team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'])
                teams_by_class['open'].append(team)

        # Calculate weighted scores for CP Individual (cp_dsz) - single event
        # NOTE: Weighted scores only calculated when ALL competitors in a class have scored that round
        if competition['event_type'] == 'cp_dsz':
            # Process each class separately
            for class_name in teams_by_class:
                class_teams = teams_by_class[class_name]
                if not class_teams:
                    continue

                total_teams_in_class = len(class_teams)

                # Find best raw score for each round AND check if all teams have scored
                best_scores = {}
                round_complete = {}

                for round_num in range(1, 10):  # 9 rounds
                    is_speed_round = round_num >= 7
                    best_score = None
                    scored_count = 0

                    for team in class_teams:
                        team_has_score = False
                        for score in team.get('scores', []):
                            if score.get('round_num') == round_num:
                                raw = score.get('score')
                                score_data = score.get('score_data', '')

                                if raw is not None or (score_data and not score_data.startswith('{')):
                                    team_has_score = True

                                if score_data and not score_data.startswith('{'):
                                    continue

                                if raw is not None and raw > 0:
                                    if best_score is None:
                                        best_score = raw
                                    elif is_speed_round and raw < best_score:
                                        best_score = raw
                                    elif not is_speed_round and raw > best_score:
                                        best_score = raw

                        if team_has_score:
                            scored_count += 1

                    best_scores[round_num] = best_score
                    round_complete[round_num] = (scored_count == total_teams_in_class)

                # Calculate weighted scores for each team (only for complete rounds)
                for team in class_teams:
                    weighted_total = 0
                    for score in team.get('scores', []):
                        round_num = score.get('round_num')
                        raw_score = score.get('score')
                        score_data = score.get('score_data', '')
                        is_speed_round = round_num >= 7

                        if score_data and not score_data.startswith('{'):
                            score['weighted_score'] = 0
                            score['penalty'] = score_data
                            continue

                        # Only calculate weighted score if round is complete
                        if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                            best = best_scores[round_num]
                            if is_speed_round:
                                score_calc = raw_score ** 1.333
                                best_calc = best ** 1.333
                                weighted = (best_calc / score_calc) * 100
                            else:
                                weighted = (raw_score / best) * 100
                            score['weighted_score'] = int(weighted * 1000) / 1000
                            weighted_total += score['weighted_score']
                        else:
                            score['weighted_score'] = None

                    team['total_score'] = int(weighted_total * 1000) / 1000

        # Calculate weighted scores for WS Performance (ws_performance) - single event
        # Rounds 1-3: Time (higher is better)
        # Rounds 4-6: Distance (higher is better)
        # Rounds 7-9: Speed (higher is better)
        if competition['event_type'] == 'ws_performance':
            for class_name in teams_by_class:
                class_teams = teams_by_class[class_name]
                if not class_teams:
                    continue

                total_teams_in_class = len(class_teams)

                best_scores = {}
                round_complete = {}

                for round_num in range(1, 10):
                    best_score = None
                    scored_count = 0

                    for team in class_teams:
                        team_has_score = False
                        for score in team.get('scores', []):
                            if score.get('round_num') == round_num:
                                raw = score.get('score')
                                score_data = score.get('score_data', '')

                                if raw is not None or (score_data and not score_data.startswith('{')):
                                    team_has_score = True

                                if score_data and not score_data.startswith('{'):
                                    continue

                                if raw is not None and raw > 0:
                                    if best_score is None or raw > best_score:
                                        best_score = raw

                        if team_has_score:
                            scored_count += 1

                    best_scores[round_num] = best_score
                    round_complete[round_num] = (scored_count == total_teams_in_class)

                for team in class_teams:
                    weighted_total = 0
                    for score in team.get('scores', []):
                        round_num = score.get('round_num')
                        raw_score = score.get('score')
                        score_data = score.get('score_data', '')

                        if score_data and not score_data.startswith('{'):
                            score['weighted_score'] = 0
                            score['penalty'] = score_data
                            continue

                        if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                            best = best_scores[round_num]
                            weighted = (raw_score / best) * 100
                            score['weighted_score'] = int(weighted * 1000) / 1000
                            weighted_total += score['weighted_score']
                        else:
                            score['weighted_score'] = None

                    team['total_score'] = int(weighted_total * 1000) / 1000

        # Sort each class by total score descending
        for class_name in teams_by_class:
            teams_by_class[class_name].sort(key=lambda t: t['total_score'], reverse=True)

        return render_template('competition.html',
                             competition=competition,
                             teams_by_class=teams_by_class,
                             teams_by_event={},
                             is_multi_event=False,
                             event_types=event_types,
                             event_rounds=event_rounds,
                             categories=CATEGORIES,
                             is_admin=session.get('role') == 'admin',
                             has_scores=has_scores,
                             EVENT_DISPLAY_NAMES=EVENT_DISPLAY_NAMES,
                             is_public_view=False,
                             score_approvals=score_approvals)


@app.route('/results/<comp_id>')
def public_results_page(comp_id):
    """Public results page for competitors to view scores (read-only)."""
    competition = get_competition(comp_id)
    if not competition:
        return "Competition not found", 404

    # Parse event_types from JSON
    event_types = []
    if competition.get('event_types'):
        try:
            event_types = json.loads(competition['event_types'])
        except:
            event_types = [competition.get('event_type', 'fs')]
    else:
        event_types = [competition.get('event_type', 'fs')]

    # Default rounds per event type
    default_event_rounds = {
        'fs_4way_fs': 10, 'fs_4way_vfs': 10, 'fs_2way_mfs': 10, 'fs_8way': 10,
        'fs_16way': 6, 'fs_10way': 6,
        'cf_4way_rot': 8, 'cf_4way_seq': 8, 'cf_2way_open': 8, 'cf_2way_proam': 8, 'cf_2way': 8,
        'ae_freestyle': 7, 'ae_freefly': 7,
        'cp_dsz': 9, 'cp_team': 9, 'cp_freestyle': 3,
        'ws_performance': 9, 'ws_acrobatic': 7,
        'sp_individual': 8, 'sp_mixed_team': 3,
        'al_individual': 8, 'al_team': 8,
    }

    # Parse event_rounds from JSON
    event_rounds = {}
    if competition.get('event_rounds'):
        try:
            event_rounds = json.loads(competition['event_rounds'])
        except:
            pass
    for et in event_types:
        if et in default_event_rounds:
            event_rounds[et] = default_event_rounds[et]
        elif et not in event_rounds:
            event_rounds[et] = competition.get('total_rounds', 10)

    competition['parsed_event_types'] = event_types
    competition['parsed_event_rounds'] = event_rounds
    is_multi_event = len(event_types) > 1

    # Parse score approvals
    score_approvals = {}
    if competition.get('score_approvals'):
        try:
            score_approvals = json.loads(competition['score_approvals'])
        except:
            score_approvals = {}

    teams = get_competition_teams(comp_id)

    if is_multi_event:
        teams_by_event = {}
        for event_type in event_types:
            teams_by_event[event_type] = {
                'beginner': [], 'intermediate': [], 'advanced': [], 'open': []
            }

        for team in teams:
            team_class = team.get('class', 'open').lower()
            team_event = team.get('event', event_types[0])
            team['scores'] = get_team_scores(team['id'])
            team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'])

            if team_event in teams_by_event:
                if team_class in teams_by_event[team_event]:
                    teams_by_event[team_event][team_class].append(team)
                else:
                    teams_by_event[team_event]['open'].append(team)

        for event_type in teams_by_event:
            for class_name in teams_by_event[event_type]:
                teams_by_event[event_type][class_name].sort(key=lambda t: t['total_score'], reverse=True)

        return render_template('competition.html',
                             competition=competition,
                             teams_by_event=teams_by_event,
                             teams_by_class={'beginner': [], 'intermediate': [], 'advanced': [], 'open': []},
                             is_multi_event=True,
                             event_types=event_types,
                             event_rounds=event_rounds,
                             categories=CATEGORIES,
                             is_admin=False,
                             has_scores=True,
                             EVENT_DISPLAY_NAMES=EVENT_DISPLAY_NAMES,
                             is_public_view=True,
                             score_approvals=score_approvals)
    else:
        teams_by_class = {
            'beginner': [], 'intermediate': [], 'advanced': [], 'open': []
        }
        for team in teams:
            team_class = team.get('class', 'open').lower()
            team['scores'] = get_team_scores(team['id'])
            team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'])

            if team_class in teams_by_class:
                teams_by_class[team_class].append(team)
            else:
                teams_by_class['open'].append(team)

        for class_name in teams_by_class:
            teams_by_class[class_name].sort(key=lambda t: t['total_score'], reverse=True)

        return render_template('competition.html',
                             competition=competition,
                             teams_by_class=teams_by_class,
                             teams_by_event={},
                             is_multi_event=False,
                             event_types=event_types,
                             event_rounds=event_rounds,
                             categories=CATEGORIES,
                             is_admin=False,
                             has_scores=True,
                             EVENT_DISPLAY_NAMES=EVENT_DISPLAY_NAMES,
                             is_public_view=True,
                             score_approvals=score_approvals)


@app.route('/admin/competition/create', methods=['POST'])
@admin_required
def create_competition():
    """Create a new competition."""
    data = request.json

    name = data.get('name', '').strip()
    event_types = data.get('event_types', [])  # Array of event types
    event_type = data.get('event_type', 'fs')  # Legacy single event type
    event_rounds = data.get('event_rounds', {})  # Rounds per event type
    event_locations = data.get('event_locations', {})  # Location per event type
    event_dates = data.get('event_dates', {})  # Date per event type
    total_rounds = int(data.get('total_rounds', 10))

    if not name:
        return jsonify({'error': 'Competition name is required'}), 400

    # If event_types array provided, use that; otherwise use single event_type
    if event_types and isinstance(event_types, list):
        event_types_json = json.dumps(event_types)
        # Set primary event_type to first in list for backward compatibility
        event_type = event_types[0] if event_types else event_type
    else:
        # Single event - store as array for consistency
        event_types_json = json.dumps([event_type])

    # Store event_rounds as JSON
    event_rounds_json = json.dumps(event_rounds) if event_rounds else '{}'
    event_locations_json = json.dumps(event_locations) if event_locations else '{}'
    event_dates_json = json.dumps(event_dates) if event_dates else '{}'

    comp_id = str(uuid.uuid4())[:8]

    save_competition({
        'id': comp_id,
        'name': name,
        'event_type': event_type,
        'event_types': event_types_json,
        'event_rounds': event_rounds_json,
        'event_locations': event_locations_json,
        'event_dates': event_dates_json,
        'total_rounds': total_rounds,
        'created_at': datetime.now().isoformat(),
        'status': 'active'
    })

    return jsonify({'success': True, 'id': comp_id, 'message': 'Competition created'})


@app.route('/admin/competition/<comp_id>/delete', methods=['POST'])
@admin_required
def delete_competition(comp_id):
    """Delete a competition (only if no scores have been entered)."""
    try:
        # Check if any scores exist for this competition
        if USE_SUPABASE:
            scores = supabase.table('competition_scores').select('id').eq('competition_id', comp_id).limit(1).execute()
            has_scores = len(scores.data) > 0
        else:
            db = get_sqlite_db()
            cursor = db.execute('SELECT id FROM competition_scores WHERE competition_id = ? AND score IS NOT NULL LIMIT 1', (comp_id,))
            has_scores = cursor.fetchone() is not None

        if has_scores:
            return jsonify({'error': 'Cannot delete competition once scoring has started. Remove all scores first.'}), 400

        delete_competition_db(comp_id)
        return jsonify({'success': True, 'message': 'Competition deleted'})
    except Exception as e:
        print(f"Error deleting competition {comp_id}: {e}")
        return jsonify({'error': f'Failed to delete: {str(e)}'}), 500


@app.route('/admin/competition/<comp_id>/remove-event', methods=['POST'])
@admin_required
def remove_event_from_competition(comp_id):
    """Remove an event from a multi-event competition."""
    try:
        data = request.json
        event_type = data.get('event_type')

        if not event_type:
            return jsonify({'error': 'Event type is required'}), 400

        competition = get_competition(comp_id)
        if not competition:
            return jsonify({'error': 'Competition not found'}), 404

        # Parse current event types
        event_types = []
        if competition.get('event_types'):
            try:
                event_types = json.loads(competition['event_types'])
            except:
                event_types = [competition.get('event_type', 'fs')]

        if event_type not in event_types:
            return jsonify({'error': f'Event {event_type} not found in competition'}), 400

        if len(event_types) <= 1:
            return jsonify({'error': 'Cannot remove the last event from a competition'}), 400

        # Remove the event type
        event_types.remove(event_type)

        # Parse and update event_rounds
        event_rounds = {}
        if competition.get('event_rounds'):
            try:
                event_rounds = json.loads(competition['event_rounds'])
            except:
                pass
        if event_type in event_rounds:
            del event_rounds[event_type]

        # Delete teams and scores for this event
        if USE_SUPABASE:
            # Get team IDs for this event
            teams = supabase.table('competition_teams').select('id').eq('competition_id', comp_id).eq('event', event_type).execute()
            team_ids = [t['id'] for t in teams.data]

            # Delete scores for these teams
            for team_id in team_ids:
                supabase.table('competition_scores').delete().eq('team_id', team_id).execute()

            # Delete teams
            supabase.table('competition_teams').delete().eq('competition_id', comp_id).eq('event', event_type).execute()

            # Update competition
            supabase.table('competitions').update({
                'event_types': json.dumps(event_types),
                'event_rounds': json.dumps(event_rounds),
                'event_type': event_types[0] if event_types else 'fs'
            }).eq('id', comp_id).execute()
        else:
            db = get_sqlite_db()

            # Get team IDs for this event
            cursor = db.execute('SELECT id FROM competition_teams WHERE competition_id = ? AND event = ?', (comp_id, event_type))
            team_ids = [row['id'] for row in cursor.fetchall()]

            # Delete scores for these teams
            for team_id in team_ids:
                db.execute('DELETE FROM competition_scores WHERE team_id = ?', (team_id,))

            # Delete teams
            db.execute('DELETE FROM competition_teams WHERE competition_id = ? AND event = ?', (comp_id, event_type))

            # Update competition
            db.execute('''
                UPDATE competitions SET event_types = ?, event_rounds = ?, event_type = ?
                WHERE id = ?
            ''', (json.dumps(event_types), json.dumps(event_rounds), event_types[0] if event_types else 'fs', comp_id))

            db.commit()

        return jsonify({
            'success': True,
            'message': f'Event {event_type.upper()} removed successfully',
            'remaining_events': event_types
        })

    except Exception as e:
        print(f"Error removing event: {e}")
        return jsonify({'error': f'Failed to remove event: {str(e)}'}), 500


@app.route('/admin/competition/<comp_id>/add-event', methods=['POST'])
@admin_required
def add_event_to_competition(comp_id):
    """Add a new event to an existing competition."""
    try:
        data = request.json
        event_type = data.get('event_type')
        rounds = int(data.get('rounds', 10))

        if not event_type:
            return jsonify({'error': 'Event type is required'}), 400

        competition = get_competition(comp_id)
        if not competition:
            return jsonify({'error': 'Competition not found'}), 404

        # Parse current event types
        event_types = []
        if competition.get('event_types'):
            try:
                event_types = json.loads(competition['event_types'])
            except:
                event_types = [competition.get('event_type', 'fs')]
        else:
            event_types = [competition.get('event_type', 'fs')]

        # Check if event already exists
        if event_type in event_types:
            return jsonify({'error': f'Event {event_type} already exists in this competition'}), 400

        # Add the new event type
        event_types.append(event_type)

        # Parse and update event_rounds
        event_rounds = {}
        if competition.get('event_rounds'):
            try:
                event_rounds = json.loads(competition['event_rounds'])
            except:
                pass
        event_rounds[event_type] = rounds

        # Calculate new total_rounds (max of all events)
        total_rounds = max(event_rounds.values()) if event_rounds else rounds

        # Update competition
        if USE_SUPABASE:
            supabase.table('competitions').update({
                'event_types': json.dumps(event_types),
                'event_rounds': json.dumps(event_rounds),
                'total_rounds': total_rounds
            }).eq('id', comp_id).execute()
        else:
            db = get_sqlite_db()
            db.execute('''
                UPDATE competitions SET event_types = ?, event_rounds = ?, total_rounds = ?
                WHERE id = ?
            ''', (json.dumps(event_types), json.dumps(event_rounds), total_rounds, comp_id))
            db.commit()

        return jsonify({
            'success': True,
            'message': f'Event {event_type.upper()} added successfully ({rounds} rounds)',
            'event_types': event_types
        })

    except Exception as e:
        print(f"Error adding event: {e}")
        return jsonify({'error': f'Failed to add event: {str(e)}'}), 500


@app.route('/admin/score/<score_id>/training-flag', methods=['POST'])
@event_judge_required
def toggle_training_flag(score_id):
    """Toggle the training flag for a score/video."""
    data = request.json
    flag_value = data.get('training_flag', 0)

    if USE_SUPABASE:
        supabase.table('competition_scores').update({'training_flag': flag_value}).eq('id', score_id).execute()
    else:
        db = get_sqlite_db()
        db.execute('UPDATE competition_scores SET training_flag = ? WHERE id = ?', (flag_value, score_id))
        db.commit()

    return jsonify({'success': True, 'training_flag': flag_value})


@app.route('/competition/<comp_id>/training-report')
@event_judge_required
def training_report(comp_id):
    """Generate CSV report of videos flagged for training."""
    import csv
    import io

    competition = get_competition(comp_id)
    if not competition:
        return "Competition not found", 404

    # Get all scores with training flag
    if USE_SUPABASE:
        result = supabase.table('competition_scores').select('*').eq('competition_id', comp_id).eq('training_flag', 1).execute()
        flagged_scores = result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competition_scores WHERE competition_id = ? AND training_flag = 1', (comp_id,))
        flagged_scores = [dict(row) for row in cursor.fetchall()]

    # Build CSV data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Team Name', 'Team Number', 'Round', 'Score', 'Video ID', 'Video File', 'Video Title', 'Event', 'Class'])

    for score in flagged_scores:
        team = get_team(score['team_id'])
        video = get_video(score['video_id']) if score.get('video_id') else None

        writer.writerow([
            team.get('team_name', '') if team else '',
            team.get('team_number', '') if team else '',
            score.get('round_num', ''),
            score.get('score', ''),
            score.get('video_id', ''),
            video.get('local_file', video.get('url', '')) if video else '',
            video.get('title', '') if video else '',
            team.get('event', '') if team else '',
            team.get('class', '') if team else ''
        ])

    output.seek(0)
    response = app.response_class(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={competition["name"]}_training_videos.csv'}
    )
    return response


@app.route('/competition/<comp_id>/training-download')
@event_judge_required
def download_training_videos(comp_id):
    """Download all videos flagged for training as a zip file."""
    import zipfile
    import io

    competition = get_competition(comp_id)
    if not competition:
        return "Competition not found", 404

    # Get all scores with training flag
    if USE_SUPABASE:
        result = supabase.table('competition_scores').select('*').eq('competition_id', comp_id).eq('training_flag', 1).execute()
        flagged_scores = result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competition_scores WHERE competition_id = ? AND training_flag = 1', (comp_id,))
        flagged_scores = [dict(row) for row in cursor.fetchall()]

    if not flagged_scores:
        return jsonify({'error': 'No videos flagged for training'}), 404

    # Create zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for score in flagged_scores:
            if not score.get('video_id'):
                continue

            video = get_video(score['video_id'])
            if not video:
                continue

            team = get_team(score['team_id'])
            team_name = team.get('team_name', 'Unknown') if team else 'Unknown'
            round_num = score.get('round_num', 0)

            # Check if it's a local file
            if video.get('local_file'):
                local_path = os.path.join(VIDEOS_FOLDER, video['local_file'])
                if os.path.exists(local_path):
                    # Create a descriptive filename
                    ext = os.path.splitext(video['local_file'])[1]
                    zip_filename = f"{team_name}_Round{round_num}_{video['local_file']}"
                    zip_file.write(local_path, zip_filename)

    zip_buffer.seek(0)

    response = app.response_class(
        zip_buffer.getvalue(),
        mimetype='application/zip',
        headers={'Content-Disposition': f'attachment; filename={competition["name"]}_training_videos.zip'}
    )
    return response


@app.route('/competition/<comp_id>/training-videos')
@event_judge_required
def get_training_videos(comp_id):
    """Get list of videos flagged for training."""
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    # Get all scores with training flag
    if USE_SUPABASE:
        result = supabase.table('competition_scores').select('*').eq('competition_id', comp_id).eq('training_flag', 1).execute()
        flagged_scores = result.data
    else:
        db = get_sqlite_db()
        cursor = db.execute('SELECT * FROM competition_scores WHERE competition_id = ? AND training_flag = 1', (comp_id,))
        flagged_scores = [dict(row) for row in cursor.fetchall()]

    # Enrich with team and video info
    videos = []
    for score in flagged_scores:
        team = get_team(score['team_id'])
        video = get_video(score['video_id']) if score.get('video_id') else None

        videos.append({
            'score_id': score['id'],
            'team_name': team.get('team_name', '') if team else '',
            'team_number': team.get('team_number', '') if team else '',
            'round_num': score.get('round_num'),
            'score': score.get('score'),
            'video_id': score.get('video_id'),
            'video_title': video.get('title', '') if video else '',
            'video_file': video.get('local_file', '') if video else ''
        })

    return jsonify({'videos': videos, 'count': len(videos)})


@app.route('/admin/competition/<comp_id>/teams', methods=['GET'])
@admin_required
def admin_get_competition_teams(comp_id):
    """Get all teams for a competition with their score status (admin)."""
    teams = get_competition_teams(comp_id)

    # Add has_scores flag to each team
    for team in teams:
        scores = get_team_scores(team['id'])
        team['has_scores'] = any(s.get('score') is not None for s in scores)

    return jsonify({'success': True, 'teams': teams})


@app.route('/api/signers', methods=['GET'])
def get_signers():
    """Get users who can sign documents (chief_judge or admin roles with a PIN set)."""
    all_users = get_all_users()
    signers = []
    for user in all_users:
        if user.get('role') in ['chief_judge', 'admin'] and user.get('signature_pin'):
            signers.append({
                'username': user['username'],
                'name': user['name'],
                'role': user['role'],
                'has_signature': bool(user.get('signature_data'))
            })
    return jsonify({'success': True, 'signers': signers})


@app.route('/api/signature/<username>', methods=['GET'])
def get_signature(username):
    """Get signature image for a user (returns base64 PNG)."""
    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    signature_data = user.get('signature_data', '')
    return jsonify({
        'success': True,
        'signature_data': signature_data,
        'has_signature': bool(signature_data)
    })


@app.route('/api/signature/<username>', methods=['POST'])
def save_signature(username):
    """Save signature image for a user (base64 PNG data)."""
    # Allow admins and chief_judges to save signatures for any user
    user_role = session.get('role', '')
    if session.get('username') != username and user_role not in ['admin', 'chief_judge']:
        return jsonify({'error': 'Not authorized'}), 403

    user = get_user(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    data = request.json
    signature_data = data.get('signature_data', '')

    # Validate it looks like base64 PNG data
    if signature_data and not signature_data.startswith('data:image/png;base64,'):
        return jsonify({'error': 'Invalid signature format. Must be base64 PNG.'}), 400

    # Update user with signature data
    try:
        if USE_SUPABASE:
            result = supabase.table('users').update({'signature_data': signature_data}).eq('username', username).execute()
            if not result.data:
                return jsonify({'error': 'Failed to update user. Make sure signature_data column exists in Supabase.'}), 500
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET signature_data = ? WHERE username = ?', (signature_data, username))
            conn.commit()
            conn.close()

        return jsonify({'success': True, 'message': 'Signature saved'})
    except Exception as e:
        print(f"Error saving signature: {e}")
        return jsonify({'error': f'Database error: {str(e)}. You may need to add the signature_data column to the users table.'}), 500


@app.route('/admin/competition/<comp_id>/set-chief-judge', methods=['POST'])
@admin_required
def set_chief_judge(comp_id):
    """Set the chief judge for a competition (stores username of the signer)."""
    data = request.json
    chief_judge = data.get('chief_judge', '').strip()  # This is the username

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    # If a chief judge is specified, verify they exist and have a PIN
    if chief_judge:
        user = get_user(chief_judge)
        if not user:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        if not user.get('signature_pin'):
            return jsonify({'success': False, 'error': 'User does not have a signature PIN set'}), 400

    competition['chief_judge'] = chief_judge
    save_competition(competition)

    # Get the user's display name for the response
    if chief_judge:
        user = get_user(chief_judge)
        display_name = user.get('name', chief_judge) if user else chief_judge
    else:
        display_name = ''

    return jsonify({'success': True, 'chief_judge': chief_judge, 'display_name': display_name})


@app.route('/admin/competition/<comp_id>/set-event-details', methods=['POST'])
@admin_required
def set_event_details(comp_id):
    """Set location and date for each event in the competition."""
    data = request.json
    event_locations = data.get('event_locations', {})
    event_dates = data.get('event_dates', {})

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    competition['event_locations'] = json.dumps(event_locations)
    competition['event_dates'] = json.dumps(event_dates)
    save_competition(competition)

    return jsonify({'success': True})


@app.route('/admin/competition/<comp_id>/save-draw', methods=['POST'])
@admin_required
def save_competition_draw(comp_id):
    """Save a draw for an event/class in the competition."""
    data = request.json
    event_type = data.get('event_type')
    class_name = data.get('class_name', 'open')
    draw_data = data.get('draw')  # Array of rounds with formations

    if not event_type or not draw_data:
        return jsonify({'success': False, 'error': 'Missing event_type or draw data'}), 400

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    # Get existing draws or initialize
    draws = {}
    if competition.get('draws'):
        try:
            draws = json.loads(competition['draws'])
        except:
            draws = {}

    # Structure: draws[event_type][class_name] = draw_data
    if event_type not in draws:
        draws[event_type] = {}
    draws[event_type][class_name] = draw_data

    # Save back to competition
    competition['draws'] = json.dumps(draws)
    save_competition(competition)

    return jsonify({'success': True})


@app.route('/competition/<comp_id>/draws', methods=['GET'])
def get_competition_draws(comp_id):
    """Get all draws for a competition."""
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    draws = {}
    if competition.get('draws'):
        try:
            draws = json.loads(competition['draws'])
        except:
            draws = {}

    return jsonify({'success': True, 'draws': draws})


@app.route('/admin/competition/<comp_id>/delete-draw', methods=['POST'])
@admin_required
def delete_competition_draw(comp_id):
    """Delete a draw for an event/class in the competition."""
    data = request.json
    event_type = data.get('event_type')
    class_name = data.get('class_name', 'open')

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    draws = {}
    if competition.get('draws'):
        try:
            draws = json.loads(competition['draws'])
        except:
            draws = {}

    # Remove the draw
    if event_type in draws and class_name in draws[event_type]:
        del draws[event_type][class_name]
        if not draws[event_type]:
            del draws[event_type]

    competition['draws'] = json.dumps(draws)
    save_competition(competition)

    return jsonify({'success': True})


@app.route('/competition/<comp_id>/ws-task-order', methods=['GET'])
def get_ws_task_order(comp_id):
    """Get the WS Performance task order for a competition."""
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    # Task order is stored in competition metadata
    metadata = {}
    if competition.get('metadata'):
        try:
            metadata = json.loads(competition['metadata'])
        except:
            metadata = {}

    task_order = metadata.get('ws_task_order', None)
    return jsonify({'success': True, 'task_order': task_order})


@app.route('/competition/<comp_id>/ws-task-order', methods=['POST'])
@admin_required
def save_ws_task_order(comp_id):
    """Save the WS Performance task order for a competition."""
    data = request.json
    task_order = data.get('task_order', [])

    if not task_order or len(task_order) != 3:
        return jsonify({'success': False, 'error': 'Task order must have exactly 3 tasks'}), 400

    valid_tasks = ['Time', 'Distance', 'Speed']
    if not all(t in valid_tasks for t in task_order) or len(set(task_order)) != 3:
        return jsonify({'success': False, 'error': 'Task order must contain Time, Distance, and Speed'}), 400

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    # Store task order in competition metadata
    metadata = {}
    if competition.get('metadata'):
        try:
            metadata = json.loads(competition['metadata'])
        except:
            metadata = {}

    metadata['ws_task_order'] = task_order
    competition['metadata'] = json.dumps(metadata)
    save_competition(competition)

    return jsonify({'success': True, 'task_order': task_order})


@app.route('/competition/<comp_id>/verify-pin', methods=['POST'])
def verify_chief_judge_pin(comp_id):
    """Verify the Chief Judge PIN."""
    data = request.json
    pin = data.get('pin', '').strip()

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404

    chief_judge_username = competition.get('chief_judge', '')
    if not chief_judge_username:
        return jsonify({'success': False, 'error': 'No Chief Judge set'}), 400

    # Get the user's PIN
    user = get_user(chief_judge_username)
    if not user:
        return jsonify({'success': False, 'error': 'Chief Judge user not found'}), 404

    stored_pin = user.get('signature_pin', '')
    if not stored_pin:
        return jsonify({'success': False, 'error': 'Chief Judge does not have a PIN set'}), 400

    # Hash the provided PIN and compare
    import hashlib
    provided_hash = hashlib.sha256(pin.encode()).hexdigest()

    if provided_hash == stored_pin:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Invalid PIN'}), 401


@app.route('/competition/<comp_id>/print-pdf')
def print_competition_pdf(comp_id):
    """Generate a PDF of the competition results."""
    from flask import Response

    if not REPORTLAB_AVAILABLE:
        return jsonify({'error': 'PDF generation not available. Install reportlab package.'}), 500

    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    # Get round selection parameters
    print_range = request.args.get('range', 'full')  # full, upTo, single
    selected_round = int(request.args.get('round', 9))
    provided_pin = request.args.get('pin', '')

    # Verify PIN for signature - look up user's PIN
    import hashlib
    pin_verified = False
    chief_judge_username = competition.get('chief_judge', '')
    chief_judge_name = ''
    if chief_judge_username:
        chief_judge_user = get_user(chief_judge_username)
        if chief_judge_user:
            chief_judge_name = chief_judge_user.get('name', chief_judge_username)
            stored_pin = chief_judge_user.get('signature_pin', '')
            if stored_pin and provided_pin:
                provided_hash = hashlib.sha256(provided_pin.encode()).hexdigest()
                pin_verified = (provided_hash == stored_pin)

    teams = get_competition_teams(comp_id)
    event_type = competition.get('event_type', '')

    # Get scores for each team
    for team in teams:
        team['scores'] = get_team_scores(team['id'])

    # Calculate weighted scores for CP DSZ events (per class, only when round is complete)
    if event_type == 'cp_dsz':
        # Group teams by class
        teams_by_class_pdf = {}
        for team in teams:
            team_class = team.get('class', 'open')
            if team_class not in teams_by_class_pdf:
                teams_by_class_pdf[team_class] = []
            teams_by_class_pdf[team_class].append(team)

        # Process each class separately
        for class_name, class_teams in teams_by_class_pdf.items():
            if not class_teams:
                continue

            total_teams_in_class = len(class_teams)

            # Find best score for each round AND check if all teams have scored
            best_scores = {}
            round_complete = {}

            for round_num in range(1, 10):
                is_speed_round = round_num >= 7
                best_score = None
                scored_count = 0

                for team in class_teams:
                    team_has_score = False
                    for score in team.get('scores', []):
                        if score.get('round_num') == round_num:
                            raw = score.get('score')
                            score_data = score.get('score_data', '')

                            if raw is not None or (score_data and not score_data.startswith('{')):
                                team_has_score = True

                            if score_data and not score_data.startswith('{'):
                                continue

                            if raw is not None and raw > 0:
                                if is_speed_round:
                                    if best_score is None or raw < best_score:
                                        best_score = raw
                                else:
                                    if best_score is None or raw > best_score:
                                        best_score = raw

                    if team_has_score:
                        scored_count += 1

                best_scores[round_num] = best_score
                round_complete[round_num] = (scored_count == total_teams_in_class)

            # Calculate weighted scores for each team (only for complete rounds)
            for team in class_teams:
                weighted_total = 0
                for score in team.get('scores', []):
                    round_num = score.get('round_num')
                    raw_score = score.get('score')
                    score_data = score.get('score_data', '')
                    is_speed_round = round_num >= 7

                    if score_data and not score_data.startswith('{'):
                        score['weighted_score'] = 0
                        continue

                    if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                        best = best_scores[round_num]
                        if is_speed_round:
                            score_calc = raw_score ** 1.333
                            best_calc = best ** 1.333
                            weighted = (best_calc / score_calc) * 100
                        else:
                            weighted = (raw_score / best) * 100
                        score['weighted_score'] = int(weighted * 1000) / 1000
                        weighted_total += score['weighted_score']
                    else:
                        score['weighted_score'] = None

                team['total_score'] = int(weighted_total * 1000) / 1000

    # Calculate weighted scores for WS Performance events
    elif event_type == 'ws_performance':
        teams_by_class_pdf = {}
        for team in teams:
            team_class = team.get('class', 'open')
            if team_class not in teams_by_class_pdf:
                teams_by_class_pdf[team_class] = []
            teams_by_class_pdf[team_class].append(team)

        for class_name, class_teams in teams_by_class_pdf.items():
            if not class_teams:
                continue

            total_teams_in_class = len(class_teams)
            best_scores = {}
            round_complete = {}

            for round_num in range(1, 10):
                best_score = None
                scored_count = 0

                for team in class_teams:
                    team_has_score = False
                    for score in team.get('scores', []):
                        if score.get('round_num') == round_num:
                            raw = score.get('score')
                            score_data = score.get('score_data', '')

                            if raw is not None or (score_data and not score_data.startswith('{')):
                                team_has_score = True

                            if score_data and not score_data.startswith('{'):
                                continue

                            # All WS tasks: higher is better
                            if raw is not None and raw > 0:
                                if best_score is None or raw > best_score:
                                    best_score = raw

                    if team_has_score:
                        scored_count += 1

                best_scores[round_num] = best_score
                round_complete[round_num] = (scored_count == total_teams_in_class)

            for team in class_teams:
                weighted_total = 0
                for score in team.get('scores', []):
                    round_num = score.get('round_num')
                    raw_score = score.get('score')
                    score_data = score.get('score_data', '')

                    if score_data and not score_data.startswith('{'):
                        score['weighted_score'] = 0
                        continue

                    if round_complete.get(round_num) and raw_score is not None and raw_score > 0 and best_scores.get(round_num):
                        best = best_scores[round_num]
                        weighted = (raw_score / best) * 100
                        score['weighted_score'] = int(weighted * 1000) / 1000
                        weighted_total += score['weighted_score']
                    else:
                        score['weighted_score'] = None

                team['total_score'] = int(weighted_total * 1000) / 1000
    else:
        # Non-CP/WS events - just sum raw scores
        for team in teams:
            team['total_score'] = sum(s.get('score', 0) or 0 for s in team['scores'] if s.get('score') is not None)

    # Sort by total score (descending)
    teams.sort(key=lambda t: t['total_score'], reverse=True)

    # Create PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
                           leftMargin=0.5*inch, rightMargin=0.5*inch,
                           topMargin=0.5*inch, bottomMargin=0.5*inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=20, alignment=1, spaceAfter=6,
                                  textColor=colors.Color(0.0, 0.25, 0.4))
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11, alignment=1, spaceAfter=4,
                                     textColor=colors.Color(0.3, 0.3, 0.3))
    signature_style = ParagraphStyle('Signature', parent=styles['Normal'], fontSize=10, alignment=2)

    elements = []

    # Title
    elements.append(Paragraph(f"<b>{competition['name']}</b>", title_style))

    # Subtitle based on range
    if print_range == 'single':
        if event_type == 'cp_dsz':
            round_names = {1: 'ZA1', 2: 'ZA2', 3: 'ZA3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            round_label = round_names.get(selected_round, f'Round {selected_round}')
        elif event_type == 'ws_performance':
            round_names = {1: 'T1', 2: 'T2', 3: 'T3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            round_label = round_names.get(selected_round, f'Round {selected_round}')
        else:
            round_label = f'Round {selected_round}'
        elements.append(Paragraph(f"Results - {round_label}", subtitle_style))
    elif print_range == 'upTo':
        if event_type == 'cp_dsz':
            if selected_round <= 3:
                range_label = 'Zone Accuracy (ZA1-ZA3)'
            elif selected_round <= 6:
                range_label = 'Through Distance (ZA1-D3)'
            else:
                range_label = 'Full Event'
        elif event_type == 'ws_performance':
            if selected_round <= 3:
                range_label = 'Time (T1-T3)'
            elif selected_round <= 6:
                range_label = 'Through Distance (T1-D3)'
            else:
                range_label = 'Full Event'
        else:
            range_label = f'Rounds 1-{selected_round}'
        elements.append(Paragraph(f"Results - {range_label}", subtitle_style))
    else:
        elements.append(Paragraph(f"Official Competition Results", subtitle_style))

    # Event type display
    event_display = EVENT_DISPLAY_NAMES.get(event_type, event_type.upper().replace('_', ' '))
    elements.append(Paragraph(f"Event: {event_display}", subtitle_style))

    # Event location and date
    event_locations = json.loads(competition.get('event_locations', '{}') or '{}')
    event_dates = json.loads(competition.get('event_dates', '{}') or '{}')
    event_location = event_locations.get(event_type, '')
    event_date = event_dates.get(event_type, '')

    if event_location or event_date:
        location_date_parts = []
        if event_location:
            location_date_parts.append(event_location)
        if event_date:
            # Format date nicely
            try:
                date_obj = datetime.strptime(event_date, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%B %d, %Y')
                location_date_parts.append(formatted_date)
            except:
                location_date_parts.append(event_date)
        elements.append(Paragraph(' | '.join(location_date_parts), subtitle_style))
    elements.append(Spacer(1, 0.25*inch))

    # Determine rounds to include based on selection
    if event_type == 'cp_dsz':
        # Build headers with separate columns for each round (no Raw/Wtd labels)
        if print_range == 'single':
            num_rounds = selected_round
            start_round = selected_round
            round_names = {1: 'Z1', 2: 'Z2', 3: 'Z3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            rn = round_names[selected_round]
            round_headers = [rn, f'{rn}W']
        elif print_range == 'upTo':
            num_rounds = selected_round
            start_round = 1
            round_headers = []
            # Z rounds
            for i in range(1, min(4, selected_round + 1)):
                rn = ['Z1', 'Z2', 'Z3'][i-1]
                round_headers.extend([rn, f'{rn}W'])
            if selected_round >= 3:
                round_headers.append('ZT')
            # D rounds
            if selected_round >= 4:
                for i in range(4, min(7, selected_round + 1)):
                    rn = ['D1', 'D2', 'D3'][i-4]
                    round_headers.extend([rn, f'{rn}W'])
                if selected_round >= 6:
                    round_headers.append('DT')
            # S rounds
            if selected_round >= 7:
                for i in range(7, min(10, selected_round + 1)):
                    rn = ['S1', 'S2', 'S3'][i-7]
                    round_headers.extend([rn, f'{rn}W'])
                if selected_round >= 9:
                    round_headers.append('ST')
        else:
            # Full event
            num_rounds = 9
            start_round = 1
            round_headers = []
            for rn in ['Z1', 'Z2', 'Z3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('ZT')
            for rn in ['D1', 'D2', 'D3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('DT')
            for rn in ['S1', 'S2', 'S3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('ST')
    elif event_type == 'ws_performance':
        # WS Performance: Time (T), Distance (D), Speed (S)
        if print_range == 'single':
            num_rounds = selected_round
            start_round = selected_round
            round_names = {1: 'T1', 2: 'T2', 3: 'T3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            rn = round_names[selected_round]
            round_headers = [rn, f'{rn}W']
        elif print_range == 'upTo':
            num_rounds = selected_round
            start_round = 1
            round_headers = []
            # T rounds (Time)
            for i in range(1, min(4, selected_round + 1)):
                rn = ['T1', 'T2', 'T3'][i-1]
                round_headers.extend([rn, f'{rn}W'])
            if selected_round >= 3:
                round_headers.append('TT')
            # D rounds
            if selected_round >= 4:
                for i in range(4, min(7, selected_round + 1)):
                    rn = ['D1', 'D2', 'D3'][i-4]
                    round_headers.extend([rn, f'{rn}W'])
                if selected_round >= 6:
                    round_headers.append('DT')
            # S rounds
            if selected_round >= 7:
                for i in range(7, min(10, selected_round + 1)):
                    rn = ['S1', 'S2', 'S3'][i-7]
                    round_headers.extend([rn, f'{rn}W'])
                if selected_round >= 9:
                    round_headers.append('ST')
        else:
            # Full event
            num_rounds = 9
            start_round = 1
            round_headers = []
            for rn in ['T1', 'T2', 'T3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('TT')
            for rn in ['D1', 'D2', 'D3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('DT')
            for rn in ['S1', 'S2', 'S3']:
                round_headers.extend([rn, f'{rn}W'])
            round_headers.append('ST')
    else:
        total_rounds = competition.get('total_rounds', 10)
        if print_range == 'single':
            num_rounds = selected_round
            start_round = selected_round
            round_headers = [f'R{selected_round}']
        elif print_range == 'upTo':
            num_rounds = selected_round
            start_round = 1
            round_headers = [f'R{i}' for i in range(1, selected_round + 1)]
        else:
            num_rounds = total_rounds
            start_round = 1
            round_headers = [f'R{i}' for i in range(1, total_rounds + 1)]

    # Build table data - separate tables per class
    is_individual = event_type.startswith('cp') or event_type.startswith('al') or event_type.startswith('sp') or event_type.startswith('ws_performance')

    # For CP DSZ, build two-row header with round labels spanning raw/weighted columns
    if event_type == 'cp_dsz':
        # Build header row 1 (round labels that will span 2 columns)
        # Build header row 2 (Score/Points sub-columns under each round)
        header_row1 = ['Rank', 'Name' if is_individual else 'Team']
        header_row2 = ['', '']  # Empty for rank/name columns
        span_commands = []  # Will hold SPAN commands for merging cells
        col_idx = 2  # Start after Rank and Name

        if print_range == 'single':
            # Single round - just one round label spanning 2 columns
            round_names = {1: 'Z1', 2: 'Z2', 3: 'Z3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            rn = round_names[selected_round]
            header_row1.extend([rn, ''])  # Label + empty for span
            header_row2.extend(['Score', 'Points'])  # Sub-columns
            span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
            col_idx += 2
        elif print_range == 'upTo':
            # Z rounds
            for i in range(1, min(4, selected_round + 1)):
                rn = ['Z1', 'Z2', 'Z3'][i-1]
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            if selected_round >= 3:
                header_row1.append('ZT')
                header_row2.append('')
                col_idx += 1
            # D rounds
            if selected_round >= 4:
                for i in range(4, min(7, selected_round + 1)):
                    rn = ['D1', 'D2', 'D3'][i-4]
                    header_row1.extend([rn, ''])
                    header_row2.extend(['Score', 'Points'])
                    span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                    col_idx += 2
                if selected_round >= 6:
                    header_row1.append('DT')
                    header_row2.append('')
                    col_idx += 1
            # S rounds
            if selected_round >= 7:
                for i in range(7, min(10, selected_round + 1)):
                    rn = ['S1', 'S2', 'S3'][i-7]
                    header_row1.extend([rn, ''])
                    header_row2.extend(['Score', 'Points'])
                    span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                    col_idx += 2
                if selected_round >= 9:
                    header_row1.append('ST')
                    header_row2.append('')
                    col_idx += 1
        else:
            # Full event
            for rn in ['Z1', 'Z2', 'Z3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('ZT')
            header_row2.append('')
            col_idx += 1
            for rn in ['D1', 'D2', 'D3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('DT')
            header_row2.append('')
            col_idx += 1
            for rn in ['S1', 'S2', 'S3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('ST')
            header_row2.append('')
            col_idx += 1

        header_row1.append('Total')
        header_row2.append('')
        # Span Rank and Name vertically across both header rows
        span_commands.append(('SPAN', (0, 0), (0, 1)))  # Rank
        span_commands.append(('SPAN', (1, 0), (1, 1)))  # Name
        span_commands.append(('SPAN', (col_idx, 0), (col_idx, 1)))  # Total

    elif event_type == 'ws_performance':
        # WS Performance: Time (T), Distance (D), Speed (S) with two-row headers
        header_row1 = ['Rank', 'Name']
        header_row2 = ['', '']
        span_commands = []
        col_idx = 2

        if print_range == 'single':
            round_names = {1: 'T1', 2: 'T2', 3: 'T3', 4: 'D1', 5: 'D2', 6: 'D3', 7: 'S1', 8: 'S2', 9: 'S3'}
            rn = round_names[selected_round]
            header_row1.extend([rn, ''])
            header_row2.extend(['Score', 'Points'])
            span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
            col_idx += 2
        elif print_range == 'upTo':
            # T rounds (Time)
            for i in range(1, min(4, selected_round + 1)):
                rn = ['T1', 'T2', 'T3'][i-1]
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            if selected_round >= 3:
                header_row1.append('TT')
                header_row2.append('')
                col_idx += 1
            # D rounds
            if selected_round >= 4:
                for i in range(4, min(7, selected_round + 1)):
                    rn = ['D1', 'D2', 'D3'][i-4]
                    header_row1.extend([rn, ''])
                    header_row2.extend(['Score', 'Points'])
                    span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                    col_idx += 2
                if selected_round >= 6:
                    header_row1.append('DT')
                    header_row2.append('')
                    col_idx += 1
            # S rounds
            if selected_round >= 7:
                for i in range(7, min(10, selected_round + 1)):
                    rn = ['S1', 'S2', 'S3'][i-7]
                    header_row1.extend([rn, ''])
                    header_row2.extend(['Score', 'Points'])
                    span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                    col_idx += 2
                if selected_round >= 9:
                    header_row1.append('ST')
                    header_row2.append('')
                    col_idx += 1
        else:
            # Full event
            for rn in ['T1', 'T2', 'T3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('TT')
            header_row2.append('')
            col_idx += 1
            for rn in ['D1', 'D2', 'D3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('DT')
            header_row2.append('')
            col_idx += 1
            for rn in ['S1', 'S2', 'S3']:
                header_row1.extend([rn, ''])
                header_row2.extend(['Score', 'Points'])
                span_commands.append(('SPAN', (col_idx, 0), (col_idx + 1, 0)))
                col_idx += 2
            header_row1.append('ST')
            header_row2.append('')
            col_idx += 1

        header_row1.append('Total')
        header_row2.append('')
        span_commands.append(('SPAN', (0, 0), (0, 1)))  # Rank
        span_commands.append(('SPAN', (1, 0), (1, 1)))  # Name
        span_commands.append(('SPAN', (col_idx, 0), (col_idx, 1)))  # Total
    else:
        header = ['Rank', 'Name' if is_individual else 'Team'] + round_headers + ['Total']
        span_commands = []

    # Group teams by class
    team_classes = sorted(set(t.get('class', 'open') for t in teams))

    for team_class in team_classes:
        # Add class header
        class_style = ParagraphStyle('ClassHeader', parent=styles['Heading2'], fontSize=12, spaceAfter=6, spaceBefore=12,
                                         textColor=colors.Color(0.0, 0.25, 0.4), borderPadding=4)
        elements.append(Paragraph(f"<b>{event_display} - {team_class.capitalize()}</b>", class_style))

        # Filter and sort teams for this class
        class_teams = [t for t in teams if t.get('class', 'open') == team_class]
        class_teams.sort(key=lambda t: t['total_score'], reverse=True)

        if event_type in ['cp_dsz', 'ws_performance']:
            table_data = [header_row1, header_row2]
        else:
            table_data = [header]

        for rank, team in enumerate(class_teams, 1):
            row = [str(rank), team['team_name']]

            if event_type == 'cp_dsz':
                # CP DSZ with separate raw and weighted columns
                za_total = 0
                d_total = 0
                s_total = 0

                if print_range == 'single':
                    # Single round only - separate raw and weighted columns
                    score = next((s for s in team['scores'] if s['round_num'] == selected_round), None)
                    if score and score.get('score') is not None:
                        if selected_round <= 3:
                            raw = str(int(score['score']))
                        elif selected_round <= 6:
                            raw = f"{score['score']:.2f}"
                        else:
                            raw = f"{score['score']:.3f}"
                        row.append(raw)
                        weighted = score.get('weighted_score')
                        if weighted is not None:
                            row.append(f"{weighted:.1f}")
                            if selected_round <= 3:
                                za_total = weighted
                            elif selected_round <= 6:
                                d_total = weighted
                            else:
                                s_total = weighted
                        else:
                            row.append('-')
                    else:
                        row.append('-')
                        row.append('-')
                    # Total for single round
                    row.append(f"{za_total + d_total + s_total:.1f}")
                else:
                    # Full or upTo - include appropriate rounds with separate columns
                    max_round = 9 if print_range == 'full' else selected_round

                    # ZA rounds 1-3 (if in range)
                    if max_round >= 1:
                        for i in range(1, min(4, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = str(int(score['score']))
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    za_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        # Add ZA total if we completed ZA or it's our stopping point
                        if max_round >= 3 or (print_range == 'upTo' and max_round <= 3):
                            row.append(f"{za_total:.1f}")

                    # D rounds 4-6 (if in range)
                    if max_round >= 4:
                        for i in range(4, min(7, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = f"{score['score']:.2f}"
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    d_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        # Add D total if we completed D or it's our stopping point
                        if max_round >= 6 or (print_range == 'upTo' and max_round <= 6 and max_round >= 4):
                            row.append(f"{d_total:.1f}")

                    # S rounds 7-9 (if in range)
                    if max_round >= 7:
                        for i in range(7, min(10, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = f"{score['score']:.3f}"
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    s_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        # Add S total if full event
                        if max_round >= 9:
                            row.append(f"{s_total:.1f}")

                    # Overall total
                    overall_total = za_total + d_total + s_total
                    row.append(f"{overall_total:.2f}")

            elif event_type == 'ws_performance':
                # WS Performance with separate raw and weighted columns
                t_total = 0  # Time total
                d_total = 0  # Distance total
                s_total = 0  # Speed total

                if print_range == 'single':
                    score = next((s for s in team['scores'] if s['round_num'] == selected_round), None)
                    if score and score.get('score') is not None:
                        if selected_round <= 3:
                            raw = f"{score['score']:.1f}s"  # Time in seconds
                        elif selected_round <= 6:
                            raw = f"{int(score['score'])}m"  # Distance in meters
                        else:
                            raw = f"{score['score']:.1f}"  # Speed in km/h
                        row.append(raw)
                        weighted = score.get('weighted_score')
                        if weighted is not None:
                            row.append(f"{weighted:.1f}")
                            if selected_round <= 3:
                                t_total = weighted
                            elif selected_round <= 6:
                                d_total = weighted
                            else:
                                s_total = weighted
                        else:
                            row.append('-')
                    else:
                        row.append('-')
                        row.append('-')
                    row.append(f"{t_total + d_total + s_total:.1f}")
                else:
                    max_round = 9 if print_range == 'full' else selected_round

                    # Time rounds 1-3
                    if max_round >= 1:
                        for i in range(1, min(4, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = f"{score['score']:.1f}"
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    t_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        if max_round >= 3 or (print_range == 'upTo' and max_round <= 3):
                            row.append(f"{t_total:.1f}")

                    # Distance rounds 4-6
                    if max_round >= 4:
                        for i in range(4, min(7, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = f"{int(score['score'])}"
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    d_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        if max_round >= 6 or (print_range == 'upTo' and max_round <= 6 and max_round >= 4):
                            row.append(f"{d_total:.1f}")

                    # Speed rounds 7-9
                    if max_round >= 7:
                        for i in range(7, min(10, max_round + 1)):
                            score = next((s for s in team['scores'] if s['round_num'] == i), None)
                            if score and score.get('score') is not None:
                                raw = f"{score['score']:.1f}"
                                row.append(raw)
                                weighted = score.get('weighted_score')
                                if weighted is not None:
                                    row.append(f"{weighted:.1f}")
                                    s_total += weighted
                                else:
                                    row.append('-')
                            else:
                                row.append('-')
                                row.append('-')
                        if max_round >= 9:
                            row.append(f"{s_total:.1f}")

                    overall_total = t_total + d_total + s_total
                    row.append(f"{overall_total:.2f}")
            else:
                # Non-CP/WS events
                if print_range == 'single':
                    score = next((s for s in team['scores'] if s['round_num'] == selected_round), None)
                    if score and score.get('score') is not None:
                        row.append(f"{score['score']:.2f}" if isinstance(score['score'], float) else str(score['score']))
                    else:
                        row.append('-')
                    row.append(f"{score['score']:.2f}" if score and score.get('score') is not None else '-')
                else:
                    end_round = selected_round if print_range == 'upTo' else num_rounds
                    running_total = 0
                    for i in range(1, end_round + 1):
                        score = next((s for s in team['scores'] if s['round_num'] == i), None)
                        if score and score.get('score') is not None:
                            row.append(f"{score['score']:.2f}" if isinstance(score['score'], float) else str(score['score']))
                            running_total += score['score'] or 0
                        else:
                            row.append('-')
                    row.append(str(int(running_total)))

            table_data.append(row)

        # Create table for this class
        if event_type in ['cp_dsz', 'ws_performance']:
            num_cols = len(header_row1)
            # Compact columns to fit all data on page
            col_widths = [0.3*inch, 1.2*inch] + [0.4*inch] * (num_cols - 3) + [0.5*inch]
        else:
            num_cols = len(header)
            col_widths = [0.5*inch, 2*inch] + [0.5*inch] * (num_cols - 3) + [0.7*inch]

        table = Table(table_data, colWidths=col_widths)

        # Determine header row count
        header_rows = 1 if event_type not in ['cp_dsz', 'ws_performance'] else 1  # Single header row visually (row2 is hidden)
        data_start_row = 2 if event_type in ['cp_dsz', 'ws_performance'] else 1

        # InTime-style professional table formatting
        style_commands = [
            # Header row - dark blue background
            ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.0, 0.25, 0.4)),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 5),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
        ]

        if event_type in ['cp_dsz', 'ws_performance']:
            # Style the second header row (Score/Points sub-labels)
            style_commands.extend([
                ('BACKGROUND', (0, 1), (-1, 1), colors.Color(0.1, 0.35, 0.5)),
                ('TEXTCOLOR', (0, 1), (-1, 1), colors.white),
                ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, 1), 6),
                ('TOPPADDING', (0, 1), (-1, 1), 2),
                ('BOTTOMPADDING', (0, 1), (-1, 1), 2),
                ('VALIGN', (0, 1), (-1, 1), 'MIDDLE'),
            ])
            # Add span commands for round labels
            style_commands.extend(span_commands)

        style_commands.extend([
            # Data rows
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (1, data_start_row), (1, -1), 'LEFT'),
            ('FONTNAME', (0, data_start_row), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, data_start_row), (-1, -1), 7),
            ('TOPPADDING', (0, data_start_row), (-1, -1), 4),
            ('BOTTOMPADDING', (0, data_start_row), (-1, -1), 4),
            # Alternating row colors (starting from data rows)
            ('ROWBACKGROUNDS', (0, data_start_row), (-1, -1), [colors.white, colors.Color(0.94, 0.96, 0.98)]),
            # Grid borders around all cells
            ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.7, 0.7, 0.7)),
            # Outer border (darker)
            ('BOX', (0, 0), (-1, -1), 1, colors.Color(0.3, 0.3, 0.3)),
            # Header bottom border
            ('LINEBELOW', (0, data_start_row - 1), (-1, data_start_row - 1), 1, colors.Color(0.0, 0.2, 0.35)),
            # Make rank and total columns bold
            ('FONTNAME', (0, data_start_row), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (-1, data_start_row), (-1, -1), 'Helvetica-Bold'),
        ])

        table.setStyle(TableStyle(style_commands))

        elements.append(table)
        elements.append(Spacer(1, 0.3*inch))

    # Get current date/time for signature (but don't display generated time)
    now = datetime.now()
    print_datetime = now.strftime("%B %d, %Y at %I:%M %p")

    # Chief Judge signature (only if PIN verified)
    if chief_judge_name and pin_verified:
        elements.append(Spacer(1, 0.4*inch))

        # Check if user has a drawn signature
        chief_judge_user = get_user(competition.get('chief_judge', ''))
        signature_data = chief_judge_user.get('signature_data', '') if chief_judge_user else ''

        # Create signature block
        sig_width = 250
        sig_height = 100 if signature_data else 80

        # Create a drawing for the signature
        d = Drawing(sig_width, sig_height)

        # Add a light border/box
        d.add(Rect(0, 0, sig_width, sig_height, strokeColor=colors.Color(0.7, 0.7, 0.7),
                   fillColor=colors.Color(0.98, 0.98, 0.98), strokeWidth=0.5))

        # Add "OFFICIAL SIGNATURE" header
        d.add(String(sig_width/2, sig_height - 12, "OFFICIAL SIGNATURE",
                    fontSize=8, fillColor=colors.Color(0.5, 0.5, 0.5),
                    textAnchor='middle'))

        # Add timestamp
        d.add(String(sig_width/2, sig_height - 25, f"Electronically signed: {print_datetime}",
                    fontSize=7, fillColor=colors.Color(0.5, 0.5, 0.5),
                    textAnchor='middle'))

        # Add signature line
        d.add(Line(20, 25, sig_width - 20, 25, strokeColor=colors.Color(0.3, 0.3, 0.3), strokeWidth=0.5))

        # Add title below signature line
        d.add(String(sig_width/2, 10, "Chief Judge",
                    fontSize=9, fillColor=colors.Color(0.3, 0.3, 0.3),
                    textAnchor='middle'))

        # Add name below title
        d.add(String(sig_width/2, 2, chief_judge_name,
                    fontSize=7, fillColor=colors.Color(0.4, 0.4, 0.4),
                    textAnchor='middle'))

        # Wrap drawing in a right-aligned table to position it
        sig_table = Table([[d]], colWidths=[sig_width])
        sig_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'RIGHT'),
        ]))
        elements.append(sig_table)

        # If user has a drawn signature, add it as an image on top
        if signature_data:
            import base64
            try:
                # Remove the data URL prefix
                if signature_data.startswith('data:image/png;base64,'):
                    base64_data = signature_data.split(',')[1]
                else:
                    base64_data = signature_data

                # Decode and create image
                sig_image_data = base64.b64decode(base64_data)
                sig_image_buffer = BytesIO(sig_image_data)

                # Create image with reportlab
                sig_img = Image(sig_image_buffer, width=180, height=45)

                # Add to a right-aligned table
                sig_img_table = Table([[sig_img]], colWidths=[sig_width])
                sig_img_table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'RIGHT'),
                    ('TOPPADDING', (0, 0), (-1, -1), -60),  # Overlap with signature box
                ]))
                elements.append(sig_img_table)
            except Exception as e:
                print(f"Error adding signature image to PDF: {e}")

    doc.build(elements)

    buffer.seek(0)
    filename = f"{competition['name'].replace(' ', '_')}_Results_{now.strftime('%Y%m%d_%H%M')}.pdf"

    return Response(
        buffer.getvalue(),
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )


@app.route('/admin/competition/<comp_id>/add-team', methods=['POST'])
@admin_required
def add_team(comp_id):
    """Add a team to a competition."""
    data = request.json

    team_name = data.get('team_name', '').strip()
    team_number = data.get('team_number', '').strip()
    team_class = data.get('class', 'open').lower()
    members = data.get('members', '').strip()
    category = data.get('category', '').strip()
    event = data.get('event', '').strip()

    if not team_name or not team_number:
        return jsonify({'error': 'Team name and number are required'}), 400

    team_id = str(uuid.uuid4())[:8]

    save_team({
        'id': team_id,
        'competition_id': comp_id,
        'team_number': team_number,
        'team_name': team_name,
        'class': team_class,
        'members': members,
        'category': category,
        'event': event,
        'created_at': datetime.now().isoformat()
    })

    return jsonify({'success': True, 'id': team_id, 'message': 'Team added'})


def find_csv_column(headers, possible_names):
    """
    Loose header matching for CSV imports.
    Finds a column by checking various possible names (case-insensitive, partial matches).
    Returns the actual header name if found, None otherwise.
    """
    headers_lower = {h.lower().strip().replace(' ', '_').replace('-', '_'): h for h in headers}

    for name in possible_names:
        name_normalized = name.lower().strip().replace(' ', '_').replace('-', '_')
        # Exact match (normalized)
        if name_normalized in headers_lower:
            return headers_lower[name_normalized]
        # Partial match - header contains the search term
        for h_lower, h_orig in headers_lower.items():
            if name_normalized in h_lower or h_lower in name_normalized:
                return h_orig
    return None


def get_csv_value(row, headers, possible_names):
    """Get value from CSV row using loose header matching."""
    col = find_csv_column(headers, possible_names)
    if col and col in row:
        return row[col]
    return ''


@app.route('/admin/competition/<comp_id>/import-teams', methods=['POST'])
@admin_required
def import_teams(comp_id):
    """Import teams from CSV."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    try:
        import csv
        import io

        # Read CSV content
        content = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))

        # Check for required columns in header using loose matching
        headers = reader.fieldnames or []

        # Possible names for each required column
        name_variants = ['name', 'team_name', 'teamname', 'competitor', 'competitor_name', 'athlete', 'athlete_name', 'full_name', 'fullname']
        class_variants = ['class', 'category', 'division', 'skill_class', 'skill_level', 'level']
        event_variants = ['event', 'event_type', 'eventtype', 'discipline', 'event_name']

        # Check for name column (required)
        name_col = find_csv_column(headers, name_variants)
        if not name_col:
            return jsonify({'error': f'Missing required column: name. Expected one of: {", ".join(name_variants[:4])}'}), 400

        # Check for class column (required)
        class_col = find_csv_column(headers, class_variants)
        if not class_col:
            return jsonify({'error': f'Missing required column: class. Expected one of: {", ".join(class_variants[:4])}'}), 400

        # Check for event column (required)
        event_col = find_csv_column(headers, event_variants)
        if not event_col:
            return jsonify({'error': f'Missing required column: event. Expected one of: {", ".join(event_variants[:4])}'}), 400

        # Optional columns
        number_variants = ['team_number', 'teamnumber', 'number', 'num', 'id', 'competitor_number', 'bib', 'bib_number']
        members_variants = ['members', 'team_members', 'teammembers', 'country', 'nationality', 'nation', 'federation', 'club']

        imported = 0
        errors = []
        row_num = 1  # Start at 1 since header is row 0

        import_type = request.form.get('import_type', 'teams')

        for row in reader:
            row_num += 1
            try:
                # Use loose matching to get values
                team_number = get_csv_value(row, headers, number_variants)
                team_name = get_csv_value(row, headers, name_variants)
                members = get_csv_value(row, headers, members_variants)
                row_class = get_csv_value(row, headers, class_variants)
                event_type = get_csv_value(row, headers, event_variants)

                # Validate required fields
                if not team_name.strip():
                    errors.append(f'Row {row_num}: Missing required field "name"')
                    continue
                if not row_class.strip():
                    errors.append(f'Row {row_num}: Missing required field "class"')
                    continue
                if not event_type.strip():
                    errors.append(f'Row {row_num}: Missing required field "event"')
                    continue

                # Normalize event type for flexible matching (e.g., "4 way fs" -> "fs_4way_fs")
                normalized_event = normalize_event_type(event_type)

                team_id = str(uuid.uuid4())[:8]

                save_team({
                    'id': team_id,
                    'competition_id': comp_id,
                    'team_number': team_number.strip(),
                    'team_name': team_name.strip(),
                    'class': row_class.lower().strip(),
                    'members': members.strip(),
                    'event': normalized_event,
                    'created_at': datetime.now().isoformat()
                })
                imported += 1

            except Exception as e:
                errors.append(f'Row {row_num}: {str(e)}')

        if imported == 0 and errors:
            return jsonify({'error': f'No rows imported. Errors: {"; ".join(errors[:5])}'}), 400

        return jsonify({
            'success': True,
            'message': f'Imported {imported} teams/competitors' + (f' ({len(errors)} errors)' if errors else ''),
            'imported': imported,
            'errors': errors
        })

    except Exception as e:
        return jsonify({'error': f'Failed to parse CSV: {str(e)}'}), 400


@app.route('/admin/competition/<comp_id>/renumber', methods=['POST'])
@admin_required
def renumber_teams(comp_id):
    """Renumber all teams/competitors in a competition sequentially by class."""
    try:
        data = request.json or {}
        class_start_numbers = data.get('class_start_numbers', {
            'open': 1,
            'advanced': 101,
            'intermediate': 201,
            'beginner': 301
        })

        # Get all teams for this competition
        teams = get_competition_teams(comp_id)

        if not teams:
            return jsonify({'error': 'No teams found'}), 404

        # Group teams by class
        teams_by_class = {}
        for team in teams:
            team_class = team.get('class', 'open').lower()
            if team_class not in teams_by_class:
                teams_by_class[team_class] = []
            teams_by_class[team_class].append(team)

        # Sort each class by current number
        for team_class in teams_by_class:
            teams_by_class[team_class].sort(
                key=lambda t: int(t.get('team_number', 0)) if str(t.get('team_number', '')).isdigit() else 999999
            )

        # Renumber each class starting from its start number
        renumbered = 0
        for team_class, class_teams in teams_by_class.items():
            start_num = int(class_start_numbers.get(team_class, class_start_numbers.get('open', 1)))

            for idx, team in enumerate(class_teams):
                new_number = str(start_num + idx)
                if team.get('team_number') != new_number:
                    # Update the team number
                    team_data = {
                        'id': team['id'],
                        'competition_id': comp_id,
                        'team_number': new_number,
                        'team_name': team['team_name'],
                        'class': team.get('class', 'open'),
                        'members': team.get('members', ''),
                        'category': team.get('category', ''),
                        'event': team.get('event', ''),
                        'photo': team.get('photo', ''),
                        'created_at': team.get('created_at', datetime.now().isoformat())
                    }
                    save_team(team_data)
                    renumbered += 1

        return jsonify({
            'success': True,
            'message': f'Renumbered {renumbered} entries (total: {len(teams)})'
        })

    except Exception as e:
        return jsonify({'error': f'Failed to renumber: {str(e)}'}), 400


@app.route('/admin/team/<team_id>/update', methods=['POST'])
@admin_required
def update_team(team_id):
    """Update a team."""
    data = request.json

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Update team data
    team_data = {
        'id': team_id,
        'competition_id': team['competition_id'],
        'team_number': data.get('team_number', team['team_number']),
        'team_name': data.get('team_name', team['team_name']),
        'class': data.get('class', team['class']),
        'members': data.get('members', team.get('members', '')),
        'category': data.get('category', team.get('category', '')),
        'event': data.get('event', team.get('event', '')),
        'photo': data.get('photo', team.get('photo', '')),
        'display_order': data.get('display_order', team.get('display_order', 0)),
        'created_at': team['created_at']
    }

    save_team(team_data)
    return jsonify({'success': True, 'message': 'Team updated'})


@app.route('/admin/competition/<comp_id>/update-team-order', methods=['POST'])
@admin_required
def update_team_order(comp_id):
    """Update display order for multiple teams."""
    data = request.json
    orders = data.get('orders', [])  # List of {team_id, display_order}

    if USE_SUPABASE:
        for item in orders:
            supabase.table('competition_teams').update({'display_order': item['display_order']}).eq('id', item['team_id']).execute()
    else:
        db = get_sqlite_db()
        for item in orders:
            db.execute('UPDATE competition_teams SET display_order = ? WHERE id = ?',
                      (item['display_order'], item['team_id']))
        db.commit()

    return jsonify({'success': True, 'message': 'Order updated'})


@app.route('/admin/team/<team_id>/upload-photo', methods=['POST'])
@admin_required
def upload_team_photo(team_id):
    """Upload a photo for a team."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    allowed_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.webp')

    if ext not in allowed_extensions:
        return jsonify({'error': f'Invalid file type. Allowed: {", ".join(allowed_extensions)}'}), 400

    # Save file
    photo_filename = f"team_{team_id}{ext}"
    photo_path = os.path.join(VIDEOS_FOLDER, photo_filename)
    file.save(photo_path)

    # Update team with photo path
    team['photo'] = f"/static/videos/{photo_filename}"
    save_team(team)

    return jsonify({
        'success': True,
        'photo_url': team['photo']
    })


@app.route('/admin/team/<team_id>/delete', methods=['POST'])
@admin_required
def delete_team(team_id):
    """Delete a team."""
    delete_team_db(team_id)
    return jsonify({'success': True, 'message': 'Team deleted'})


@app.route('/admin/team/<team_id>/round/<int:round_num>/remove-video', methods=['POST'])
@admin_required
def remove_round_video(team_id, round_num):
    """Remove video from a round (admin only)."""
    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Find the score record for this round
    existing_scores = get_team_scores(team_id)
    existing = next((s for s in existing_scores if s['round_num'] == round_num), None)

    if not existing:
        return jsonify({'error': 'No score record found for this round'}), 404

    if not existing.get('video_id'):
        return jsonify({'error': 'No video linked to this round'}), 400

    # Optionally delete the video file
    delete_file = request.json.get('delete_file', False) if request.json else False
    video_id = existing['video_id']

    if delete_file and video_id:
        video = get_video(video_id)
        if video:
            # Delete local file if exists
            if video.get('local_file'):
                local_path = os.path.join(VIDEOS_FOLDER, video['local_file'])
                if os.path.exists(local_path):
                    os.remove(local_path)
            # Delete thumbnail if exists
            if video.get('thumbnail') and video['thumbnail'].startswith('/static/videos/'):
                thumb_path = os.path.join(VIDEOS_FOLDER, os.path.basename(video['thumbnail']))
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
            # Delete from database
            delete_video_db(video_id)

    # Update the score record to remove video_id
    save_score({
        'id': existing['id'],
        'competition_id': team['competition_id'],
        'team_id': team_id,
        'round_num': round_num,
        'score': existing.get('score'),
        'score_data': existing.get('score_data', ''),
        'video_id': '',  # Clear video link
        'created_at': existing['created_at']
    })

    return jsonify({'success': True, 'message': 'Video removed from round', 'deleted_file': delete_file})


@app.route('/admin/team/<team_id>/score', methods=['POST'])
@event_judge_required
def save_team_score(team_id):
    """Save a score for a team (event judge and above)."""
    data = request.json

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    round_num = int(data.get('round_num', 1))
    score_val = data.get('score')
    raw_score = float(score_val) if score_val is not None else None
    score_data = data.get('score_data', '')
    video_id = data.get('video_id', '')
    exit_time_penalty = int(data.get('exit_time_penalty', 0))

    # Apply 20% penalty for CF events when working time cannot be determined
    # Penalty is rounded down per USPA rules
    score = raw_score
    penalty_amount = 0
    if raw_score is not None and exit_time_penalty:
        penalty_amount = int(raw_score * 0.20)  # 20% rounded down
        score = raw_score - penalty_amount

    # Check if score already exists for this round
    existing_scores = get_team_scores(team_id)
    existing = next((s for s in existing_scores if s['round_num'] == round_num), None)

    if existing:
        score_id = existing['id']
        # Preserve existing video_id if not provided
        if not video_id and existing.get('video_id'):
            video_id = existing['video_id']
    else:
        score_id = str(uuid.uuid4())[:8]

    # Record who scored (only if score is being set)
    scored_by = session.get('username', '') if score is not None else (existing.get('scored_by', '') if existing else '')

    # Store raw score and penalty info in score_data if penalty applied
    if exit_time_penalty and raw_score is not None:
        score_data = f"Raw: {int(raw_score)}, Penalty: -{penalty_amount} (20%)"

    save_score({
        'id': score_id,
        'competition_id': team['competition_id'],
        'team_id': team_id,
        'round_num': round_num,
        'score': score,
        'score_data': score_data,
        'video_id': video_id,
        'scored_by': scored_by,
        'rejump': 0,  # Clear rejump flag when new data is saved
        'exit_time_penalty': exit_time_penalty,
        'created_at': datetime.now().isoformat()
    })

    response_data = {'success': True, 'message': 'Score saved', 'score_id': score_id}
    if exit_time_penalty and penalty_amount > 0:
        response_data['penalty_applied'] = True
        response_data['raw_score'] = int(raw_score)
        response_data['penalty_amount'] = penalty_amount
        response_data['final_score'] = int(score)
    return jsonify(response_data)


@app.route('/admin/team/<team_id>/rejump', methods=['POST'])
@event_judge_required
def award_rejump(team_id):
    """Award a rejump for a team's round - clears score and allows new video upload."""
    data = request.json
    round_num = int(data.get('round_num', 0))

    if not round_num:
        return jsonify({'error': 'Round number is required'}), 400

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Find the score record for this round
    existing_scores = get_team_scores(team_id)
    existing = next((s for s in existing_scores if s['round_num'] == round_num), None)

    if existing:
        # Clear the score and video, mark as rejump
        save_score({
            'id': existing['id'],
            'competition_id': team['competition_id'],
            'team_id': team_id,
            'round_num': round_num,
            'score': None,
            'score_data': '',
            'video_id': '',
            'scored_by': '',
            'rejump': 1,
            'created_at': existing.get('created_at', datetime.now().isoformat())
        })
    else:
        # Create a new score record marked as rejump
        score_id = str(uuid.uuid4())[:8]
        save_score({
            'id': score_id,
            'competition_id': team['competition_id'],
            'team_id': team_id,
            'round_num': round_num,
            'score': None,
            'score_data': '',
            'video_id': '',
            'scored_by': '',
            'rejump': 1,
            'created_at': datetime.now().isoformat()
        })

    return jsonify({'success': True, 'message': f'Rejump awarded for Round {round_num}'})


@app.route('/admin/team/<team_id>/clear-rejump', methods=['POST'])
@event_judge_required
def clear_rejump(team_id):
    """Clear the rejump status for a team's round."""
    data = request.json
    round_num = int(data.get('round_num', 0))

    if not round_num:
        return jsonify({'error': 'Round number is required'}), 400

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Find the score record for this round
    existing_scores = get_team_scores(team_id)
    existing = next((s for s in existing_scores if s['round_num'] == round_num), None)

    if existing:
        save_score({
            'id': existing['id'],
            'competition_id': team['competition_id'],
            'team_id': team_id,
            'round_num': round_num,
            'score': existing.get('score'),
            'score_data': existing.get('score_data', ''),
            'video_id': existing.get('video_id', ''),
            'scored_by': existing.get('scored_by', ''),
            'rejump': 0,
            'created_at': existing.get('created_at', datetime.now().isoformat())
        })

    return jsonify({'success': True, 'message': f'Rejump cleared for Round {round_num}'})


@app.route('/api/competition/<comp_id>/approve-scores', methods=['POST'])
def approve_scores(comp_id):
    """Approve scores for a round (requires Chief Judge PIN)."""
    data = request.json
    pin = data.get('pin', '')
    round_num = int(data.get('round_num', 0))
    event_type = data.get('event_type', 'default')

    if not round_num:
        return jsonify({'error': 'Round number is required'}), 400

    # Verify PIN - check competition-specific PIN first, then global
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    comp_pin = competition.get('chief_judge_pin', '')
    valid_pin = comp_pin if comp_pin else CHIEF_JUDGE_PIN

    if pin != valid_pin:
        return jsonify({'error': 'Invalid Chief Judge PIN'}), 403

    # Load existing approvals
    approvals = {}
    if competition.get('score_approvals'):
        try:
            approvals = json.loads(competition['score_approvals'])
        except:
            approvals = {}

    # Add this approval
    if event_type not in approvals:
        approvals[event_type] = {}

    approvals[event_type][str(round_num)] = {
        'approved_at': datetime.now().isoformat(),
        'approved_by': session.get('username', 'Chief Judge')
    }

    # Save to competition
    if USE_SUPABASE:
        supabase.table('competitions').update({
            'score_approvals': json.dumps(approvals)
        }).eq('id', comp_id).execute()
    else:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE competitions SET score_approvals = ? WHERE id = ?',
                      (json.dumps(approvals), comp_id))
        conn.commit()
        conn.close()

    return jsonify({
        'success': True,
        'message': f'Round {round_num} scores approved',
        'approved_at': approvals[event_type][str(round_num)]['approved_at']
    })


@app.route('/api/competition/<comp_id>/score-approvals')
def get_score_approvals(comp_id):
    """Get score approval status for a competition."""
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    approvals = {}
    if competition.get('score_approvals'):
        try:
            approvals = json.loads(competition['score_approvals'])
        except:
            approvals = {}

    return jsonify({'approvals': approvals})


@app.route('/admin/get-video-info/<video_id>')
@admin_required
def get_video_info(video_id):
    """Get video file info for embedding."""
    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    video_url = ''
    if video.get('local_file'):
        video_url = f'/static/videos/{video["local_file"]}'
    elif video.get('url'):
        video_url = video['url']

    # Handle None start_time (from older records)
    start_time = video.get('start_time')
    if start_time is None:
        start_time = 0

    return jsonify({
        'id': video_id,
        'title': video.get('title', ''),
        'url': video_url,
        'local_file': video.get('local_file', ''),
        'start_time': start_time
    })


# ===========================================
# Videographer Routes (no admin login required)
# ===========================================

@app.route('/videographer/upload-video', methods=['POST'])
@chief_judge_required
def videographer_upload_video():
    """Upload a video file (chief judge and above)."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Get form data
    title = request.form.get('title', '').strip()
    category = request.form.get('category', '')
    subcategory = request.form.get('subcategory', '')
    event = request.form.get('event', '').strip()
    background = request.form.get('background', 'true').lower() == 'true'

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    allowed_extensions = ('.mp4', '.webm', '.mov', '.m4v', '.ogg', '.ogv', '.mts', '.m2ts', '.avi', '.mkv')

    if ext not in allowed_extensions:
        return jsonify({'error': f'Invalid file type. Allowed: {", ".join(allowed_extensions)}'}), 400

    # Videographer uploads are manually assigned to team/round slots
    # No auto-categorization - category comes from competition context
    if not category:
        category = 'fs'  # Default category for competition videos

    video_id = str(uuid.uuid4())[:8]

    # Generate title from filename if not provided
    if not title:
        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

    needs_conversion = ext in CONVERSION_FORMATS

    try:
        if needs_conversion and background:
            # Background conversion - save file and start thread
            import tempfile
            temp_path = os.path.join(tempfile.gettempdir(), f"{video_id}_input{ext}")
            file.save(temp_path)

            output_filename = f"{video_id}.mp4"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)

            # Create job tracking entry
            job_id = str(uuid.uuid4())[:8]
            session_id = session.get('_id', request.remote_addr)

            video_data = {
                'id': video_id,
                'title': title,
                'description': '',
                'url': '',
                'thumbnail': None,
                'category': category,
                'subcategory': subcategory,
                'tags': '',
                'duration': None,
                'created_at': datetime.now().isoformat(),
                'views': 0,
                'video_type': 'local',
                'local_file': output_filename,
                'event': event,
                'category_auto': False  # Videographer uploads are manually assigned
            }

            with conversion_lock:
                conversion_jobs[job_id] = {
                    'job_id': job_id,
                    'video_id': video_id,
                    'filename': filename,
                    'title': title,
                    'status': 'queued',
                    'progress': 0,
                    'session_id': session_id,
                    'created_at': datetime.now().isoformat(),
                    'error': None
                }

            # Start background thread
            thread = threading.Thread(
                target=background_convert_video,
                args=(job_id, temp_path, output_path, video_data, temp_path)
            )
            thread.daemon = True
            thread.start()

            return jsonify({
                'success': True,
                'background': True,
                'job_id': job_id,
                'video_id': video_id,
                'message': 'Video upload started - conversion running in background'
            })

        elif needs_conversion:
            # Synchronous conversion (legacy behavior)
            import tempfile
            temp_path = os.path.join(tempfile.gettempdir(), f"{video_id}_input{ext}")
            file.save(temp_path)

            output_filename = f"{video_id}.mp4"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)

            if convert_video_to_mp4(temp_path, output_path):
                os.remove(temp_path)
                local_file = output_filename
            else:
                os.remove(temp_path)
                return jsonify({'error': 'Failed to convert video. Make sure ffmpeg is installed.'}), 400
        else:
            # Save directly (no conversion needed)
            output_filename = f"{video_id}{ext}"
            output_path = os.path.join(VIDEOS_FOLDER, output_filename)
            file.save(output_path)
            local_file = output_filename

        # Generate thumbnail
        thumbnail_filename = f"{video_id}_thumb.jpg"
        thumbnail_path = os.path.join(VIDEOS_FOLDER, thumbnail_filename)
        if generate_thumbnail(output_path, thumbnail_path):
            thumbnail = f"/static/videos/{thumbnail_filename}"
        else:
            thumbnail = None

        # Get duration
        duration = get_video_duration(output_path)

        # Upload to Supabase Storage if enabled
        video_url = ''
        video_type = 'local'
        final_local_file = local_file

        if USE_SUPABASE:
            # Upload video file
            supabase_video_url = upload_to_supabase_storage(output_path, f"videos/{local_file}")
            if supabase_video_url:
                video_url = supabase_video_url
                video_type = 'url'
                final_local_file = ''
                # Clean up local file after upload
                if os.path.exists(output_path):
                    os.remove(output_path)

            # Upload thumbnail
            if thumbnail and os.path.exists(thumbnail_path):
                thumb_url = upload_to_supabase_storage(thumbnail_path, f"thumbnails/{thumbnail_filename}")
                if thumb_url:
                    thumbnail = thumb_url
                    os.remove(thumbnail_path)

        # Save to database
        save_video({
            'id': video_id,
            'title': title,
            'description': '',
            'url': video_url,
            'thumbnail': thumbnail,
            'category': category,
            'subcategory': subcategory,
            'tags': '',
            'duration': duration,
            'created_at': datetime.now().isoformat(),
            'views': 0,
            'video_type': video_type,
            'local_file': final_local_file,
            'event': event,
            'category_auto': False  # Videographer uploads are manually assigned
        })

        return jsonify({
            'success': True,
            'message': 'Video uploaded successfully',
            'id': video_id,
            'converted': needs_conversion
        })

    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@app.route('/videographer/upload-flysight', methods=['POST'])
@chief_judge_required
def videographer_upload_flysight():
    """Upload a FlysSight CSV file for Speed Skydiving (chief judge and above)."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Get form data
    title = request.form.get('title', '').strip()
    event = request.form.get('event', '').strip()

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()

    if ext != '.csv':
        return jsonify({'error': 'Invalid file type. Only CSV files are allowed for FlysSight data.'}), 400

    flysight_id = str(uuid.uuid4())[:8]

    # Generate title from filename if not provided
    if not title:
        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')

    try:
        # Create flysight directory if it doesn't exist
        flysight_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'flysight')
        os.makedirs(flysight_folder, exist_ok=True)

        # Save the file
        output_filename = f"{flysight_id}.csv"
        output_path = os.path.join(flysight_folder, output_filename)
        file.save(output_path)

        # Save to database (using videos table with special category)
        save_video({
            'id': flysight_id,
            'title': title,
            'description': 'FlysSight GPS data',
            'url': '',
            'thumbnail': None,
            'category': 'sp',
            'subcategory': '',
            'tags': 'flysight,gps,speed',
            'duration': '',
            'created_at': datetime.now().isoformat(),
            'views': 0,
            'video_type': 'flysight',
            'local_file': output_filename,
            'event': event
        })

        return jsonify({
            'success': True,
            'message': 'FlysSight data uploaded successfully',
            'id': flysight_id
        })

    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


def parse_flysight_csv(file_content):
    """Parse FlySight CSV and extract competition window data for WS Performance.

    Competition window: 3000m to 2000m altitude (hMSL)
    Returns: time (seconds), distance (meters), speed (km/h)
    """
    import csv
    import io
    import math

    lines = file_content.decode('utf-8').splitlines()

    # Find the header row (skip $ prefixed metadata lines)
    header_idx = 0
    for i, line in enumerate(lines):
        if not line.startswith('$') and line.strip():
            header_idx = i
            break

    # Parse CSV
    reader = csv.DictReader(lines[header_idx:])

    data_points = []
    for row in reader:
        try:
            # FlySight columns: time,lat,lon,hMSL,velN,velE,velD,hAcc,vAcc,sAcc,numSV
            point = {
                'time': row.get('time', ''),
                'lat': float(row.get('lat', 0)),
                'lon': float(row.get('lon', 0)),
                'hMSL': float(row.get('hMSL', 0)),  # Altitude in meters
                'velN': float(row.get('velN', 0)),  # North velocity m/s
                'velE': float(row.get('velE', 0)),  # East velocity m/s
                'velD': float(row.get('velD', 0)),  # Down velocity m/s
            }
            data_points.append(point)
        except (ValueError, KeyError):
            continue

    if not data_points:
        return None, "No valid data points found in CSV"

    # Find competition window (3000m to 2000m)
    window_start = None
    window_end = None
    window_points = []

    for i, point in enumerate(data_points):
        alt = point['hMSL']

        # Find when we first drop below 3000m (entering window)
        if window_start is None and alt <= 3000:
            window_start = i

        # Collect points in the window
        if window_start is not None and alt >= 2000:
            window_points.append(point)

        # Find when we drop below 2000m (exiting window)
        if window_start is not None and alt < 2000:
            window_end = i
            break

    if not window_points:
        return None, "Could not find competition window (3000m-2000m) in data"

    # Calculate metrics
    # Time: duration in competition window
    if len(window_points) >= 2:
        # Parse timestamps (format: YYYY-MM-DDTHH:MM:SS.sssZ or similar)
        from datetime import datetime
        try:
            t_start = datetime.fromisoformat(window_points[0]['time'].replace('Z', '+00:00'))
            t_end = datetime.fromisoformat(window_points[-1]['time'].replace('Z', '+00:00'))
            time_seconds = (t_end - t_start).total_seconds()
        except:
            # Fallback: estimate from data point count (typically 5Hz)
            time_seconds = len(window_points) / 5.0
    else:
        time_seconds = 0

    # Distance: horizontal distance traveled
    total_distance = 0
    for i in range(1, len(window_points)):
        p1 = window_points[i-1]
        p2 = window_points[i]

        # Haversine formula for distance between GPS points
        lat1, lon1 = math.radians(p1['lat']), math.radians(p1['lon'])
        lat2, lon2 = math.radians(p2['lat']), math.radians(p2['lon'])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371000  # Earth radius in meters

        total_distance += r * c

    # Speed: average horizontal speed in km/h
    if time_seconds > 0:
        speed_kmh = (total_distance / time_seconds) * 3.6  # m/s to km/h
    else:
        # Calculate from velocity components
        total_speed = 0
        for point in window_points:
            horiz_speed = math.sqrt(point['velN']**2 + point['velE']**2)
            total_speed += horiz_speed
        speed_kmh = (total_speed / len(window_points)) * 3.6 if window_points else 0

    return {
        'time': round(time_seconds, 2),
        'distance': round(total_distance, 2),
        'speed': round(speed_kmh, 2),
        'points_in_window': len(window_points)
    }, None


@app.route('/ws-performance/upload-flysight/<team_id>/<int:round_num>', methods=['POST'])
@chief_judge_required
def ws_performance_upload_flysight(team_id, round_num):
    """Upload and parse FlySight CSV for WS Performance scoring.

    round_num mapping:
    1-3: Time rounds (store time value)
    4-6: Distance rounds (store distance value)
    7-9: Speed rounds (store speed value)
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()

    if ext != '.csv':
        return jsonify({'error': 'Invalid file type. Only CSV files are allowed.'}), 400

    # Get team
    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Parse FlySight CSV
    file_content = file.read()
    result, error = parse_flysight_csv(file_content)

    if error:
        return jsonify({'error': error}), 400

    # Determine which value to use based on round number
    # Rounds 1-3: Time, Rounds 4-6: Distance, Rounds 7-9: Speed
    if round_num <= 3:
        score_value = result['time']
        task_type = 'Time'
    elif round_num <= 6:
        score_value = result['distance']
        task_type = 'Distance'
    else:
        score_value = result['speed']
        task_type = 'Speed'

    # Save FlySight file
    flysight_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'flysight')
    os.makedirs(flysight_folder, exist_ok=True)

    flysight_id = f"{team_id}_r{round_num}_{str(uuid.uuid4())[:8]}"
    output_path = os.path.join(flysight_folder, f"{flysight_id}.csv")

    with open(output_path, 'wb') as f:
        f.write(file_content)

    # Update team's score for this round
    scores = team.get('scores', [])
    score_updated = False

    for score in scores:
        if score.get('round_num') == round_num:
            score['score'] = score_value
            score['score_data'] = json.dumps({
                'flysight_file': f"{flysight_id}.csv",
                'time': result['time'],
                'distance': result['distance'],
                'speed': result['speed'],
                'task_type': task_type
            })
            score['scored_by'] = session.get('username', 'system')
            score_updated = True
            break

    if not score_updated:
        scores.append({
            'id': str(uuid.uuid4())[:8],
            'round_num': round_num,
            'score': score_value,
            'score_data': json.dumps({
                'flysight_file': f"{flysight_id}.csv",
                'time': result['time'],
                'distance': result['distance'],
                'speed': result['speed'],
                'task_type': task_type
            }),
            'video_id': None,
            'scored_by': session.get('username', 'system')
        })

    team['scores'] = scores
    save_team(team)

    return jsonify({
        'success': True,
        'message': f'{task_type} score recorded: {score_value}',
        'result': result,
        'score': score_value
    })


@app.route('/ws-performance/bulk-upload-flysight/<team_id>', methods=['POST'])
@chief_judge_required
def ws_performance_bulk_upload_flysight(team_id):
    """Upload FlySight CSV and apply to all three task types for a round.

    Expects round_base (1, 2, or 3) and applies:
    - Time to round_base
    - Distance to round_base + 3
    - Speed to round_base + 6
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    round_base = int(request.form.get('round_base', 1))
    if round_base not in [1, 2, 3]:
        return jsonify({'error': 'Invalid round. Must be 1, 2, or 3.'}), 400

    # Check file extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()

    if ext != '.csv':
        return jsonify({'error': 'Invalid file type. Only CSV files are allowed.'}), 400

    # Get team
    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    # Parse FlySight CSV
    file_content = file.read()
    result, error = parse_flysight_csv(file_content)

    if error:
        return jsonify({'error': error}), 400

    # Save FlySight file
    flysight_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'flysight')
    os.makedirs(flysight_folder, exist_ok=True)

    flysight_id = f"{team_id}_round{round_base}_{str(uuid.uuid4())[:8]}"
    output_path = os.path.join(flysight_folder, f"{flysight_id}.csv")

    with open(output_path, 'wb') as f:
        f.write(file_content)

    # Update scores for all three tasks
    scores = team.get('scores', [])

    tasks = [
        (round_base, result['time'], 'Time'),
        (round_base + 3, result['distance'], 'Distance'),
        (round_base + 6, result['speed'], 'Speed')
    ]

    for round_num, score_value, task_type in tasks:
        score_updated = False
        for score in scores:
            if score.get('round_num') == round_num:
                score['score'] = score_value
                score['score_data'] = json.dumps({
                    'flysight_file': f"{flysight_id}.csv",
                    'time': result['time'],
                    'distance': result['distance'],
                    'speed': result['speed'],
                    'task_type': task_type
                })
                score['scored_by'] = session.get('username', 'system')
                score_updated = True
                break

        if not score_updated:
            scores.append({
                'id': str(uuid.uuid4())[:8],
                'round_num': round_num,
                'score': score_value,
                'score_data': json.dumps({
                    'flysight_file': f"{flysight_id}.csv",
                    'time': result['time'],
                    'distance': result['distance'],
                    'speed': result['speed'],
                    'task_type': task_type
                }),
                'video_id': None,
                'scored_by': session.get('username', 'system')
            })

    team['scores'] = scores
    save_team(team)

    return jsonify({
        'success': True,
        'message': f'Round {round_base} scores recorded from FlySight',
        'result': result,
        'scores': {
            'time': result['time'],
            'distance': result['distance'],
            'speed': result['speed']
        }
    })


@app.route('/ws-performance/save-score/<team_id>', methods=['POST'])
@chief_judge_required
def ws_performance_save_score(team_id):
    """Save a single WS Performance score for a specific task and round."""
    data = request.json

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    round_num = int(data.get('round_num', 1))
    score_value = float(data.get('score', 0))
    task = data.get('task', 'time')  # 'time', 'distance', 'speed'
    raw_score = float(data.get('raw_score', score_value))
    dl_violation = data.get('dl_violation', 'none')

    # Validate round number based on task
    # Time: 1-3, Distance: 4-6, Speed: 7-9
    valid_rounds = {
        'time': [1, 2, 3],
        'distance': [4, 5, 6],
        'speed': [7, 8, 9]
    }

    if round_num not in valid_rounds.get(task, []):
        return jsonify({'error': f'Invalid round number {round_num} for task {task}'}), 400

    # Update or create score
    scores = team.get('scores', [])
    score_updated = False

    for score in scores:
        if score.get('round_num') == round_num:
            score['score'] = score_value
            score['score_data'] = json.dumps({
                'task_type': task.capitalize(),
                'source': 'flysight',
                'raw_score': raw_score,
                'dl_violation': dl_violation
            })
            score['scored_by'] = session.get('username', 'system')
            score_updated = True
            break

    if not score_updated:
        scores.append({
            'id': str(uuid.uuid4())[:8],
            'round_num': round_num,
            'score': score_value,
            'score_data': json.dumps({
                'task_type': task.capitalize(),
                'source': 'flysight',
                'raw_score': raw_score,
                'dl_violation': dl_violation
            }),
            'video_id': None,
            'scored_by': session.get('username', 'system')
        })

    team['scores'] = scores
    save_team(team)

    return jsonify({
        'success': True,
        'message': f'{task.capitalize()} score saved',
        'round_num': round_num,
        'score': score_value
    })


@app.route('/ws-performance/reference-points/<competition_id>', methods=['GET', 'POST'])
@chief_judge_required
def ws_performance_reference_points(competition_id):
    """Get or set ground reference points and validation window for WS Performance competition."""
    competition = get_competition(competition_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    if request.method == 'GET':
        # Return existing reference points, validation window, and competitor assignments
        ref_points = competition.get('ws_reference_points', [])
        validation_window = competition.get('ws_validation_window')
        competitor_assignments = competition.get('ws_competitor_ref_points', {})

        # Parse if stored as JSON string
        if isinstance(ref_points, str):
            try:
                ref_points = json.loads(ref_points)
            except:
                ref_points = []
        if isinstance(validation_window, str):
            try:
                validation_window = json.loads(validation_window)
            except:
                validation_window = None
        if isinstance(competitor_assignments, str):
            try:
                competitor_assignments = json.loads(competitor_assignments)
            except:
                competitor_assignments = {}

        field_elevation = competition.get('ws_field_elevation', 0)

        return jsonify({
            'success': True,
            'points': ref_points,
            'validation_window': validation_window,
            'competitor_assignments': competitor_assignments,
            'field_elevation': field_elevation
        })

    elif request.method == 'POST':
        # Save reference points, validation window, competitor assignments, and field elevation
        data = request.json
        points = data.get('points', [])
        validation_window = data.get('validation_window')
        competitor_assignments = data.get('competitor_assignments', {})
        field_elevation = data.get('field_elevation', 0)

        if len(points) == 0:
            return jsonify({'error': 'At least one reference point is required'}), 400

        if len(points) > 4:
            return jsonify({'error': 'Maximum 4 reference points allowed'}), 400

        # Validate points
        validated_points = []
        for point in points:
            lat = point.get('lat')
            lng = point.get('lng')

            if lat is None or lng is None:
                return jsonify({'error': 'Each point must have lat and lng'}), 400

            if not (-90 <= lat <= 90):
                return jsonify({'error': 'Latitude must be between -90 and 90'}), 400

            if not (-180 <= lng <= 180):
                return jsonify({'error': 'Longitude must be between -180 and 180'}), 400

            validated_points.append({
                'index': point.get('index', len(validated_points)),
                'lat': float(lat),
                'lng': float(lng)
            })

        # Validate validation window if provided
        validated_vw = None
        if validation_window:
            vw_lat = validation_window.get('lat')
            vw_lng = validation_window.get('lng')
            if vw_lat is not None and vw_lng is not None:
                if not (-90 <= vw_lat <= 90) or not (-180 <= vw_lng <= 180):
                    return jsonify({'error': 'Invalid validation window coordinates'}), 400
                validated_vw = {'lat': float(vw_lat), 'lng': float(vw_lng)}

        # Validate field elevation
        try:
            field_elevation = float(field_elevation) if field_elevation else 0
        except:
            field_elevation = 0

        # Save to competition
        competition['ws_reference_points'] = json.dumps(validated_points)
        competition['ws_validation_window'] = json.dumps(validated_vw) if validated_vw else None
        competition['ws_competitor_ref_points'] = json.dumps(competitor_assignments)
        competition['ws_field_elevation'] = field_elevation
        save_competition(competition)

        return jsonify({
            'success': True,
            'message': 'Flight path configuration saved',
            'points': validated_points,
            'validation_window': validated_vw,
            'competitor_assignments': competitor_assignments,
            'field_elevation': field_elevation
        })


@app.route('/ws-performance/assign-ref-point/<competition_id>', methods=['POST'])
@chief_judge_required
def ws_performance_assign_ref_point(competition_id):
    """Quickly assign a reference point to a competitor for WS Performance."""
    competition = get_competition(competition_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404

    data = request.json
    team_id = data.get('team_id')
    ref_point_index = data.get('ref_point_index')

    if team_id is None:
        return jsonify({'error': 'Team ID is required'}), 400

    if ref_point_index is None:
        return jsonify({'error': 'Reference point index is required'}), 400

    # Get existing reference points and assignments
    ref_points = competition.get('ws_reference_points', [])
    if isinstance(ref_points, str):
        try:
            ref_points = json.loads(ref_points)
        except:
            ref_points = []

    if not ref_points:
        return jsonify({'error': 'No reference points configured'}), 400

    if ref_point_index < 0 or ref_point_index >= len(ref_points):
        return jsonify({'error': 'Invalid reference point index'}), 400

    # Get existing competitor assignments
    competitor_assignments = competition.get('ws_competitor_ref_points', {})
    if isinstance(competitor_assignments, str):
        try:
            competitor_assignments = json.loads(competitor_assignments)
        except:
            competitor_assignments = {}

    # Update the assignment
    competitor_assignments[str(team_id)] = ref_point_index

    # Save back to competition
    competition['ws_competitor_ref_points'] = json.dumps(competitor_assignments)
    save_competition(competition)

    return jsonify({
        'success': True,
        'message': 'Reference point assigned',
        'team_id': team_id,
        'ref_point_index': ref_point_index
    })


@app.route('/videographer/team/<team_id>/score', methods=['POST'])
@event_judge_required
def videographer_link_video(team_id):
    """Link a video to a team's round (videographer - NO score entry allowed)."""
    data = request.json

    team = get_team(team_id)
    if not team:
        return jsonify({'error': 'Team not found'}), 404

    round_num = int(data.get('round_num', 1))
    video_id = data.get('video_id', '')

    if not video_id:
        return jsonify({'error': 'Video ID is required'}), 400

    # Check if score already exists for this round
    existing_scores = get_team_scores(team_id)
    existing = next((s for s in existing_scores if s['round_num'] == round_num), None)

    if existing:
        score_id = existing['id']
        # Preserve existing score - videographer cannot modify scores
        score = existing.get('score')
        score_data = existing.get('score_data', '')
        scored_by = existing.get('scored_by', '')
    else:
        score_id = str(uuid.uuid4())[:8]
        score = None
        score_data = ''
        scored_by = ''

    save_score({
        'id': score_id,
        'competition_id': team['competition_id'],
        'team_id': team_id,
        'round_num': round_num,
        'score': score,  # Preserved from existing or None
        'score_data': score_data,  # Preserved from existing
        'video_id': video_id,
        'scored_by': scored_by,  # Preserved from existing
        'rejump': 0,  # Clear rejump flag when new video is uploaded
        'created_at': datetime.now().isoformat()
    })

    return jsonify({'success': True, 'message': 'Video linked to round'})


@app.route('/videographer/get-video-info/<video_id>')
def videographer_get_video_info(video_id):
    """Get video file info for embedding (no admin required)."""
    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    video_url = ''
    if video.get('local_file'):
        video_url = f'/static/videos/{video["local_file"]}'
    elif video.get('url'):
        video_url = video['url']

    # Handle None start_time (from older records)
    start_time = video.get('start_time')
    if start_time is None:
        start_time = 0

    return jsonify({
        'id': video_id,
        'title': video.get('title', ''),
        'url': video_url,
        'local_file': video.get('local_file', ''),
        'start_time': start_time
    })


@app.route('/videographer')
@chief_judge_required
def videographer_upload_page():
    """Videographer upload page (chief judge and above)."""
    try:
        competitions = get_all_competitions()
        return render_template('videographer.html',
                             competitions=competitions,
                             categories=CATEGORIES)
    except Exception as e:
        print(f"Error in videographer_upload_page: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading videographer page: {str(e)}", 500


@app.route('/api/competitions')
def api_get_competitions():
    """API endpoint to get all competitions."""
    competitions = get_all_competitions()
    return jsonify(competitions)


@app.route('/api/competition/<comp_id>/teams')
def api_get_competition_teams(comp_id):
    """API endpoint to get teams for a competition."""
    teams = get_competition_teams(comp_id)
    # Also get scores for each team to show which rounds have videos
    for team in teams:
        team['scores'] = get_team_scores(team['id'])
    return jsonify(teams)


@app.route('/api/competition/<comp_id>')
def api_get_competition(comp_id):
    """API endpoint to get competition details."""
    competition = get_competition(comp_id)
    if not competition:
        return jsonify({'error': 'Competition not found'}), 404
    return jsonify(competition)


@app.route('/debug/status')
def debug_status():
    """Debug endpoint to check system status."""
    try:
        # Check database connection
        db_status = "Supabase" if USE_SUPABASE else "SQLite"
        competitions = get_all_competitions()

        # Check for event_types column
        has_event_types = False
        if competitions and len(competitions) > 0:
            has_event_types = 'event_types' in competitions[0]

        return jsonify({
            'status': 'ok',
            'database': db_status,
            'supabase_connected': USE_SUPABASE,
            'competitions_count': len(competitions) if competitions else 0,
            'has_event_types_column': has_event_types,
            'sample_competition': competitions[0] if competitions else None
        })
    except Exception as e:
        import traceback
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


# ==================== SYNC VIEWING ====================

@app.route('/sync-room/create', methods=['POST'])
@login_required
def create_sync_room():
    """Create a sync viewing room (event judge only)."""
    if session.get('role') not in ['admin', 'chief_judge', 'event_judge']:
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.json
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({'error': 'Video ID required'}), 400

    video = get_video(video_id)
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    room_id = str(uuid.uuid4())[:8]
    sync_rooms[room_id] = {
        'video_id': video_id,
        'video': video,
        'event_judge': session.get('username'),
        'judges': {},
        'state': 'waiting',
        'play_time': None,
        'created_at': datetime.now().isoformat()
    }

    return jsonify({'success': True, 'room_id': room_id})


@app.route('/sync-room/<room_id>')
@login_required
def sync_room_page(room_id):
    """Join a sync viewing room."""
    if room_id not in sync_rooms:
        return "Room not found", 404

    room = sync_rooms[room_id]
    video = room.get('video') or get_video(room['video_id'])
    is_event_judge = session.get('username') == room['event_judge']

    return render_template('sync_room.html',
                         room_id=room_id,
                         room=room,
                         video=video,
                         is_event_judge=is_event_judge,
                         username=session.get('username'),
                         categories=CATEGORIES)


@app.route('/sync-room/<room_id>/status')
@login_required
def sync_room_status(room_id):
    """Get current room status."""
    if room_id not in sync_rooms:
        return jsonify({'error': 'Room not found'}), 404

    room = sync_rooms[room_id]
    return jsonify({
        'state': room['state'],
        'judges': room['judges'],
        'play_time': room['play_time']
    })


# SocketIO events for sync viewing
if SOCKETIO_ENABLED:
    @socketio.on('join_sync_room')
    def on_join_sync_room(data):
        room_id = data.get('room_id')
        username = data.get('username')
        is_event_judge = data.get('is_event_judge', False)

        if room_id not in sync_rooms:
            emit('error', {'message': 'Room not found'})
            return

        join_room(room_id)
        room = sync_rooms[room_id]

        if not is_event_judge:
            room['judges'][username] = {
                'ready': False,
                'start_time': None,
                'joined_at': datetime.now().isoformat()
            }

        # Broadcast updated judge list to everyone in room
        emit('room_update', {
            'judges': room['judges'],
            'state': room['state']
        }, room=room_id)

    @socketio.on('leave_sync_room')
    def on_leave_sync_room(data):
        room_id = data.get('room_id')
        username = data.get('username')

        if room_id in sync_rooms:
            leave_room(room_id)
            if username in sync_rooms[room_id]['judges']:
                del sync_rooms[room_id]['judges'][username]
            emit('room_update', {
                'judges': sync_rooms[room_id]['judges'],
                'state': sync_rooms[room_id]['state']
            }, room=room_id)

    @socketio.on('event_judge_play')
    def on_event_judge_play(data):
        """Event judge starts playback - all judges should watch."""
        room_id = data.get('room_id')
        username = data.get('username')

        if room_id not in sync_rooms:
            return

        room = sync_rooms[room_id]
        if username != room['event_judge']:
            emit('error', {'message': 'Only event judge can control playback'})
            return

        import time
        room['state'] = 'syncing'
        room['play_time'] = time.time()
        # Reset all judge ready states
        for judge in room['judges']:
            room['judges'][judge]['ready'] = False
            room['judges'][judge]['start_time'] = None

        # Tell all judges to prepare and press X
        emit('prepare_to_start', {
            'play_time': room['play_time'],
            'message': 'Press X when ready to start video'
        }, room=room_id)

    @socketio.on('judge_start_video')
    def on_judge_start_video(data):
        """Judge pressed X to start video - check timing tolerance."""
        room_id = data.get('room_id')
        username = data.get('username')
        press_time = data.get('press_time')

        if room_id not in sync_rooms:
            return

        room = sync_rooms[room_id]
        if username not in room['judges']:
            return

        import time
        room['judges'][username]['ready'] = True
        room['judges'][username]['start_time'] = press_time

        # Check if all judges have pressed X
        all_ready = all(j['ready'] for j in room['judges'].values())

        if all_ready and len(room['judges']) > 0:
            # Check timing tolerance (0.5 seconds)
            start_times = [j['start_time'] for j in room['judges'].values()]
            time_spread = max(start_times) - min(start_times)

            if time_spread <= 0.5:
                # All within tolerance - play video
                room['state'] = 'playing'
                emit('sync_play', {
                    'message': 'All judges synchronized! Playing video.',
                    'sync_successful': True
                }, room=room_id)
            else:
                # Outside tolerance - reset
                room['state'] = 'waiting'
                for judge in room['judges']:
                    room['judges'][judge]['ready'] = False
                    room['judges'][judge]['start_time'] = None

                emit('sync_failed', {
                    'message': f'Timing spread was {time_spread:.2f}s (max 0.5s). Video reset. Event judge must press Play again.',
                    'time_spread': time_spread
                }, room=room_id)
        else:
            # Update room status - waiting for other judges
            emit('room_update', {
                'judges': room['judges'],
                'state': room['state'],
                'waiting_for': [j for j, d in room['judges'].items() if not d['ready']]
            }, room=room_id)

    @socketio.on('video_ended')
    def on_video_ended(data):
        """Video playback ended."""
        room_id = data.get('room_id')

        if room_id in sync_rooms:
            room = sync_rooms[room_id]
            room['state'] = 'waiting'
            for judge in room['judges']:
                room['judges'][judge]['ready'] = False
                room['judges'][judge]['start_time'] = None

            emit('room_update', {
                'judges': room['judges'],
                'state': room['state']
            }, room=room_id)

    # Panel judging sessions for synchronized multi-judge scoring
    panel_sessions = {}
    WORKING_TIME_TOLERANCE = 0.5  # seconds - all judges must press X within this time

    @socketio.on('create_panel_session')
    def on_create_panel_session(data):
        """Create a new panel judging session (Event Judge only)."""
        video_id = data.get('video_id')
        panel_size = data.get('panel_size', 3)
        judge_name = data.get('judge_name')

        session_id = f"panel_{video_id}_{int(time.time())}"
        panel_sessions[session_id] = {
            'video_id': video_id,
            'panel_size': panel_size,
            'event_judge': judge_name,
            'judges': {},  # {judge_num: {name, connected, ready, x_press_time}}
            'scores': [],
            'state': 'waiting_for_judges',  # waiting_for_judges -> waiting_for_ready -> playing -> waiting_for_x -> scoring -> review
            'video_started': False,
            'x_presses': {},  # {judge_num: timestamp}
            'timer_running': False,
            'timer_start': None
        }

        join_room(session_id)

        emit('panel_session_created', {
            'session_id': session_id,
            'panel_size': panel_size,
            'event_judge': judge_name,
            'state': 'waiting_for_judges'
        })

    @socketio.on('join_panel_session')
    def on_join_panel_session(data):
        """Panel judge joins an existing session."""
        session_id = data.get('session_id')
        judge_name = data.get('judge_name')
        judge_num = data.get('judge_num')

        if session_id not in panel_sessions:
            emit('panel_error', {'error': 'Session not found'})
            return

        session = panel_sessions[session_id]

        # Check if judge number is already taken
        if judge_num in session['judges'] and session['judges'][judge_num]['connected']:
            emit('panel_error', {'error': f'Judge {judge_num} position already taken'})
            return

        join_room(session_id)
        session['judges'][judge_num] = {
            'name': judge_name,
            'connected': True,
            'ready': False,
            'x_press_time': None
        }

        emit('panel_joined', {
            'session_id': session_id,
            'judge_num': judge_num,
            'judges': session['judges'],
            'state': session['state'],
            'panel_size': session['panel_size']
        })

        # Notify all judges in session
        emit('panel_update', {
            'judges': session['judges'],
            'state': session['state'],
            'message': f'{judge_name} joined as Judge {judge_num}'
        }, room=session_id)

        # Check if all judges have joined
        connected_judges = sum(1 for j in session['judges'].values() if j['connected'])
        if connected_judges >= session['panel_size']:
            session['state'] = 'waiting_for_ready'
            emit('panel_state_change', {
                'state': 'waiting_for_ready',
                'message': 'All judges connected. Please confirm ready.'
            }, room=session_id)

    @socketio.on('panel_judge_ready')
    def on_panel_judge_ready(data):
        """Panel judge confirms they are ready."""
        session_id = data.get('session_id')
        judge_num = data.get('judge_num')

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]
        if judge_num in session['judges']:
            session['judges'][judge_num]['ready'] = True

        emit('panel_update', {
            'judges': session['judges'],
            'state': session['state'],
            'message': f'Judge {judge_num} is ready'
        }, room=session_id)

        # Check if all judges are ready
        ready_judges = sum(1 for j in session['judges'].values() if j.get('ready', False))
        if ready_judges >= session['panel_size']:
            session['state'] = 'all_ready'
            emit('panel_state_change', {
                'state': 'all_ready',
                'message': 'All judges ready. Event judge can start video.'
            }, room=session_id)

    @socketio.on('panel_start_video')
    def on_panel_start_video(data):
        """Event judge starts the video for all judges."""
        session_id = data.get('session_id')
        video_time = data.get('video_time', 0)

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]
        session['state'] = 'playing'
        session['video_started'] = True
        session['x_presses'] = {}  # Reset X presses

        emit('panel_video_start', {
            'video_time': video_time,
            'message': 'Video started. Press X when working time begins.'
        }, room=session_id)

    @socketio.on('panel_x_press')
    def on_panel_x_press(data):
        """Judge presses X to mark start of working time."""
        session_id = data.get('session_id')
        judge_num = data.get('judge_num')
        press_time = data.get('press_time')  # Client timestamp

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]

        if session['state'] != 'playing':
            return

        # Record this judge's X press time
        session['x_presses'][judge_num] = press_time

        emit('panel_x_received', {
            'judge_num': judge_num,
            'x_presses': list(session['x_presses'].keys())
        }, room=session_id)

        # Check if all judges have pressed X
        if len(session['x_presses']) >= session['panel_size']:
            # Calculate the spread
            times = list(session['x_presses'].values())
            spread = max(times) - min(times)

            if spread <= WORKING_TIME_TOLERANCE:
                # All judges within tolerance - start scoring!
                session['state'] = 'scoring'
                session['timer_running'] = True
                session['timer_start'] = time.time()

                emit('panel_working_time_accepted', {
                    'spread': spread,
                    'message': f'Working time started! (spread: {spread:.2f}s)'
                }, room=session_id)
            else:
                # Spread too large - reset!
                session['state'] = 'reset_required'
                session['x_presses'] = {}
                # Reset judge ready status
                for j in session['judges'].values():
                    j['ready'] = False

                emit('panel_working_time_rejected', {
                    'spread': spread,
                    'tolerance': WORKING_TIME_TOLERANCE,
                    'message': f'X press spread too large ({spread:.2f}s > {WORKING_TIME_TOLERANCE}s). Video will reset.'
                }, room=session_id)

    @socketio.on('panel_reset')
    def on_panel_reset(data):
        """Event judge resets the session after failed X sync."""
        session_id = data.get('session_id')

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]
        session['state'] = 'waiting_for_ready'
        session['video_started'] = False
        session['x_presses'] = {}
        session['timer_running'] = False
        session['timer_start'] = None
        session['scores'] = []

        # Reset judge ready status
        for j in session['judges'].values():
            j['ready'] = False

        emit('panel_session_reset', {
            'state': 'waiting_for_ready',
            'message': 'Session reset. Judges please confirm ready.'
        }, room=session_id)

    @socketio.on('panel_score')
    def on_panel_score(data):
        """Judge submits a score (x, c, or q) during scoring."""
        session_id = data.get('session_id')
        judge_num = data.get('judge_num')
        score_type = data.get('score_type')
        position = data.get('position')
        timestamp = data.get('timestamp')

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]

        if session['state'] != 'scoring':
            return

        # Find or create score entry for this position
        score_entry = None
        for s in session['scores']:
            if s['position'] == position:
                score_entry = s
                break

        if score_entry is None:
            score_entry = {
                'position': position,
                'votes': {},
                'timestamp': timestamp
            }
            session['scores'].append(score_entry)

        score_entry['votes'][judge_num] = score_type

        emit('panel_score_update', {
            'position': position,
            'judge_num': judge_num,
            'score_type': score_type,
            'votes': score_entry['votes'],
            'timestamp': timestamp
        }, room=session_id)

    @socketio.on('panel_timer_stop')
    def on_panel_timer_stop(data):
        """Working time ended - stop scoring."""
        session_id = data.get('session_id')

        if session_id not in panel_sessions:
            return

        session = panel_sessions[session_id]
        session['timer_running'] = False
        session['state'] = 'review'

        emit('panel_timer_stopped', {
            'scores': session['scores'],
            'state': 'review'
        }, room=session_id)

    @socketio.on('leave_panel_session')
    def on_leave_panel_session(data):
        """Judge leaves the panel session."""
        session_id = data.get('session_id')
        judge_num = data.get('judge_num')

        if session_id in panel_sessions:
            session = panel_sessions[session_id]
            if judge_num in session['judges']:
                session['judges'][judge_num]['connected'] = False

            leave_room(session_id)

            emit('panel_update', {
                'judges': session['judges'],
                'state': session['state'],
                'message': f'Judge {judge_num} disconnected'
            }, room=session_id)

            # Clean up empty sessions
            if all(not j['connected'] for j in session['judges'].values()):
                del panel_sessions[session_id]


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_ENV', 'development') == 'development'
    print("\n=== Video Library ===")
    print(f"Database: {'Supabase' if USE_SUPABASE else 'SQLite'}")
    print(f"SocketIO: {'Enabled' if SOCKETIO_ENABLED else 'Disabled'}")
    print(f"Open http://localhost:{port} in your browser")
    print("\nAdmin login: admin / admin123\n")
    if SOCKETIO_ENABLED:
        socketio.run(app, debug=debug, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
    else:
        app.run(debug=debug, host='0.0.0.0', port=port)
