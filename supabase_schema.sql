-- USPA Video Library - Supabase Schema
-- Run this in Supabase SQL Editor to create the required tables

-- Videos table
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    url TEXT,
    thumbnail TEXT,
    category TEXT NOT NULL,
    subcategory TEXT,
    tags TEXT,
    duration TEXT,
    created_at TEXT NOT NULL,
    views INTEGER DEFAULT 0,
    video_type TEXT DEFAULT 'url',
    local_file TEXT,
    event TEXT,
    team TEXT,
    round_num TEXT,
    jump_num TEXT
);

-- Add columns if table already exists
ALTER TABLE videos ADD COLUMN IF NOT EXISTS event TEXT;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS team TEXT;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS round_num TEXT;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS jump_num TEXT;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    role TEXT NOT NULL,
    name TEXT NOT NULL
);

-- Insert default admin user
INSERT INTO users (username, password, role, name)
VALUES ('admin', 'admin123', 'admin', 'Administrator')
ON CONFLICT (username) DO NOTHING;

-- Enable Row Level Security (optional but recommended)
ALTER TABLE videos ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

-- Allow public read access to videos
CREATE POLICY "Allow public read access to videos" ON videos
    FOR SELECT USING (true);

-- Allow authenticated users to manage videos (for admin)
CREATE POLICY "Allow all access to videos" ON videos
    FOR ALL USING (true);

-- Allow all access to users table (for login)
CREATE POLICY "Allow all access to users" ON users
    FOR ALL USING (true);
