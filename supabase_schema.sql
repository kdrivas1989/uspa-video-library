-- Supabase Schema for USPA Video Library
-- Run this in your Supabase SQL Editor (https://supabase.com/dashboard)

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

-- Videos table
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
    event TEXT,
    team TEXT,
    round_num TEXT,
    jump_num TEXT,
    start_time REAL DEFAULT 0
);

-- Competitions table
CREATE TABLE IF NOT EXISTS competitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_types TEXT,
    total_rounds INTEGER DEFAULT 10,
    created_at TEXT NOT NULL,
    status TEXT DEFAULT 'active'
);

-- Competition teams table
CREATE TABLE IF NOT EXISTS competition_teams (
    id TEXT PRIMARY KEY,
    competition_id TEXT NOT NULL REFERENCES competitions(id),
    team_number TEXT NOT NULL,
    team_name TEXT NOT NULL,
    class TEXT NOT NULL,
    members TEXT,
    category TEXT,
    event TEXT,
    photo TEXT,
    created_at TEXT NOT NULL
);

-- Competition scores table
CREATE TABLE IF NOT EXISTS competition_scores (
    id TEXT PRIMARY KEY,
    competition_id TEXT NOT NULL REFERENCES competitions(id),
    team_id TEXT NOT NULL REFERENCES competition_teams(id),
    round_num INTEGER NOT NULL,
    score REAL,
    score_data TEXT,
    video_id TEXT,
    created_at TEXT NOT NULL
);

-- Enable Row Level Security (optional but recommended)
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE videos ENABLE ROW LEVEL SECURITY;
ALTER TABLE competitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE competition_teams ENABLE ROW LEVEL SECURITY;
ALTER TABLE competition_scores ENABLE ROW LEVEL SECURITY;

-- Allow public access (adjust policies as needed for your security requirements)
CREATE POLICY "Allow public read access to videos" ON videos FOR SELECT USING (true);
CREATE POLICY "Allow public read access to competitions" ON competitions FOR SELECT USING (true);
CREATE POLICY "Allow public read access to competition_teams" ON competition_teams FOR SELECT USING (true);
CREATE POLICY "Allow public read access to competition_scores" ON competition_scores FOR SELECT USING (true);
CREATE POLICY "Allow public read access to users" ON users FOR SELECT USING (true);

-- Allow insert/update/delete for service role (your app)
CREATE POLICY "Allow service role full access to videos" ON videos FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow service role full access to competitions" ON competitions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow service role full access to competition_teams" ON competition_teams FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow service role full access to competition_scores" ON competition_scores FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow service role full access to users" ON users FOR ALL USING (true) WITH CHECK (true);
