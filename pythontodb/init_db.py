import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS candidates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT,
    email TEXT,
    phone TEXT,
    city TEXT,
    experience_yrs REAL,
    current_ctc_lpa REAL,
    rate_per_hr REAL,
    applied_date DATE,
    status TEXT,
    verified BOOLEAN DEFAULT FALSE,
    projects_completed INTEGER,
    skills TEXT[],
    skill_category TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS candidate_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID REFERENCES candidates(id),
    source_name TEXT,
    source_row_ref TEXT,
    match_method TEXT,
    match_confidence REAL
);

CREATE TABLE IF NOT EXISTS audio_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID REFERENCES candidates(id),
    audio_path TEXT,
    duration_sec REAL,
    sample_rate INTEGER,
    bitrate INTEGER,
    loudness_db REAL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS candidates_email_idx ON candidates (email);
CREATE INDEX IF NOT EXISTS candidates_phone_idx ON candidates (phone);
"""

def init_schema():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    cur.close()
    conn.close()
    print("Schema ready.")

if __name__ == "__main__":
    init_schema()
