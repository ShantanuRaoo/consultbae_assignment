"""
Task 3 — Mini audio collection app, FastAPI backend.

Two endpoints:
  POST /submit        — name, phone, audio file -> stores file, extracts audio
                         properties, matches/creates a candidate (Task 1 DB),
                         inserts an audio_submissions row.
  GET  /submissions    — lists every submission with its extracted properties
                         and a playable audio URL, for the second "view" the
                         assignment asks for.

Run:
    uvicorn main:app --reload --port 8000
"""

import os
import uuid
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from extraction import extract_audio_props, estimate_quality
from matching import find_or_create_candidate

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_AUDIO_TYPES = {"audio/wav", "audio/x-wav", "audio/mpeg", "audio/mp3",
                        "audio/webm", "audio/ogg", "audio/m4a", "audio/x-m4a"}

app = FastAPI(title="ConsultBae Audio Collection")

# serves uploaded audio files back out so <audio src="/uploads/..."> works in the browser
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


@app.get("/")
def serve_index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))


@app.post("/submit")
async def submit(name: str = Form(...), phone: str = Form(...), audio: UploadFile = None):
    if audio is None:
        raise HTTPException(status_code=400, detail="No audio file provided")

    if audio.content_type not in ALLOWED_AUDIO_TYPES:
        # not fatal by itself — some browsers send inconsistent MIME types for
        # recorded blobs — but worth flagging rather than silently accepting anything
        pass

    safe_name = "".join(c for c in name if c.isalnum() or c in " ._-").strip() or "unknown"
    ext = os.path.splitext(audio.filename or "")[1] or ".webm"
    file_id = str(uuid.uuid4())
    filename = f"{file_id}_{safe_name.replace(' ', '_')}{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)

    contents = await audio.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Uploaded audio file is empty")

    with open(filepath, "wb") as f:
        f.write(contents)

    try:
        props = extract_audio_props(filepath)
    except Exception as e:
        os.remove(filepath)  # don't leave an orphaned file we can't do anything with
        raise HTTPException(status_code=422, detail=f"Could not process audio file: {e}")

    quality = estimate_quality(props)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        candidate_id, was_merged = find_or_create_candidate(cur, name, phone)

        submission_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO audio_submissions
                (id, candidate_id, audio_path, duration_sec, sample_rate, bitrate, loudness_db)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (submission_id, candidate_id, filename, props["duration_sec"],
             props["sample_rate"], props["bitrate"], props["loudness_db"]),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        os.remove(filepath)
        raise HTTPException(status_code=500, detail="Failed to save submission")
    finally:
        cur.close()
        conn.close()

    return {
        "status": "ok",
        "submission_id": submission_id,
        "candidate_id": candidate_id,
        "matched_existing_candidate": was_merged,
        **props,
        "quality_estimate": quality,
    }


@app.get("/submissions")
def list_submissions():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """
            SELECT
                s.id AS submission_id,
                c.name,
                c.phone,
                s.audio_path,
                s.duration_sec,
                s.sample_rate,
                s.bitrate,
                s.loudness_db,
                s.created_at
            FROM audio_submissions s
            JOIN candidates c ON c.id = s.candidate_id
            ORDER BY s.created_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    results = []
    for row in rows:
        row = dict(row)
        row["audio_url"] = f"/uploads/{row['audio_path']}"
        row["quality_estimate"] = estimate_quality({
            "sample_rate": row["sample_rate"],
            "loudness_db": row["loudness_db"],
        })
        if isinstance(row.get("created_at"), datetime):
            row["created_at"] = row["created_at"].isoformat()
        results.append(row)

    return results