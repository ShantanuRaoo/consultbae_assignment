import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def reset_tables():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE audio_submissions, candidate_sources, candidates CASCADE;")
        conn.commit()
        print("Tables truncated.")
    except Exception as e:
        conn.rollback()
        print(f"Truncate failed, rolled back: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    reset_tables()