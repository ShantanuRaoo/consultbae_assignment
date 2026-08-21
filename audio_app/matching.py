"""
Task 3 — candidate matching wrapper.

The audio submission form only collects name + phone (per the assignment spec),
so matching here is intentionally scoped to just those two signals — not a
blind reuse of Task 1's full email/phone/name/city composite score, since
email and city are never available from this form and would just be dead
weight in the scoring.

Reuses ingest.py's normalize_name/normalize_phone/split_name so the actual
string-cleaning rules stay identical to Task 1 — only the scoring weights
are redefined here for the two-field case.

ASSUMPTION: ingest.py lives at ../pythontodb/ingest.py relative to this file
(sibling of audio_app/ under the repo root). Adjust IMPORT_PATH below if your
actual repo layout differs.
"""

import os
import sys
import uuid

from rapidfuzz import fuzz

IMPORT_PATH = os.path.join(os.path.dirname(__file__), "..", "pythontodb")
sys.path.insert(0, os.path.abspath(IMPORT_PATH))

from ingestion_pipeline import (  # noqa: E402  (import after sys.path edit is intentional)
    normalize_phone,
    normalize_name,
    split_name,
)

MERGE_THRESHOLD = 50  # same cutoff as Task 1, applied to a smaller signal set here


def has_phone_conflict(phone_a: str, phone_b: str) -> bool:
    """Both sides have a phone and they genuinely differ — strong evidence
    these are NOT the same person. Blocks a name-only match from overriding it."""
    return bool(phone_a) and bool(phone_b) and phone_a != phone_b


def match_score_name_phone(name_a: str, phone_a: str, candidate: dict):
    """
    Scores a submitted (name, phone) against one existing candidate.
    - Exact phone match: +60 (phone is effectively ground truth here — there's
      no email/city backup signal from this form, so a phone match alone is
      enough to merge on its own).
    - Exact full-name match: +30 (both sides already uppercased/normalized).
    - Fuzzy surname / first-name, as a fallback when names aren't an exact match.

    Exact-name-alone (30) intentionally stays below MERGE_THRESHOLD (50) —
    with no city or email to corroborate it, name alone isn't strong enough
    to auto-merge two different people who might share a common name.
    """
    score = 0
    detail = {}

    candidate_phone = candidate.get("phone")
    if phone_a and candidate_phone and phone_a == candidate_phone:
        score += 60
        detail["phone_match"] = True

    name_b = (candidate.get("name") or "").strip()
    name_a_norm = (name_a or "").strip()

    if name_a_norm and name_b and name_a_norm == name_b:
        score += 30
        detail["exact_name_match"] = True
    else:
        fa, sa = split_name(name_a_norm)
        fb, sb = split_name(name_b)
        surname_score = fuzz.ratio(sa.lower(), sb.lower()) if sa and sb else 0
        first_score = fuzz.ratio(fa.lower(), fb.lower()) if fa and fb else 0
        if surname_score >= 85:
            score += 10
            detail["surname_match"] = True
        if first_score >= 85 or (fa and fb and fa[0].lower() == fb[0].lower()):
            score += 5
            detail["first_name_match"] = True

    return score, detail


def fetch_all_candidates(cur):
    """Pull every candidate's id/name/phone/skills — the only fields this
    matching path actually needs. Requires a RealDictCursor."""
    cur.execute("SELECT id, name, phone, skills FROM candidates")
    return [dict(row) for row in cur.fetchall()]


def find_or_create_candidate(cur, name: str, phone: str):
    """
    Matches a submitted name+phone against every existing candidate using
    match_score_name_phone + has_phone_conflict. On a strong match, updates
    the existing candidate's name/phone if they were previously null (never
    overwrites a real value). Otherwise inserts a brand-new candidate.

    Returns (candidate_id: str, was_merged: bool).
    """
    norm_name = normalize_name(name)
    norm_phone = normalize_phone(phone)

    existing_candidates = fetch_all_candidates(cur)

    best = None
    best_score = 0
    for cand in existing_candidates:
        if has_phone_conflict(norm_phone, cand.get("phone")):
            continue
        score, _ = match_score_name_phone(norm_name, norm_phone, cand)
        if score > best_score:
            best, best_score = cand, score

    if best and best_score >= MERGE_THRESHOLD:
        new_name = norm_name if not best.get("name") else best["name"]
        new_phone = norm_phone if not best.get("phone") else best["phone"]
        cur.execute(
            "UPDATE candidates SET name=%s, phone=%s WHERE id=%s",
            (new_name, new_phone, best["id"]),
        )
        return best["id"], True

    cid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO candidates (id, name, phone, skills) VALUES (%s, %s, %s, %s)",
        (cid, norm_name, norm_phone, []),
    )
    return cid, False