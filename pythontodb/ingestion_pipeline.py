"""
Task 1 — Merge pipeline
Reads source1_naukri_applicants.csv, source2_gig_workers.csv, source3_cbnexus_contacts.csv,
normalizes every field per the agreed business rules, matches people across sources using
a composite score (email + phone + fuzzy name), and writes one clean record per person into Postgres.

Business rules applied during normalization:
  - name              -> UPPERCASE. Abbreviated first names (e.g. "R. VERMA") are replaced
                         with the fuller version once a matching full-name record is found.
  - current_ctc_lpa   -> raw rupee value converted to lakhs, truncated (not rounded) to 2 decimals.
                         e.g. 456780 -> 4.56
  - rate_per_hr       -> "NNN/hr" kept as-is. "NNk/month" converted to hourly using an assumed
                         HOURS_PER_MONTH (documented below). Only the numeric value is stored,
                         no "/hr" suffix.
  - status            -> lowercased, stripped. No further collapsing (active/inactive/paused
                         are kept distinct, just case-normalized).
  - verified          -> collapsed to a strict boolean (Y/Yes/Verified -> true, N/No -> false).

Run:
    python ingest.py
"""

import math
import os
import re
import uuid

import pandas as pd
import psycopg2
import psycopg2.extras
from dateutil import parser as dateparser
from dotenv import load_dotenv
from rapidfuzz import fuzz

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

CSV_PATHS = {
    "naukri": "original_csv/source1_naukri_applicants.csv",
    "gig_workers": "original_csv/source2_gig_workers.csv",
    "cbnexus": "original_csv/source3_cbnexus_contacts.csv",
}

# canonical city mapping — adjust freely, just document your choice in data_issues.md
CITY_MAP = {
    "bangalore": "Bengaluru",
    "bengaluru": "Bengaluru",
    "delhi": "Delhi",
    "new delhi": "Delhi",
    "delhi ncr": "Delhi",
    "gurgaon": "Gurgaon",
    "gurugram": "Gurgaon",
    "noida": "Noida",
    "pune": "Pune",
}

# assumption used to convert ".../month" rates to hourly — documented in data_issues.md
HOURS_PER_MONTH_ASSUMPTION = 160  # ~20 working days x 8 hrs/day

MERGE_THRESHOLD = 50
REVIEW_THRESHOLD = 30

# original column names per source — used to detect stray header rows embedded as data
NAUKRI_COLS = ["Full Name", "Email", "Phone", "City", "Experience (Years)",
               "Current CTC", "Applied Date", "Skills"]
GIG_COLS = ["email_id", "worker_name", "rate", "location", "status", "skill_tags"]
CBNEXUS_COLS = ["Name", "Phone Number", "City", "Verified", "Projects Completed"]


def is_header_row(row, columns):
    """True if every value in the row exactly matches its own column name —
    a stray duplicated header embedded as a data row, not just one coincidental field match."""
    try:
        return all(str(row.get(col)).strip() == col for col in columns)
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_email(val):
    if not isinstance(val, str) or not val.strip():
        return None
    return val.strip().lower()


def normalize_phone(val):
    """Strip +91 / 91 / 0 country-code prefixes and non-digits, keep last 10 digits."""
    if val is None:
        return None
    digits = re.sub(r"\D", "", str(val))
    if len(digits) > 10:
        digits = digits[-10:]
    return digits if len(digits) == 10 else None


def normalize_city(val):
    if not isinstance(val, str) or not val.strip():
        return None
    key = val.strip().lower()
    return CITY_MAP.get(key, val.strip().title())


def normalize_status(val):
    """Just lowercase + strip. Keeps active/inactive/paused distinct."""
    if not isinstance(val, str) or not val.strip():
        return None
    return val.strip().lower()


def normalize_verified_bool(val):
    """Collapse Y/Yes/Verified/N/No into a strict boolean."""
    if not isinstance(val, str):
        return None
    v = val.strip().lower()
    if v in ("y", "yes", "verified"):
        return True
    if v in ("n", "no"):
        return False
    return None


def normalize_name(val):
    """Uppercase. Abbreviation resolution (R. VERMA -> ROHIT VERMA) happens at merge time,
    since we need a second matching record to know the fuller name."""
    if not isinstance(val, str) or not val.strip():
        return None
    return val.strip().upper()


def is_abbreviated_name(name):
    """True if the first token looks like an initial, e.g. 'R' or 'R.' """
    if not name:
        return False
    tokens = name.strip().split()
    if not tokens:
        return False
    return bool(re.fullmatch(r"[A-Za-z]\.?", tokens[0]))


def pick_better_name(existing_name, new_name):
    """When merging two records, prefer the non-abbreviated / longer name."""
    if not existing_name:
        return new_name
    if not new_name:
        return existing_name
    existing_abbrev = is_abbreviated_name(existing_name)
    new_abbrev = is_abbreviated_name(new_name)
    if existing_abbrev and not new_abbrev:
        return new_name
    if new_abbrev and not existing_abbrev:
        return existing_name
    return existing_name if len(existing_name) >= len(new_name) else new_name


def normalize_ctc_lpa(val, issues=None, context=""):
    """Some rows report CTC already in lakhs (e.g. 7.8), others in raw rupees (e.g. 456780).
    Raw rupees < 60000 as an annual salary is implausible, so treat anything under that
    threshold as already-in-lakhs and leave it as-is; anything at/above it gets divided
    down and truncated (not rounded) to 2 decimals. e.g. 456780 -> 4.56, but 7.8 stays 7.8."""
    raw = to_float_safe(val)
    if raw is None:
        return None

    if raw < 60000:
        if issues is not None:
            issues.append(f"{context}: CTC {raw} treated as already-in-lakhs, kept as-is")
        return round(raw, 2)

    lakhs = raw / 100000
    result = math.floor(lakhs * 100) / 100
    if issues is not None:
        issues.append(f"{context}: CTC {raw} converted to {result} LPA (truncated)")
    return result


def normalize_rate(val, issues=None, context=""):
    """'1415/hr' -> 1415.0. '55k/month' -> hourly value using HOURS_PER_MONTH_ASSUMPTION.
    Returns just the numeric value, no unit suffix."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s or "/" not in s:
        return None

    numeric_part, _, unit_part = s.partition("/")
    numeric_part = numeric_part.strip()
    unit = "month" if "month" in unit_part else "hr" if "hr" in unit_part else None

    multiplier = 1
    if numeric_part.endswith("k"):
        multiplier = 1000
        numeric_part = numeric_part[:-1]

    numeric_part = re.sub(r"[^\d.]", "", numeric_part)
    if not numeric_part:
        return None

    value = float(numeric_part) * multiplier

    if unit == "month":
        value = value / HOURS_PER_MONTH_ASSUMPTION
        if issues is not None:
            issues.append(
                f"{context}: converted monthly rate '{val}' to hourly "
                f"({value:.2f}/hr) using {HOURS_PER_MONTH_ASSUMPTION} hrs/month assumption"
            )

    return round(value, 2)


def parse_date_safe(val):
    if not isinstance(val, str) or not val.strip():
        return None
    try:
        return dateparser.parse(val.strip(), dayfirst=True).date()
    except (ValueError, OverflowError):
        return None


def to_int_safe(val):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def to_float_safe(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def split_name(full_name):
    if not isinstance(full_name, str) or not full_name.strip():
        return "", ""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def parse_skills(val):
    if not isinstance(val, str) or not val.strip():
        return []
    return [s.strip().lower() for s in val.split(",") if s.strip()]


# ---------------------------------------------------------------------------
# Source-specific loaders — each returns a list of normalized dicts
# ---------------------------------------------------------------------------

def load_naukri(path, issues):
    df = pd.read_csv(path)
    records = []
    for idx, row in df.iterrows():
        if is_header_row(row, NAUKRI_COLS):
            issues.append(f"naukri row {idx}: stray header row found in data, skipped")
            continue

        email = normalize_email(row.get("Email"))
        phone = normalize_phone(row.get("Phone"))
        if phone is None:
            issues.append(f"naukri row {idx}: unparseable phone '{row.get('Phone')}'")

        applied_date = parse_date_safe(row.get("Applied Date"))
        if applied_date is None and isinstance(row.get("Applied Date"), str):
            issues.append(f"naukri row {idx}: unparseable date '{row.get('Applied Date')}'")

        raw_ctc = row.get("Current CTC")
        ctc_lpa = normalize_ctc_lpa(raw_ctc, issues=issues, context=f"naukri row {idx}")

        raw_name = row.get("Full Name")
        name = normalize_name(raw_name)
        if is_abbreviated_name(name):
            issues.append(f"naukri row {idx}: abbreviated name '{raw_name}' — will resolve on merge if a fuller match is found")

        records.append({
            "name": name,
            "email": email,
            "phone": phone,
            "city": normalize_city(row.get("City")),
            "experience_yrs": to_float_safe(row.get("Experience (Years)")),
            "current_ctc_lpa": ctc_lpa,
            "rate_per_hr": None,
            "applied_date": applied_date,
            "status": None,
            "verified": None,
            "projects_completed": None,
            "skills": parse_skills(row.get("Skills")),
            "source_name": "naukri",
            "source_row_ref": str(idx),
        })
    return records


def load_gig_workers(path, issues):
    df = pd.read_csv(path)
    records = []
    for idx, row in df.iterrows():
        if is_header_row(row, GIG_COLS):
            issues.append(f"gig_workers row {idx}: stray header row found in data, skipped")
            continue

        email_raw = row.get("email_id")

        # detect the known column-shift bug: email_id field doesn't look like an email
        # (checked after the header-row guard above, since a header row would also fail this test)
        if isinstance(email_raw, str) and "@" not in email_raw:
            issues.append(f"gig_workers row {idx}: column-shifted row detected, fields realigned")
            row = {
                "email_id": row.get("worker_name"),
                "worker_name": row.get("rate"),
                "rate": row.get("location"),
                "location": row.get("status"),
                "status": row.get("skill_tags"),
                "skill_tags": email_raw,
            }

        # skip fully-null rows
        if pd.isna(row.get("email_id")) and pd.isna(row.get("worker_name")):
            issues.append(f"gig_workers row {idx}: fully null row, skipped")
            continue

        email = normalize_email(row.get("email_id"))
        rate_val = normalize_rate(row.get("rate"), issues=issues, context=f"gig_workers row {idx}")

        raw_name = row.get("worker_name")
        name = normalize_name(raw_name)
        if is_abbreviated_name(name):
            issues.append(f"gig_workers row {idx}: abbreviated name '{raw_name}' — will resolve on merge if a fuller match is found")

        records.append({
            "name": name,
            "email": email,
            "phone": None,
            "city": normalize_city(row.get("location")),
            "experience_yrs": None,
            "current_ctc_lpa": None,
            "rate_per_hr": rate_val,
            "applied_date": None,
            "status": normalize_status(row.get("status")),
            "verified": None,
            "projects_completed": None,
            "skills": parse_skills(row.get("skill_tags")),
            "source_name": "gig_workers",
            "source_row_ref": str(idx),
        })
    return records


def load_cbnexus(path, issues):
    df = pd.read_csv(path)
    records = []
    for idx, row in df.iterrows():
        if is_header_row(row, CBNEXUS_COLS):
            issues.append(f"cbnexus row {idx}: stray header row found in data, skipped")
            continue

        phone = normalize_phone(row.get("Phone Number"))
        if phone is None:
            issues.append(f"cbnexus row {idx}: unparseable phone '{row.get('Phone Number')}'")

        projects = to_int_safe(row.get("Projects Completed"))
        if projects is None and not pd.isna(row.get("Projects Completed")):
            issues.append(f"cbnexus row {idx}: non-numeric Projects Completed '{row.get('Projects Completed')}'")

        raw_name = row.get("Name")
        name = normalize_name(raw_name)
        if is_abbreviated_name(name):
            issues.append(f"cbnexus row {idx}: abbreviated name '{raw_name}' — will resolve on merge if a fuller match is found")

        records.append({
            "name": name,
            "email": None,
            "phone": phone,
            "city": normalize_city(row.get("City")),
            "experience_yrs": None,
            "current_ctc_lpa": None,
            "rate_per_hr": None,
            "applied_date": None,
            "status": None,
            "verified": normalize_verified_bool(row.get("Verified")),
            "projects_completed": projects,
            "skills": [],
            "source_name": "cbnexus",
            "source_row_ref": str(idx),
        })
    return records


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def has_conflict(a, b):
    """Strong evidence these are NOT the same person: both sides have a non-null
    email (or phone) and they genuinely differ. Blocks a name+city-only merge
    from overriding conflicting hard identifiers."""
    if a["email"] and b["email"] and a["email"] != b["email"]:
        return True
    if a["phone"] and b["phone"] and a["phone"] != b["phone"]:
        return True
    return False


def match_score(a, b):
    score = 0
    detail = {}

    if a["email"] and b["email"] and a["email"] == b["email"]:
        score += 50
        detail["email_match"] = True

    if a["phone"] and b["phone"] and a["phone"] == b["phone"]:
        score += 35
        detail["phone_match"] = True

    name_a = (a["name"] or "").strip()
    name_b = (b["name"] or "").strip()

    if name_a and name_b and name_a == name_b:
        # exact full-name match (both already uppercased/normalized) — strong signal,
        # matters most when email/phone are null on one side and there's nothing else to go on
        score += 30
        detail["exact_name_match"] = True
    else:
        fa, sa = split_name(a["name"])
        fb, sb = split_name(b["name"])
        surname_score = fuzz.ratio(sa.lower(), sb.lower()) if sa and sb else 0
        first_score = fuzz.ratio(fa.lower(), fb.lower()) if fa and fb else 0
        if surname_score >= 85:
            score += 10
            detail["surname_match"] = True
        if first_score >= 85 or (fa and fb and fa[0].lower() == fb[0].lower()):
            score += 5
            detail["first_name_match"] = True

    if a["city"] and b["city"] and a["city"].strip().lower() == b["city"].strip().lower():
        score += 20
        detail["city_match"] = True

    return score, detail


def find_best_match(record, existing_candidates):
    best = None
    best_score = 0
    best_detail = {}
    for cand in existing_candidates:
        if has_conflict(record, cand):
            continue  # differing email or phone on both sides = different people, skip regardless of name/city
        score, detail = match_score(record, cand)
        if score > best_score:
            best, best_score, best_detail = cand, score, detail
    return best, best_score, best_detail


def merge_fields(existing, new, issues=None):
    """Fill nulls on the existing candidate with values from the new record (never overwrite
    non-null), except `name`, which actively swaps to the fuller/non-abbreviated version."""
    merged = dict(existing)

    better_name = pick_better_name(existing.get("name"), new.get("name"))
    if better_name != existing.get("name") and issues is not None:
        issues.append(f"name resolved on merge: '{existing.get('name')}' -> '{better_name}'")
    merged["name"] = better_name

    for key in ("email", "phone", "city", "experience_yrs", "current_ctc_lpa",
                "rate_per_hr", "applied_date", "status", "verified", "projects_completed"):
        if merged.get(key) in (None, "") and new.get(key) not in (None, ""):
            merged[key] = new[key]

    merged["skills"] = sorted(set(existing.get("skills") or []) | set(new.get("skills") or []))
    return merged


# ---------------------------------------------------------------------------
# DB ops
# ---------------------------------------------------------------------------

def insert_candidate(cur, record):
    cid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO candidates
            (id, name, email, phone, city, experience_yrs, current_ctc_lpa,
             rate_per_hr, applied_date, status, verified, projects_completed, skills)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (cid, record["name"], record["email"], record["phone"], record["city"],
         record["experience_yrs"], record["current_ctc_lpa"], record["rate_per_hr"],
         record["applied_date"], record["status"], record["verified"],
         record["projects_completed"], record["skills"]),
    )
    record["id"] = cid
    return cid


def update_candidate(cur, cid, merged):
    cur.execute(
        """
        UPDATE candidates SET
            name=%s, email=%s, phone=%s, city=%s, experience_yrs=%s, current_ctc_lpa=%s,
            rate_per_hr=%s, applied_date=%s, status=%s, verified=%s,
            projects_completed=%s, skills=%s
        WHERE id=%s
        """,
        (merged["name"], merged["email"], merged["phone"], merged["city"], merged["experience_yrs"],
         merged["current_ctc_lpa"], merged["rate_per_hr"], merged["applied_date"],
         merged["status"], merged["verified"], merged["projects_completed"],
         merged["skills"], cid),
    )


def insert_source_link(cur, candidate_id, record, match_method, confidence):
    cur.execute(
        """
        INSERT INTO candidate_sources
            (id, candidate_id, source_name, source_row_ref, match_method, match_confidence)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (str(uuid.uuid4()), candidate_id, record["source_name"], record["source_row_ref"],
         match_method, confidence),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    issues = []

    all_records = []
    all_records += load_naukri(CSV_PATHS["naukri"], issues)
    all_records += load_gig_workers(CSV_PATHS["gig_workers"], issues)
    all_records += load_cbnexus(CSV_PATHS["cbnexus"], issues)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    existing_candidates = []
    stats = {"new": 0, "merged": 0, "review": 0}

    try:
        for record in all_records:
            if not record.get("name") and not record.get("email") and not record.get("phone"):
                issues.append(f"skipped fully-empty record from {record['source_name']} row {record['source_row_ref']}")
                continue

            match, score, detail = find_best_match(record, existing_candidates)

            if match and score >= MERGE_THRESHOLD:
                method = "email" if detail.get("email_match") else \
                         "phone" if detail.get("phone_match") else "name_composite"
                merged = merge_fields(match, record, issues=issues)
                update_candidate(cur, match["id"], merged)
                match.update(merged)
                insert_source_link(cur, match["id"], record, method, score / 100)
                stats["merged"] += 1

            elif match and score >= REVIEW_THRESHOLD:
                cid = insert_candidate(cur, record)
                insert_source_link(cur, cid, record, "needs_review", score / 100)
                existing_candidates.append(record)
                issues.append(
                    f"needs_review: '{record['name']}' ({record['source_name']} row "
                    f"{record['source_row_ref']}) scored {score} vs candidate {match['id']} — inserted as new, verify manually"
                )
                stats["review"] += 1

            else:
                cid = insert_candidate(cur, record)
                insert_source_link(cur, cid, record, "new_candidate", 1.0)
                existing_candidates.append(record)
                stats["new"] += 1

        conn.commit()
        print(f"Done. New: {stats['new']}, Merged: {stats['merged']}, Flagged for review: {stats['review']}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    if issues:
        os.makedirs("../docs", exist_ok=True)
        with open("docs/ingestion_log.txt", "w") as f:
            f.write("\n".join(issues))
        print(f"{len(issues)} issues logged to docs/ingestion_log.txt")


if __name__ == "__main__":
    main()